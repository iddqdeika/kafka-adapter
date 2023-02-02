package kafkaadapt

import (
	"context"
	"fmt"
	sarama "github.com/Shopify/sarama"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = fmt.Errorf("kafka adapter is closed")
var ErrAsyncNack = fmt.Errorf("nack is inapplicable in async message acking mode")

const (
	writerChanSize = 100
)

func FromStruct(cfg KafkaCfg, logger Logger) (*Queue, error) {
	return newKafkaQueue(cfg, logger)
}

func FromConfig(cfg Config, logger Logger) (*Queue, error) {
	brokers, err := cfg.GetString("KAFKA.BROKERS")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	controller, err := cfg.GetString("KAFKA.CONTROLLER_ADDRESS")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	queuesToRead, err := cfg.GetString("KAFKA.QUEUES_TO_READ")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	queuesToWrite, err := cfg.GetString("KAFKA.QUEUES_TO_WRITE")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	consumerGroup, err := cfg.GetString("KAFKA.CONSUMER_GROUP")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	concurrency, err := cfg.GetInt("KAFKA.CONCURRENCY")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	batchSize, err := cfg.GetInt("KAFKA.BATCH_SIZE")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	async, err := cfg.GetInt("KAFKA.ASYNC")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	pnum, err := cfg.GetInt("KAFKA.DEFAULT_TOPIC_CONFIG.NUM_PARTITIONS")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	rfactor, err := cfg.GetInt("KAFKA.DEFAULT_TOPIC_CONFIG.REPLICATION_FACTOR")
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}
	asyncAck, err := cfg.GetInt("KAFKA.ASYNC_ACK")
	if err != nil {
		asyncAck = 0
	}
	codec, _ := cfg.GetString("KAFKA.COMPRESSION_CODEC")
	resetOffsetForTopics, _ := cfg.GetString("KAFKA.RESET_OFFSET_FOR_TOPICS")

	return newKafkaQueue(KafkaCfg{
		Concurrency:          concurrency,
		QueueToReadNames:     strings.Split(queuesToRead, ";"),
		QueueToWriteNames:    strings.Split(queuesToWrite, ";"),
		ResetOffsetForTopics: strings.Split(resetOffsetForTopics, ";"),
		Brokers:              strings.Split(brokers, ";"),
		ControllerAddress:    controller,
		ConsumerGroupID:      consumerGroup,
		BatchSize:            batchSize,
		Async:                async == 1,
		AsyncAck:             asyncAck == 1,
		DefaultTopicConfig: TopicConfig{
			NumPartitions:     pnum,
			ReplicationFactor: rfactor,
		},
		CompressionCodec: codec,
	}, logger)
}

func newKafkaQueue(cfg KafkaCfg, logger Logger) (*Queue, error) {
	fixDefaultTopicConfig(&cfg.DefaultTopicConfig, logger)
	fixConfig(cfg, logger)

	q := &Queue{
		cfg:    cfg,
		logger: logger,
	}
	err := q.init()
	if err != nil {
		return nil, fmt.Errorf("error during kafka init: %v", err)
	}
	return q, nil
}

func fixConfig(cfg KafkaCfg, logger Logger) {
	if cfg.ReaderQueueCapacity == 0 {
		cfg.ReaderQueueCapacity = 100
	}
}

func fixDefaultTopicConfig(cfg *TopicConfig, logger Logger) {
	if cfg.NumPartitions < 1 {
		logger.Errorf("DefaultTopicConfig.NumPartitions is %v. setting to 1 instead", cfg.NumPartitions)
		cfg.NumPartitions = 1
	}
	if cfg.ReplicationFactor < 1 {
		logger.Errorf("DefaultTopicConfig.ReplicationFactor is %v. setting to 1 instead", cfg.NumPartitions)
		cfg.ReplicationFactor = 1
	}
}

type KafkaCfg struct {
	//used for concurrent read support
	//due to kafka internal structure we need to create at least one
	//consumer in consumer group for each topic partition.
	//that's why concurrency must be set to equal or higher value than
	//possible topic partition count!
	Concurrency int

	//max batch size that will be delivered by single writer at once
	//in sync mode writer waits for batch is full or batch timeout outcome
	//default is 100
	BatchSize int

	//enables async mode
	//in async mode writer will not wait acquirement of successfull write operation
	//that's why Put method will not return any error and will not be blocked
	//use it if you don't need delivery guarantee
	//default is false
	Async bool

	//enables async acknowledges
	//
	//if false(default): kafka reader locks until previous message acked/nacked
	//
	//if true: kafka reader can produce multiple messages,
	//but there is no possibility to provide Nack mechanism,
	//thats why msg.Nack() will return error
	AsyncAck bool

	// The capacity of the internal message queue, defaults to 100 if none is
	// set.
	ReaderQueueCapacity int

	QueueToReadNames  []string
	QueueToWriteNames []string

	//topics for which we need to reset offset before reading.
	//for example, we need to start read some topic 'articles' from the beginning
	//in this case we need to append string "articles" to this array
	//when Adapter registers writer for this topic - consumer's offset will reset to the beginning.
	ResetOffsetForTopics []string

	Brokers           []string
	ControllerAddress string

	ConsumerGroupID string

	CompressionCodec   string
	DefaultTopicConfig TopicConfig
}

type TopicConfig kafka.TopicConfig

func (c TopicConfig) WithSetting(name, value string) {
	c.ConfigEntries = append(c.ConfigEntries, kafka.ConfigEntry{
		ConfigName:  name,
		ConfigValue: value,
	})
}

type Queue struct {
	cfg           KafkaCfg
	logger        Logger
	c             *kafka.Client
	srm           sarama.Client
	readers       map[string]chan *kafka.Reader
	readerOffsets map[string]*int64
	offsetLock    sync.RWMutex

	messages map[string]chan *Message
	writers  map[string]chan *kafka.Writer
	closed   chan struct{}

	m sync.RWMutex
}

func (q *Queue) init() error {

	if q.cfg.Concurrency < 1 {
		q.cfg.Concurrency = 1
	}

	q.readers = make(map[string]chan *kafka.Reader)
	q.readerOffsets = make(map[string]*int64)
	q.messages = make(map[string]chan *Message)
	q.writers = make(map[string]chan *kafka.Writer)
	q.closed = make(chan struct{})

	//some checkup
	for _, b := range q.cfg.Brokers {
		conn, err := kafka.Dial("tcp", b)
		if err != nil {
			return fmt.Errorf("cant connect to broker %v, err: %v", b, err)
		}
		conn.Close()
	}
	//sarama
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_3_0_0
	srm, err := sarama.NewClient(q.cfg.Brokers, cfg)
	if err != nil {
		return fmt.Errorf("cant create sarama kafka client: %v", err)
	}
	q.srm = srm

	//fill readers
	for _, topic := range q.cfg.QueueToReadNames {
		q.ReaderRegister(topic)
	}
	//fill writers
	for _, topic := range q.cfg.QueueToWriteNames {
		q.WriterRegister(topic)
	}
	return nil
}

func (q *Queue) ReaderRegister(topic string) {
	q.m.Lock()
	defer q.m.Unlock()
	if _, ok := q.readers[topic]; ok {
		return
	}
	if topic == "" {
		return
	}
	q.offsetLock.Lock()
	var offset int64
	q.readerOffsets[topic] = &offset
	q.offsetLock.Unlock()
	ch := make(chan *kafka.Reader, q.cfg.Concurrency)
	msgChan := make(chan *Message, q.cfg.ReaderQueueCapacity)
	for i := 0; i < q.cfg.Concurrency; i++ {
		cfg := kafka.ReaderConfig{
			Brokers:       q.cfg.Brokers,
			GroupID:       q.cfg.ConsumerGroupID,
			Topic:         topic,
			MinBytes:      10e1,
			MaxBytes:      10e5,
			QueueCapacity: q.cfg.ReaderQueueCapacity,
		}
		if q.cfg.AsyncAck {
			cfg.CommitInterval = time.Second
		}
		r := kafka.NewReader(cfg)
		if contains(q.cfg.ResetOffsetForTopics, topic) {
			r.SetOffset(kafka.FirstOffset)
		}
		ch <- r
		go q.produceMessages(ch, msgChan)
	}
	q.readers[topic] = ch
	q.messages[topic] = msgChan
}

func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func (q *Queue) WriterRegister(topic string) {
	q.m.Lock()
	defer q.m.Unlock()
	if topic == "" {
		return
	}
	if _, ok := q.writers[topic]; !ok {
		q.writers[topic] = make(chan *kafka.Writer, writerChanSize)
	}
	var codec kafka.CompressionCodec
	switch q.cfg.CompressionCodec {
	case "snappy":
		codec = snappy.NewCompressionCodec()
	default:
		codec = nil
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          q.cfg.Brokers,
		BatchSize:        q.cfg.BatchSize,
		BatchTimeout:     time.Millisecond * 200,
		Async:            q.cfg.Async,
		Topic:            topic,
		Balancer:         &kafka.LeastBytes{},
		CompressionCodec: codec,
	})
	q.writers[topic] <- w
}

func (q *Queue) WritersRegister(topic string, concurrency int) {
	for i := 0; i < concurrency; i++ {
		q.WriterRegister(topic)
	}
}

func (q *Queue) CleanupOffsets(topic string, partitions int) error {
	of, err := sarama.NewOffsetManagerFromClient(q.cfg.ConsumerGroupID, q.srm)
	if err != nil {
		return err
	}
	defer of.Close()
	for i := 0; i < partitions; i++ {
		p, err := of.ManagePartition(topic, int32(i))
		if err != nil {
			return err
		}
		p.MarkOffset(0, "modified by kafka-adapter")
		p.ResetOffset(0, "modified by kafka-adapter")
		p.Close()
	}
	return nil
}

func (q *Queue) produceMessages(rch chan *kafka.Reader, ch chan *Message) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-q.closed:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	for {
		ok := q.producerIteration(ctx, rch, ch)
		if !ok {
			return
		}
	}
}

func (q *Queue) producerIteration(ctx context.Context, rch chan *kafka.Reader, ch chan *Message) bool {
	select {
	case <-q.closed:
		return false
	default:
	}

	var r *kafka.Reader
	var ok bool
	select {
	case r, ok = <-rch:
		if !ok {
			return false
		}
	case <-ctx.Done():
		return false
	}

	msg, err := r.FetchMessage(ctx)
	if err != nil {
		q.logger.Errorf("error during kafka message fetching: %v", err)
		rch <- r
		return true
	}

	// суть в том, что ридер вернется в канал ридеров только при ack/nack, не раньше.
	// следующее сообщение с ридера читать нельзя, пока не будет ack/nack на предыдущем.
	mi := Message{
		msg:     &msg,
		reader:  r,
		rch:     rch,
		needack: q.cfg.ConsumerGroupID != "",
		actualizeOffset: func(o int64) {
			atomic.StoreInt64(q.readerOffsets[r.Config().Topic], o)
		},
	}
	// если консумергруппа пуста, то месседжи подтверждаются автоматически и удерживать ридер нет смысла.
	if q.cfg.ConsumerGroupID == "" {
		mi.once.Do(mi.returnReader)
	}
	// если асинхронное подтверждение, то месседжи подтверждаются в произвольном порядке и удерживать ридер нет смысла.
	if q.cfg.AsyncAck && q.cfg.ConsumerGroupID != "" {
		mi.once.Do(mi.returnReader)
	}
	select {
	case ch <- &mi:
		break
	case <-ctx.Done():
		err := r.Close()
		if err != nil {
			q.logger.Errorf("err during reader closing: %v", err)
		}
	}
	return true
}

func (q *Queue) Put(queue string, data []byte) error {
	ctx := context.Background()
	return q.PutWithCtx(ctx, queue, data)
}

func (q *Queue) PutWithCtx(ctx context.Context, queue string, data []byte) error {
	return q.PutBatchWithCtx(ctx, queue, data)
}

func (q *Queue) PutBatch(queue string, data ...[]byte) error {
	return q.PutBatchWithCtx(context.Background(), queue, data...)
}

func (q *Queue) PutBatchWithCtx(ctx context.Context, queue string, data ...[]byte) error {
	select {
	case <-q.closed:
		return ErrClosed
	default:

	}

	msgs := make([]kafka.Message, 0)
	for _, d := range data {
		msgs = append(msgs, kafka.Message{Value: d})
	}
	q.m.RLock()
	wch, ok := q.writers[queue]
	q.m.RUnlock()
	if ok {
		w := <-wch
		wch <- w
		err := w.WriteMessages(ctx, msgs...)
		if err != nil {
			return fmt.Errorf("error during writing Message to kafka: %v", err)
		}
		return nil
	}
	return fmt.Errorf("there is no such topic declared in config: %v", queue)
}

// KV - key-value pair, which can be used as data for kafka message
// key can be empty, but in topics with compaction enabled all empty keys will be compacted as the same
type KV struct {
	Key   []byte
	Value []byte
}

func (q *Queue) PutKVBatchWithCtx(ctx context.Context, queue string, kvs ...KV) error {
	select {
	case <-q.closed:
		return ErrClosed
	default:

	}

	msgs := make([]kafka.Message, 0)
	for _, kv := range kvs {
		msgs = append(msgs, kafka.Message{Key: kv.Key, Value: kv.Value})
	}
	q.m.RLock()
	wch, ok := q.writers[queue]
	q.m.RUnlock()
	if ok {
		w := <-wch
		wch <- w
		err := w.WriteMessages(ctx, msgs...)
		if err != nil {
			return fmt.Errorf("error during writing Message to kafka: %v", err)
		}
		return nil
	}
	return fmt.Errorf("there is no such topic declared in config: %v", queue)
}

func (q *Queue) Get(queue string) (*Message, error) {
	ctx := context.Background()
	return q.GetWithCtx(ctx, queue)
}

func (q *Queue) GetWithCtx(ctx context.Context, queue string) (*Message, error) {
	select {
	case <-q.closed:
		return nil, ErrClosed
	default:

	}

	q.m.RLock()
	mch, ok := q.messages[queue]
	q.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("there is no such topic declared in config: %v", queue)
	}

	select {
	case <-ctx.Done():
		return nil, context.Canceled
	case <-q.closed:
		return nil, ErrClosed
	case msg := <-mch:
		return msg, nil
	}
}

func (q *Queue) Close() {
	select {
	case <-q.closed:
		return
	default:
		close(q.closed)
	}
	wg := sync.WaitGroup{}
	q.m.Lock()
	defer q.m.Unlock()
	q.srm.Close()
	for _, rchan := range q.readers {
	readers:
		for {
			select {
			case r := <-rchan:
				wg.Add(1)
				go func() {
					err := r.Close()
					if err != nil {
						q.logger.Errorf("err during reader closing: %v", err)
					}
					wg.Done()
				}()
			default:
				//здесь можно было бы закрыть канал, но мы не знаем когда кто попробует сообщения закоммитить.
				break readers
			}
		}
	}
	for _, wch := range q.writers {
		go func() {
			//оставляем это без waitgroup, т.к. в пакете kafka-go баг.
			//если writemessages закрывается до того, как все результаты внутренних ретраев были считаны
			//например, при закрытии контекста
			//то врайтер повисает в воздухе:
			//writemessages не читает результаты из внутреннего writer
			//а внутренний writer.write блокируется в попытке записать результаты.
			for w := range wch {
				err := w.Close()
				if err != nil {
					q.logger.Errorf("err during writer closing: %v", err)
				}
			}

		}()
	}
	wg.Wait()
}

//EnsureTopic Ensures that topic with given name was created with background context set
func (q *Queue) EnsureTopic(topicName string) error {
	return q.EnsureTopicWithCtx(context.Background(), topicName)
}

//EnsureTopicWithCtx ensures that topic with given name was created
func (q *Queue) EnsureTopicWithCtx(ctx context.Context, topicName string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_3_0_0
	a, err := sarama.NewClusterAdmin(q.cfg.Brokers, cfg)
	if err != nil {
		panic(err)
	}
	defer a.Close()
	topics, err := a.ListTopics()
	if err != nil {
		return err
	}
	_, ok := topics[topicName]
	if !ok {
		err = a.CreateTopic(topicName, &sarama.TopicDetail{
			NumPartitions:     int32(q.cfg.DefaultTopicConfig.NumPartitions),
			ReplicationFactor: int16(q.cfg.DefaultTopicConfig.ReplicationFactor),
		}, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) SetTopicConfig(topic string, entries map[string]*string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_3_0_0
	a, err := sarama.NewClusterAdmin(q.cfg.Brokers, cfg)
	if err != nil {
		return err
	}
	defer a.Close()
	err = a.AlterConfig(sarama.TopicResource, topic, entries, false)
	if err != nil {
		return err
	}
	var names []string
	for name, _ := range entries {
		names = append(names, name)
	}
	res, err := a.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topic,
		ConfigNames: names,
	})
	if err != nil {
		return err
	}
	for _, cfg := range res {
		q.logger.Infof("for topic %v setting %v updated and has value %v", topic, cfg.Name, cfg.Value)
	}
	return nil
}

//Returns consumer lag for given topic, if topic previously was registered in adapter by RegisterReader
//Returns error if context was closed or topic reader wasn't registered yet
func (q *Queue) GetConsumerLagForSinglePartition(ctx context.Context, topicName string) (int64, error) {
	newest, err := q.srm.GetOffset(topicName, 0, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}
	q.offsetLock.RLock()
	defer q.offsetLock.RUnlock()
	val, ok := q.readerOffsets[topicName]
	if !ok {
		return -1, nil
	}
	lag := atomic.LoadInt64(val)
	return newest - lag - 1, nil
}
