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

	return newKafkaQueue(KafkaCfg{
		Concurrency:       concurrency,
		QueueToReadNames:  strings.Split(queuesToRead, ";"),
		QueueToWriteNames: strings.Split(queuesToWrite, ";"),
		Brokers:           strings.Split(brokers, ";"),
		ControllerAddress: controller,
		ConsumerGroupID:   consumerGroup,
		BatchSize:         batchSize,
		Async:             async == 1,
		AsyncAck:          asyncAck == 1,
		DefaultTopicConfig: TopicConfig{
			NumPartitions:     pnum,
			ReplicationFactor: rfactor,
		},
		CompressionCodec: codec,
	}, logger)
}

func newKafkaQueue(cfg KafkaCfg, logger Logger) (*Queue, error) {
	if cfg.DefaultTopicConfig.NumPartitions < 1 {
		return nil, fmt.Errorf("incorrect TopicConfig, numpartitions must be more than 1")
	}

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
	//thats why Put method will not return any error and will not be blocked
	//use it if you dont need delivery guarantee
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

	QueueToReadNames  []string
	QueueToWriteNames []string

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
	msgChan := make(chan *Message)
	for i := 0; i < q.cfg.Concurrency; i++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  q.cfg.Brokers,
			GroupID:  q.cfg.ConsumerGroupID,
			Topic:    topic,
			MinBytes: 10e1,
			MaxBytes: 10e5,
		})
		ch <- r
		go q.produceMessages(ch, msgChan)
	}
	q.readers[topic] = ch
	q.messages[topic] = msgChan
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
		msg:    &msg,
		reader: r,
		rch:    rch,
		actualizeOffset: func(o int64) {
			atomic.StoreInt64(q.readerOffsets[r.Config().Topic], o)
		},
	}
	// если консумергруппа пуста, то месседжи подтверждаются автоматически и удерживать ридер нет смысла.
	if q.cfg.ConsumerGroupID == "" {
		mi.once.Do(mi.returnReader)
	}
	// если асинхронное подтверждение, то месседжи подтверждаются в произвольном порядке и удерживать ридер нет смысла.
	if q.cfg.AsyncAck {
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

// KV - пара ключ-значение, которые можно использовать в качестве данных сообщения kafka
// ключ может быть пустым, но надо учитывать, что в топиках с компакцией по ключу, а не по дате, в таком случае
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

//Ensures that topic with given name was created with background context set
func (q *Queue) EnsureTopic(topicName string) error {
	return q.EnsureTopicWithCtx(context.Background(), topicName)
}

//Ensures that topic with given name was created
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

type Message struct {
	msg             *kafka.Message
	reader          *kafka.Reader
	rch             chan *kafka.Reader
	once            sync.Once
	async           bool
	actualizeOffset func(o int64)
}

func (k *Message) Data() []byte {
	return k.msg.Value
}

func (k *Message) returnReader() {
	k.rch <- k.reader
}

func (k *Message) returnNewReader() {
	k.rch <- kafka.NewReader(k.reader.Config())
	k.reader.Close()
}

func (k *Message) Ack() error {
	err := k.reader.CommitMessages(context.Background(), *k.msg)
	k.actualizeOffset(k.msg.Offset)
	k.once.Do(k.returnReader)
	return err
}

func (k *Message) Nack() error {
	if k.async {
		return ErrAsyncNack
	}
	k.once.Do(k.returnNewReader)
	return nil
}
