package kafkaadapt

import (
	"context"
	"fmt"
	sarama "github.com/Shopify/sarama"
	kafka "github.com/segmentio/kafka-go"
	"strings"
	"sync"
)

var ErrClosed = fmt.Errorf("kafka adapter is closed")
var ErrAsyncNack = fmt.Errorf("nack is inapplicable in async message acking mode")

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
		DefaultTopicConfig: DefaultTopicConfig{
			NumPartitions:     pnum,
			ReplicationFactor: rfactor,
		},
	}, logger)
}

func newKafkaQueue(cfg KafkaCfg, logger Logger) (*Queue, error) {
	if cfg.DefaultTopicConfig.NumPartitions < 1 {
		return nil, fmt.Errorf("incorrect DefaultTopicConfig, numpartitions must be more than 1")
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

	DefaultTopicConfig DefaultTopicConfig
}

type DefaultTopicConfig struct {
	NumPartitions     int
	ReplicationFactor int
}

type Queue struct {
	cfg        KafkaCfg
	logger     Logger
	c          *kafka.Client
	readers    map[string]chan *kafka.Reader
	readerLags map[string]int64
	messages   map[string]chan *Message
	writers    map[string]*kafka.Writer
	closed     chan struct{}

	m sync.RWMutex
}

func (q *Queue) init() error {

	if q.cfg.Concurrency < 1 {
		q.cfg.Concurrency = 1
	}

	q.readers = make(map[string]chan *kafka.Reader)
	q.messages = make(map[string]chan *Message)
	q.writers = make(map[string]*kafka.Writer)
	q.closed = make(chan struct{})

	//some checkup
	for _, b := range q.cfg.Brokers {
		conn, err := kafka.Dial("tcp", b)
		if err != nil {
			return fmt.Errorf("cant connect to broker %v, err: %v", b, err)
		}
		conn.Close()
	}

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
	if _, ok := q.writers[topic]; ok {
		return
	}
	if topic == "" {
		return
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   q.cfg.Brokers,
		BatchSize: q.cfg.BatchSize,
		Async:     q.cfg.Async,
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
	})
	q.writers[topic] = w
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
	q.m.Lock()
	q.readerLags[r.Config().Topic] = r.Lag()
	q.m.Unlock()
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
	defer q.m.RUnlock()
	if w, ok := q.writers[queue]; ok {
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
		return nil, fmt.Errorf("context was closed")
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
	for _, w := range q.writers {
		go func() {
			//оставляем это без waitgroup, т.к. в пакете kafka-go баг.
			//если writemessages закрывается до того, как все результаты внутренних ретраев были считаны
			//например, при закрытии контекста
			//то врайтер повисает в воздухе:
			//writemessages не читает результаты из внутреннего writer
			//а внутренния writer.write блокируется в попытке записать результаты.
			err := w.Close()
			if err != nil {
				q.logger.Errorf("err during writer closing: %v", err)
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
	if _, ok := topics[topicName]; !ok {
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

//Returns consumer lag for given topic, if topic previously was registered in adapter by RegisterReader
//Returns error if context was closed or topic reader wasn't registered yet
func (q *Queue) GetConsumerLag(ctx context.Context, topicName string) (int64, error) {
	lag, ok := q.readerLags[topicName]
	if !ok {
		return 0, fmt.Errorf("reader for topic topicName not registered")
	}
	return lag, nil
}

type Message struct {
	msg    *kafka.Message
	reader *kafka.Reader
	rch    chan *kafka.Reader
	once   sync.Once
	async  bool
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
