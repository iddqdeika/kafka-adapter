package kafkaadapt

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"strings"
	"sync"
)

var ErrClosed error = fmt.Errorf("kafka adapter is closed")

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
	return newKafkaQueue(KafkaCfg{
		Concurrency:       concurrency,
		QueueToReadNames:  strings.Split(queuesToRead, ";"),
		QueueToWriteNames: strings.Split(queuesToWrite, ";"),
		Brokers:           strings.Split(brokers, ";"),
		ControllerAddress: controller,
		ConsumerGroupID:   consumerGroup,
		BatchSize:         batchSize,
		Async:             async == 1,
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
	cfg      KafkaCfg
	logger   Logger
	c        *kafka.Client
	readers  map[string]chan *kafka.Reader
	messages map[string]chan *Message
	writers  map[string]*kafka.Writer
	closed   chan struct{}

	m sync.RWMutex
}

func (k *Queue) init() error {

	if k.cfg.Concurrency < 1 {
		k.cfg.Concurrency = 1
	}

	k.readers = make(map[string]chan *kafka.Reader)
	k.messages = make(map[string]chan *Message)
	k.writers = make(map[string]*kafka.Writer)
	k.closed = make(chan struct{})

	//some checkup
	for _, b := range k.cfg.Brokers {
		conn, err := kafka.Dial("tcp", b)
		if err != nil {
			return fmt.Errorf("cant connect to broker %v, err: %v", b, err)
		}
		conn.Close()
	}

	//fill readers
	for _, topic := range k.cfg.QueueToReadNames {
		k.ReaderRegister(topic)
	}
	//fill writers
	for _, topic := range k.cfg.QueueToWriteNames {
		k.WriterRegister(topic)
	}
	return nil
}

func (k *Queue) ReaderRegister(topic string) {
	k.m.Lock()
	defer k.m.Unlock()
	if _, ok := k.readers[topic]; ok {
		return
	}
	if topic == "" {
		return
	}
	ch := make(chan *kafka.Reader, k.cfg.Concurrency)
	msgChan := make(chan *Message)
	for i := 0; i < k.cfg.Concurrency; i++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  k.cfg.Brokers,
			GroupID:  k.cfg.ConsumerGroupID,
			Topic:    topic,
			MinBytes: 10e1,
			MaxBytes: 10e3,
		})
		ch <- r
		go k.produceMessages(ch, msgChan)
	}
	k.readers[topic] = ch
	k.messages[topic] = msgChan
}

func (k *Queue) WriterRegister(topic string) {
	k.m.Lock()
	defer k.m.Unlock()
	if _, ok := k.writers[topic]; ok {
		return
	}
	if topic == "" {
		return
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   k.cfg.Brokers,
		BatchSize: k.cfg.BatchSize,
		Async:     k.cfg.Async,
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
	})
	k.writers[topic] = w
}

func (k *Queue) produceMessages(rch chan *kafka.Reader, ch chan *Message) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-k.closed:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	for {
		select {
		case <-k.closed:
			return
		default:
		}
		r := <-rch
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			k.logger.Errorf("error during kafka message fetching: %v", err)
			rch <- r
			continue
		}
		// суть в том, что ридер вернется в канал ридеров только при ack/nack, не раньше.
		// следующее сообщение с ридера читать нельзя, пока не будет ack/nack на предыдущем.
		mi := Message{
			msg:    &msg,
			reader: r,
			rch:    rch,
		}
		// если консумергруппа пуста, то месседжи подтверждаются автоматически и удерживать ридер нет смысла.
		if k.cfg.ConsumerGroupID == "" {
			mi.once.Do(mi.returnReader)
		}
		select {
		case ch <- &mi:
			k.logger.Infof("kafka Message emitted from producer")
		case <-ctx.Done():
			err := r.Close()
			if err != nil {
				k.logger.Errorf("err during reader closing: %v", err)
			}
		}
	}
}

func (k *Queue) Put(queue string, data []byte) error {
	ctx := context.Background()
	return k.PutWithCtx(ctx, queue, data)
}

func (k *Queue) PutWithCtx(ctx context.Context, queue string, data []byte) error {
	return k.PutBatchWithCtx(ctx, queue, data)
}

func (k *Queue) PutBatch(queue string, data ...[]byte) error {
	return k.PutBatchWithCtx(context.Background(), queue, data...)
}

func (k *Queue) PutBatchWithCtx(ctx context.Context, queue string, data ...[]byte) error {
	select {
	case <-k.closed:
		return ErrClosed
	default:

	}

	msgs := make([]kafka.Message, 0)
	for _, d := range data {
		msgs = append(msgs, kafka.Message{Value: d})
	}
	k.m.RLock()
	defer k.m.RUnlock()
	if w, ok := k.writers[queue]; ok {
		err := w.WriteMessages(ctx, msgs...)
		if err != nil {
			return fmt.Errorf("error during writing Message to kafka: %v", err)
		}
		return nil
	}
	return fmt.Errorf("there is no such topic declared in config: %v", queue)
}

func (k *Queue) Get(queue string) (*Message, error) {
	ctx := context.Background()
	return k.GetWithCtx(ctx, queue)
}

func (k *Queue) GetWithCtx(ctx context.Context, queue string) (*Message, error) {
	select {
	case <-k.closed:
		return nil, ErrClosed
	default:

	}

	k.m.RLock()
	defer k.m.RUnlock()
	mch, ok := k.messages[queue]
	if !ok {
		return nil, fmt.Errorf("there is no such topic declared in config: %v", queue)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context was closed")
	case <-k.closed:
		return nil, ErrClosed
	case msg := <-mch:
		return msg, nil
	}
}

func (k *Queue) Close() {
	close(k.closed)
	wg := sync.WaitGroup{}
	k.m.Lock()
	defer k.m.Unlock()
	for _, rchan := range k.readers {
	readers:
		for {
			select {
			case r := <-rchan:
				wg.Add(1)
				go func() {
					err := r.Close()
					if err != nil {
						k.logger.Errorf("err during reader closing: %v", err)
					}
					wg.Done()
				}()
			default:
				close(rchan)
				break readers
			}
		}
	}
	for _, w := range k.writers {
		wg.Add(1)
		go func() {
			err := w.Close()
			if err != nil {
				k.logger.Errorf("err during writer closing: %v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

//Ensures that topic with given name was created with background context
func (q *Queue) EnsureTopic(topicName string) error {
	return q.EnsureTopicWithCtx(context.Background(), topicName)
}

//Ensures that topic with given name was created
func (q *Queue) EnsureTopicWithCtx(ctx context.Context, topicName string) error {
	if q.cfg.ControllerAddress == "" {
		return fmt.Errorf("controller address was not set in cfg")
	}
	conn, err := kafka.DialContext(ctx, "tcp", q.cfg.ControllerAddress)
	if err != nil {
		return err
	}
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     q.cfg.DefaultTopicConfig.NumPartitions,
		ReplicationFactor: q.cfg.DefaultTopicConfig.ReplicationFactor,
	})
}

type Message struct {
	msg    *kafka.Message
	reader *kafka.Reader
	rch    chan *kafka.Reader
	once   sync.Once
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
	k.once.Do(k.returnNewReader)
	return nil
}
