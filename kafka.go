package kafkaadapt

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"strings"
	"sync"
)

var ErrClosed error = fmt.Errorf("kafka adapter is closed")

func FromStruct(cfg KafkaCfg, logger Logger) (Queue, error) {
	return newKafkaQueue(cfg, logger)
}

func FromConfig(cfg Config, logger Logger) (Queue, error) {
	brokers, err := cfg.GetString("KAFKA.BROKERS")
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
	return newKafkaQueue(KafkaCfg{
		Concurrency:       200,
		QueueToReadNames:  strings.Split(queuesToRead, ";"),
		QueueToWriteNames: strings.Split(queuesToWrite, ";"),
		Brokers:           strings.Split(brokers, ";"),
		ConsumerGroupID:   consumerGroup,
	}, logger)
}

func newKafkaQueue(cfg KafkaCfg, logger Logger) (Queue, error) {
	q := &kafkaQueueImpl{
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

	QueueToReadNames  []string
	QueueToWriteNames []string

	Brokers []string

	ConsumerGroupID string
}

type kafkaQueueImpl struct {
	cfg      KafkaCfg
	logger   Logger
	c        *kafka.Client
	readers  map[string]chan *kafka.Reader
	messages map[string]chan *kafkaMessageImpl
	writers  map[string]*kafka.Writer
	closed   chan struct{}
}

func (k *kafkaQueueImpl) init() error {

	//lets check some shit
	if len(k.cfg.QueueToReadNames) < 1 && len(k.cfg.QueueToWriteNames) < 1 {
		return fmt.Errorf("must be at least one value in QueueNames in cfg")
	}
	if k.cfg.Concurrency < 1 {
		return fmt.Errorf("must be at least one value in Concurrency in cfg")
	}

	k.readers = make(map[string]chan *kafka.Reader)
	k.messages = make(map[string]chan *kafkaMessageImpl)
	k.writers = make(map[string]*kafka.Writer)
	k.closed = make(chan struct{})

	//fill readers
	for _, topic := range k.cfg.QueueToReadNames {
		if topic == "" {
			continue
		}
		ch := make(chan *kafka.Reader, k.cfg.Concurrency)
		msgChan := make(chan *kafkaMessageImpl)
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
	//fill writers
	for _, topic := range k.cfg.QueueToWriteNames {
		if topic == "" {
			continue
		}
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers:  k.cfg.Brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		})
		k.writers[topic] = w
	}
	return nil
}

func (k *kafkaQueueImpl) produceMessages(rch chan *kafka.Reader, ch chan *kafkaMessageImpl) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
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
			k.logger.Errorf("error during fetching message from kafka: %v", err)
			rch <- r
			continue
		}
		// суть в том, что ридер вернется в канал ридеров только при ack/nack, не раньше.
		// следующее сообщение с ридера читать нельзя, пока не будет ack/nack на предыдущем.
		mi := kafkaMessageImpl{
			msg:    &msg,
			reader: r,
			rch:    rch,
		}
		select {
		case ch <- &mi:
			k.logger.Infof("kafka message emitted from producer")
		case <-ctx.Done():
			err := r.Close()
			if err != nil {
				k.logger.Errorf("err during reader closing: %v", err)
			}
		}
	}
}

func (k *kafkaQueueImpl) Put(queue string, data []byte) error {
	select {
	case <-k.closed:
		return ErrClosed
	default:

	}
	if w, ok := k.writers[queue]; ok {
		err := w.WriteMessages(context.Background(), kafka.Message{Value: data})
		if err != nil {
			return fmt.Errorf("error during writing message to kafka: %v", err)
		}
		return nil
	}
	return fmt.Errorf("there is no such topic declared in config: %v", queue)
}

func (k *kafkaQueueImpl) Get(queue string) (Message, error) {
	select {
	case <-k.closed:
		return nil, ErrClosed
	default:

	}
	ctx := context.Background()
	mch, ok := k.messages[queue]
	if !ok {
		return nil, fmt.Errorf("there is no such topic declared in config: %v", queue)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context is closed")
	case <-k.closed:
		return nil, ErrClosed
	case msg := <-mch:
		return msg, nil
	}
}

func (k *kafkaQueueImpl) Close() {
	close(k.closed)
	for _, rchan := range k.readers {
		for {
			select {
			case r := <-rchan:
				err := r.Close()
				if err != nil {
					k.logger.Errorf("err during reader closing: %v", err)
				}
			default:

			}
		}
	}
	for _, w := range k.writers {
		err := w.Close()
		if err != nil {
			k.logger.Errorf("err during writer closing: %v", err)
		}
	}
}

type kafkaMessageImpl struct {
	msg    *kafka.Message
	reader *kafka.Reader
	rch    chan *kafka.Reader
	once   sync.Once
}

func (k *kafkaMessageImpl) Data() []byte {
	return k.msg.Value
}

func (k *kafkaMessageImpl) returnReader() {
	k.rch <- k.reader
}

func (k *kafkaMessageImpl) returnNewReader() {
	k.rch <- kafka.NewReader(k.reader.Config())
	k.reader.Close()
}

func (k *kafkaMessageImpl) Ack() error {
	err := k.reader.CommitMessages(context.Background(), *k.msg)
	k.once.Do(k.returnReader)
	return err
}

func (k *kafkaMessageImpl) Nack() error {
	k.once.Do(k.returnNewReader)
	return nil
}
