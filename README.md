# Kafka Adapter

Adapter provides possibility to work with kafka topics for writing messages and reading with Ack/Nack functionality.

Two main interfaces in `Queue` (instance of adapter) and `Message`

`Queue.PutWithCtx(ctx context.Context, topic string, data []byte) error`  - puts given data into topic, returns error when ctx was closed

`Queue.Put(topic string, data []byte) error`  - the same method, but with context.Background() ctx.

`Queue.GetWithCtx(ctx context.Context, topic string) (Message, error)` - gets single message from topic, returns error when ctx was closed

`Queue.Get(topic string) (Message, error)` - the same method, but with context.Background() ctx.

`Message.Data() []byte` - returns kafka message body

`Message.Ack() error` - acquires message, incrementing kafka's partition offset

`Message.Nack() error` - unacquires message, with reader re-establishing, to enable other consumers within consumer group to read this message. NOTE: to enable this functionality - summ of Concurrency param on all Consumers with same ConsumerGroupID must be higher than topic's partition count


#### Important:
If ConsumerGroupID was not set in config (or equals to empty string), then each message would be auto-acked, 
and acquiring (msg.Ack()/msg.Nack()) would return "unavailable when GroupID is not set" error. 

#### Example:
```
import (
    queue "github.com/iddqdeika/kafka-adapter"
    "log"
 )
 
 func example() {
    topic := "my_topic"
    consumerGroup := "my_group"
    broker := "my-kafka-host.lan:9092"
    messageToSend := []byte("some message")
    
    cfg := queue.KafkaCfg{
		Concurrency:       100,
		QueueToReadNames:  []string{topic},
		QueueToWriteNames: []string{topic},
		Brokers:           []string{broker},
		ConsumerGroupID:   consumerGroup,
		DefaultTopicConfig: struct {
			NumPartitions     int
			ReplicationFactor int
		}{NumPartitions: 1, ReplicationFactor: 1},
	}

	log.Printf("starting adapter")
	q, err := queue.FromStruct(cfg, queue.DefaultLogger)
	if err != nil {
		log.Fatalf("cant init kafka adapter: %v", err)
	}
	defer q.Close()
	log.Printf("adapter started")

	log.Printf("putting message")
	err = q.Put(topic, messageToSend)
	if err != nil {
		log.Fatalf("cant put message in topic: %v", err)
	}
	log.Printf("message put")

	log.Printf("getting message")
	msg, err := q.Get(topic)
	if err != nil {
		log.Fatalf("cant get message from topic: %v", err)
	}
	message := string(msg.Data())
	log.Printf("message got: %v", message)

	log.Printf("acking message")
	err = msg.Ack()
	if err != nil {
		log.Fatalf("cant ack message: %v", err)
	}
	log.Printf("message acked")
	q.Close()
	log.Printf("adapter closed")
 }
```

### Additional:
FromConfig(cfg Config, logger Logger) - constructor, which uses interface Config
LoadJsonConfig(filename string) - Config implementation, loading data from json file
DefaultLogger - default implementation of logger, just forwards all errors to fmt.Pringf method
