# Kafka Adapter

Адаптер предоставляет возможность работать с нужными топиками кафка на чтение и запись сообщений с их подтверждением.


`import (
 	queue "bitbucket.goods.ru/PIM/kafka-adapter"
 	"log"
 )
 
 func simple() {
 	topic := "my_topic"
 	broker := "kafka-01-croc.test.lan:9092"
 	messageToSend := []byte("some message")
 
 	cfg := queue.KafkaCfg{
 		Concurrency:       1,
 		QueueToReadNames:  []string{topic},
 		QueueToWriteNames: []string{topic},
 		Brokers:           []string{broker},
 		ConsumerGroupID:   "",
 	}
 
 	q, err := queue.FromStruct(cfg, queue.DefaultLogger)
 	if err != nil{
 		log.Fatalf("cant init kafka adapter: %v", err)
 	}
 	defer q.Close()
 
 	err = q.Put(topic, messageToSend)
 	if err != nil{
 		log.Fatalf("cant put message in topic: %v", err)
 	}
 
 	msg, err := q.Get(topic)
 	if err != nil{
 		log.Fatalf("cant get message from topic: %v", err)
 	}
 
 	message := string(msg.Data())
 	log.Printf("message got: %v", message)
 
 	err = msg.Ack()
 	if err != nil{
 		log.Fatalf("cant ack message: %v", err)
 	}
 }`