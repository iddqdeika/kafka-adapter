package kafkaadapt

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync"
)

type Message struct {
	msg             *kafka.Message
	reader          *kafka.Reader
	rch             chan *kafka.Reader
	once            sync.Once
	async           bool
	needack         bool
	actualizeOffset func(o int64)
}

func (k *Message) Data() []byte {
	return k.msg.Value
}

func (k *Message) KVData() *KV {
	return &KV{
		Key:   k.msg.Key,
		Value: k.msg.Value,
	}
}

func (k *Message) Offset() int64 {
	return k.msg.Offset
}

func (k *Message) returnReader() {
	k.rch <- k.reader
}

func (k *Message) returnNewReader() {
	k.rch <- kafka.NewReader(k.reader.Config())
	k.reader.Close()
}

func (k *Message) Ack() error {
	k.actualizeOffset(k.msg.Offset)
	k.once.Do(k.returnReader)
	if !k.needack {
		return nil
	}
	err := k.reader.CommitMessages(context.Background(), *k.msg)
	return err
}

func (k *Message) Nack() error {
	if k.async {
		return ErrAsyncNack
	}
	k.once.Do(k.returnNewReader)
	return nil
}
