package kafkaadapt

type Config interface {
	GetString(name string) (string, error)
	GetInt(name string) (int, error)
}

type Logger interface {
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

type Queue interface {
	Put(queue string, data []byte) error
	Get(queue string) (Message, error)
	Close()
}

type Message interface {
	Data() []byte
	Ack() error
	Nack() error
}
