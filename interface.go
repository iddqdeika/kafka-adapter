package kafkaadapt

type Config interface {
	GetString(name string) (string, error)
	GetInt(name string) (int, error)
}

type Logger interface {
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}
