package kafkaadapt

import "fmt"

var DefaultLogger Logger = &defaultLogger{}

type defaultLogger struct {
}

func (d defaultLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format+"\r\n", args...)
}

func (d defaultLogger) Infof(format string, args ...interface{}) {

}
