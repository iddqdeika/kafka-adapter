package kafkaadapt

import (
	"context"
	"github.com/segmentio/kafka-go"
)

func NewTopicManager(tcpAdress string, dtc DefaultTopicConfig) *TopicManager {
	return &TopicManager{
		tcpAdress: tcpAdress,
		dtc:       dtc,
	}
}

type DefaultTopicConfig struct {
	NumPartitions     int
	ReplicationFactor int
}

type TopicManager struct {
	tcpAdress string
	dtc       DefaultTopicConfig
}

func (m *TopicManager) EnsureTopic(topicName string) error {
	m.EnsureTopicWithCtx(context.Background(), topicName)
}

func (m *TopicManager) EnsureTopicWithCtx(ctx context.Context, topicName string) error {
	conn, err := kafka.DialContext(ctx, "tcp", m.tcpAdress)
	if err != nil {
		return err
	}
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     m.dtc.NumPartitions,
		ReplicationFactor: m.dtc.ReplicationFactor,
	})
}
