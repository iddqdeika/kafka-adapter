module github.com/iddqdeika/kafka-adapter

go 1.13

require (
	github.com/Shopify/sarama v1.26.4
	github.com/segmentio/kafka-go v0.4.28
)

replace github.com/segmentio/kafka-go v0.4.28 => github.com/iddqdeika/kafka-go v0.4.28-fix
