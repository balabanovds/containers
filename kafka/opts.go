package kafka

import "log"

type Opt func(*KafkaContainer)

// WithPort set custom port for container binding.
func WithPort(port int) Opt {
	return func(kc *KafkaContainer) {
		kc.extPort = port
	}
}

// WithTopic set custom Kafka topic for creation within container initialization.
func WithTopic(topic string) Opt {
	return func(kc *KafkaContainer) {
		kc.topic = topic
	}
}

// WithLogger use custom logger
func WithLogger(l *log.Logger) Opt {
	return func(kc *KafkaContainer) {
		kc.log = l
	}
}
