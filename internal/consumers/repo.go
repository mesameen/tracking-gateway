package consumers

import (
	"context"
	"fmt"

	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/consumers/kafkaconsumer"
	"github.com/mesameen/tracking-gateway/internal/consumers/mqttconsumer"
)

const (
	MQTT  = "mqtt"
	Kafka = "kafka"
)

type Repo interface {
	StartConsume(ctx context.Context) error
	MessageProcessor(ctx context.Context)
	Close(ctx context.Context) error
}

func NewConsumer(ctx context.Context) (Repo, error) {
	switch config.CommonConfig.QueueName {
	case MQTT:
		return mqttconsumer.Init(ctx)
	case Kafka:
		return kafkaconsumer.Init(ctx)
	default:
		return nil, fmt.Errorf("unsupported Queue: %s", config.CommonConfig.QueueName)
	}
}
