package kafkaprovider

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type KafkaProvider struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
}

func Init(ctx context.Context) (*KafkaProvider, error) {
	client, group, err := connectToClient()
	if err != nil {
		return nil, err
	}
	return &KafkaProvider{
		client:        client,
		consumerGroup: group,
	}, nil
}

func (kp *KafkaProvider) StartConsume(ctx context.Context) error {
	consumer := &Consumer{
		messageChan: make(chan ConsumeMessage, 100000),
		messages:    make([]ConsumeMessage, 0),
	}
	for {
		if err := kp.consumerGroup.Consume(ctx, []string{config.KafkaConfig.LocationTopic}, consumer); err != nil {
			logger.Errorf("Failed to start consuming messages from kafka. Error: %v", err)
			return err
		}
	}
	return nil
}

func (kp *KafkaProvider) Close(ctx context.Context) error {
	err := kp.client.Close()
	if err != nil {
		return err
	}
	err = kp.consumerGroup.Close()
	if err != nil {
		return err
	}
	return nil
}
