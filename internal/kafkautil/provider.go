package kafkautil

import (
	"context"

	"github.com/IBM/sarama"
)

type Provider struct {
	client        sarama.Client
	ConsumerGroup sarama.ConsumerGroup
}

func Init(ctx context.Context) (*Provider, error) {
	client, group, err := connectToClient()
	if err != nil {
		return nil, err
	}
	return &Provider{
		client:        client,
		ConsumerGroup: group,
	}, nil
}

func (kp *Provider) Close(ctx context.Context) error {
	err := kp.client.Close()
	if err != nil {
		return err
	}
	err = kp.ConsumerGroup.Close()
	if err != nil {
		return err
	}
	return nil
}
