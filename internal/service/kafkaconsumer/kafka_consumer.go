package kafkaconsumer

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/kafkautil"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type Consumer struct {
	lastInsertTime time.Time
	messageChan    chan ConsumeMessage
	messages       []ConsumeMessage
	provider       *kafkautil.Provider
}

type ConsumeMessage struct {
	msg sarama.Message
}

func InitConsumer(ctx context.Context) (*Consumer, error) {
	provider, err := kafkautil.Init(ctx)
	if err != nil {
		return nil, err
	}
	// creating a consumer to handle the messages
	return &Consumer{
		messageChan: make(chan ConsumeMessage, 100000),
		messages:    make([]ConsumeMessage, 0),
		provider:    provider,
	}, nil
}

func (c *Consumer) StartConsume(ctx context.Context) error {
	consumerCtx, cancel := context.WithCancel(ctx)
	go func() {
		if err := c.provider.ConsumerGroup.Consume(consumerCtx, []string{config.KafkaConfig.LocationTopic}, c); err != nil {
			logger.Errorf("Error from consumer for the topics %v. Error: %v", []string{config.KafkaConfig.LocationTopic}, err)
		}
	}()
	// waiting till cancellation
	<-ctx.Done()
	cancel()
	// clearing of consumer
	c.Close(ctx)
	return nil
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	logger.Infof("Closing the kafka consumer")
	// closing the message chan
	close(c.messageChan)
	c.provider.Close(ctx)
	return nil
}
