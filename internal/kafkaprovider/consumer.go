package kafkaprovider

import (
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	lastInsertTime time.Time
	messageChan    chan ConsumeMessage
	messages       []ConsumeMessage
}

type ConsumeMessage struct {
	msg sarama.Message
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
