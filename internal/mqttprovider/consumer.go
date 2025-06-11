package mqttprovider

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type Consumer struct {
	lastInsertTime time.Time
	messageChan    chan ConsumeMessage
	messages       []ConsumeMessage
}

type ConsumeMessage struct {
	msg mqtt.Message
}

func NewConsumer(ctx context.Context) (*Consumer, error) {
	return &Consumer{
		messageChan: make(chan ConsumeMessage, 100000),
		messages:    make([]ConsumeMessage, 0),
	}, nil
}

func (c *Consumer) MessageReciever(client mqtt.Client, msg mqtt.Message) {
	c.messageChan <- ConsumeMessage{
		msg: msg,
	}
}

func (c *Consumer) MessageProcessor(ctx context.Context) {
	c.lastInsertTime = time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Message processor closed")
			return
		case <-ticker.C:
			if len(c.messages) > 0 {
				c.InsertRecords(ctx)
			}
		case msg := <-c.messageChan:
			logger.Debugf("Message topic:%s, offset:%d", msg.msg.Topic(), msg.msg.MessageID())
			c.messages = append(c.messages, msg)
			if time.Since(c.lastInsertTime) < 50 {
				continue
			}
			c.InsertRecords(ctx)
		}
	}
}

func (c *Consumer) InsertRecords(ctx context.Context) {
	logger.Infof("total records are going to process %v", len(c.messages))
	for _, msg := range c.messages {
		msg.msg.Ack()
	}
	c.messages = make([]ConsumeMessage, 0)
	c.lastInsertTime = time.Now()
}

func (c *Consumer) Close(ctx context.Context) error {
	logger.Infof("Closing the consumer")
	// closing the message chan
	close(c.messageChan)
	return nil
}
