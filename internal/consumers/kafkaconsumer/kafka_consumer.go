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
	lastProcessedTime time.Time
	messageChan       chan ConsumeMessage
	messages          []ConsumeMessage
	provider          *kafkautil.Provider
}

type ConsumeMessage struct {
	msg     *sarama.ConsumerMessage
	session sarama.ConsumerGroupSession
}

func Init(ctx context.Context) (*Consumer, error) {
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
	// running message processor as go routine to listen the messages to process
	go c.MessageProcessor(ctx)
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
	logger.Infof("Cleaning up kafka consumer")
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			logger.Infof("Message recieved: %v", string(msg.Value))
			// pushing to buffered channel to allow the consumer reads asynchronously and process the records batch wise
			c.messageChan <- ConsumeMessage{
				msg:     msg,
				session: session,
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) MessageProcessor(ctx context.Context) {
	c.lastProcessedTime = time.Now()
	// to wait the consumer for 50 milliseconds
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(c.messages) > 0 {
				c.ProcessRecords(ctx)
			}
		case msg := <-c.messageChan:
			logger.Infof("Message recieved: %v", string(msg.msg.Value))

			if msg.msg == nil {
				continue
			}
			logger.Debugf("Message topic: %s, partition: %d, offset:%d", msg.msg.Topic, msg.msg.Partition, msg.msg.Offset)
			c.messages = append(c.messages, msg)
			if time.Since(c.lastProcessedTime).Milliseconds() > 50 {
				continue
			}
			c.ProcessRecords(ctx)
		}
	}
}

func (c *Consumer) ProcessRecords(ctx context.Context) {
	logger.Infof("total records are going to process: %v", len(c.messages))
	// giving the ack after processing the records
	for _, msg := range c.messages {
		msg.session.MarkMessage(msg.msg, "")
	}
	c.messages = make([]ConsumeMessage, 0)
	c.lastProcessedTime = time.Now()
}

func (c *Consumer) Close(ctx context.Context) error {
	logger.Infof("Closing the kafka consumer")
	// closing the message chan
	close(c.messageChan)
	c.provider.Close(ctx)
	return nil
}
