package mqttprovider

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type Consumer struct {
	lastInsertTime time.Time
	messageChan    chan ConsumeMessage // to listen for the message recieved events
	messages       []ConsumeMessage    // messages collecting to batch process
}

type ConsumeMessage struct {
	msg mqtt.Message
}

// MessageReciever recieves the messages from mqtt broker
func (c *Consumer) MessageReciever(client mqtt.Client, msg mqtt.Message) {
	// Publish the incoming messages from mqtt broker to messageChan
	c.messageChan <- ConsumeMessage{
		msg: msg,
	}
}

// MessageProcessor processes the messages batch wise
func (c *Consumer) MessageProcessor(ctx context.Context) {
	c.lastInsertTime = time.Now()
	// to make the consumer wait till 50 milliseconds
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Message processor closed")
			return
		case <-ticker.C:
			// for each interval checking any messages are presents in messages slice and then processes
			if len(c.messages) > 0 {
				c.InsertRecords(ctx)
			}
		case msg := <-c.messageChan:
			logger.Debugf("Message topic:%s, offset:%d", msg.msg.Topic(), msg.msg.MessageID())
			// append incoming messages to a slice if the last insert time isn't the configured value proceed further
			c.messages = append(c.messages, msg)
			if time.Since(c.lastInsertTime) < 50 {
				continue
			}
			// last insetion is more than confgiured value proced further to process the records
			c.InsertRecords(ctx)
		}
	}
}

// InsertRecords parses the incoming messages and process it
func (c *Consumer) InsertRecords(ctx context.Context) {
	logger.Infof("total records are going to process %v", len(c.messages))
	// giving the acknowledgements after processing
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
