package mqttconsumer

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
	"github.com/mesameen/tracking-gateway/internal/mqttutil"
)

type Consumer struct {
	lastProcessedTime time.Time
	messageChan       chan MQTTConsumeMessage // to listen for the message recieved events
	messages          []MQTTConsumeMessage    // messages collecting to batch process
	provider          *mqttutil.Provider
}

type MQTTConsumeMessage struct {
	msg mqtt.Message
}

func Init(ctx context.Context) (*Consumer, error) {
	provider, err := mqttutil.Init(ctx)
	if err != nil {
		return nil, err
	}
	// creating a consumer to handle the messages
	return &Consumer{
		messageChan: make(chan MQTTConsumeMessage, 100000),
		messages:    make([]MQTTConsumeMessage, 0),
		provider:    provider,
	}, nil
}

// MessageReciever recieves the messages from mqtt broker
func (c *Consumer) MessageReciever(client mqtt.Client, msg mqtt.Message) {
	// Publish the incoming messages from mqtt broker to messageChan
	c.messageChan <- MQTTConsumeMessage{
		msg: msg,
	}
}

// MessageProcessor processes the messages batch wise
func (c *Consumer) MessageProcessor(ctx context.Context) {
	c.lastProcessedTime = time.Now()
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
				c.ProcessRecords(ctx)
			}
		case msg := <-c.messageChan:
			logger.Debugf("Message topic:%s, offset:%d", msg.msg.Topic(), msg.msg.MessageID())
			// append incoming messages to a slice if the last insert time isn't the configured value proceed further
			c.messages = append(c.messages, msg)
			if time.Since(c.lastProcessedTime) < 50 {
				continue
			}
			// last insetion is more than confgiured value proced further to process the records
			c.ProcessRecords(ctx)
		}
	}
}

// InsertRecords parses the incoming messages and process it
func (c *Consumer) ProcessRecords(ctx context.Context) {
	logger.Infof("total records are going to process %v", len(c.messages))
	// giving the acknowledgements after processing
	for _, msg := range c.messages {
		msg.msg.Ack()
	}
	c.messages = make([]MQTTConsumeMessage, 0)
	c.lastProcessedTime = time.Now()
}

func (c *Consumer) Close(ctx context.Context) error {
	logger.Infof("Closing the mqtt consumer")
	// closing the message chan
	close(c.messageChan)
	c.provider.Close(ctx)
	return nil
}

func (c *Consumer) StartConsume(ctx context.Context) error {
	// running message processor as go routine to listen the messages to process
	go c.MessageProcessor(ctx)
	// subscribing to the topic and passing the message reciever to consume messages
	err := c.provider.Subscribe(ctx, config.MQTTConfig.LocationTopic, 1, c.MessageReciever)
	if err != nil {
		logger.Panicf("Failed to subscribe to the mqtt topic %s. Error: %v", config.MQTTConfig.LocationTopic, err)
	}
	// subscribing to the command responses topic
	err = c.provider.Subscribe(ctx, config.MQTTConfig.CommandResponseTopic, 1, c.MessageReciever)
	if err != nil {
		logger.Panicf("Failed to subscribe to the mqtt topic %s. Error: %v", config.MQTTConfig.LocationTopic, err)
	}
	// waiting till cancellation
	<-ctx.Done()
	// clearing of consumer
	c.Close(ctx)
	return nil
}
