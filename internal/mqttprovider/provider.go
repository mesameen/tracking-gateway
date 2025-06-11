package mqttprovider

import (
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type MQTTProvider struct {
	client mqtt.Client
}

func Init(ctx context.Context) (*MQTTProvider, error) {
	client, err := connectToClient(ctx)
	if err != nil {
		return nil, err
	}
	return &MQTTProvider{
		client: client,
	}, nil
}

func (m *MQTTProvider) StartConsume(ctx context.Context) error {
	// creating a consumer to handle the messages
	consumer := &Consumer{
		messageChan: make(chan ConsumeMessage, 100000),
		messages:    make([]ConsumeMessage, 0),
	}
	// running message processor as go routine to listen the messages to process
	go consumer.MessageProcessor(ctx)
	// subscribing to the topic and passing the message reciever to consume messages
	err := m.Subscribe(ctx, config.MQTTConfig.Topic, 1, consumer.MessageReciever)
	if err != nil {
		logger.Panicf("Failed to subscribe to the mqtt topic %s. Error: %v", config.MQTTConfig.Topic, err)
	}
	// waiting till cancellation
	<-ctx.Done()
	// clearing of consumer
	consumer.Close(ctx)
	return nil
}

func (m *MQTTProvider) Subscribe(ctx context.Context, topic string, qos byte, handler mqtt.MessageHandler) error {
	token := m.client.Subscribe(topic, qos, handler)
	if token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to connect mqtt broker. Error: %v", token.Error())
		return token.Error()
	}
	logger.Infof("Subscribed to the topic %v successfully", config.MQTTConfig.Topic)
	return nil
}

func (m *MQTTProvider) UnSubscribe(ctx context.Context, topics []string) error {
	if token := m.client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to unsubscribe topics %v mqtt broker. Error: %v", topics, token.Error())
		return token.Error()
	}
	return nil
}

func (m *MQTTProvider) Close(ctx context.Context) error {
	logger.Infof("Disconnecting from MQTT provider")
	m.client.Disconnect(250) // wait for some time (milliseconds) till current work completes
	return nil
}
