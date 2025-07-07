package mqttutil

import (
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

type Provider struct {
	client mqtt.Client
}

func Init(ctx context.Context) (*Provider, error) {
	client, err := connectToClient(ctx)
	if err != nil {
		return nil, err
	}
	return &Provider{
		client: client,
	}, nil
}

func (m *Provider) Subscribe(ctx context.Context, topic string, qos byte, handler mqtt.MessageHandler) error {
	token := m.client.Subscribe(topic, qos, handler)
	if token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to connect mqtt broker. Error: %v", token.Error())
		return token.Error()
	}
	logger.Infof("Subscribed to the topic %v successfully", topic)
	return nil
}

func (m *Provider) UnSubscribe(ctx context.Context, topics []string) error {
	if token := m.client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to unsubscribe topics %v mqtt broker. Error: %v", topics, token.Error())
		return token.Error()
	}
	return nil
}

func (m *Provider) Close(ctx context.Context) error {
	logger.Infof("Disconnecting from MQTT provider")
	m.client.Disconnect(250) // wait for some time (milliseconds) till current work completes
	return nil
}
