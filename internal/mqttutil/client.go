package mqttutil

import (
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

func connectToClient(_ context.Context) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.MQTTConfig.Broker)
	// should be unique
	opts.SetClientID(config.CommonConfig.PodName)
	opts.OnConnect = func(c mqtt.Client) {
		logger.Infof("Connected to mqtt broker")
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		logger.Panicf("Disconnected from mqtt broker. Error: %v", err)
	}
	opts.DefaultPublishHandler = func(c mqtt.Client, m mqtt.Message) {
		logger.Infof("Received message: %s from topic: %s\n", string(m.Payload()), m.Topic())
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to connect mqtt broker. Error: %v", token.Error())
		return nil, token.Error()
	}
	return client, nil
}
