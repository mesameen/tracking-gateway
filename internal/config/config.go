package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

type CommonConfiguration struct {
	PodName   string `default:"tracking-gateway"`
	QueueName string `default:"mqtt"`
}

type MQTTConfiguration struct {
	Broker               string `default:"tcp://localhost:1883"`
	LocationTopic        string `default:"devices/location"`          // any consumer can process the location data
	CommandsTopic        string `default:"devices/commands"`          // device_id is attached to seperate the device specific command
	CommandResponseTopic string `default:"devices/commands/response"` // any consumer can process the responses
}

type KafkaConfiguration struct {
	Brokers       string `default:"localhost:9092"`
	ConsumerGroup string `default:"tracking-gateway"`
	LocationTopic string `default:"location-info"`
}

var CommonConfig CommonConfiguration
var MQTTConfig MQTTConfiguration
var KafkaConfig KafkaConfiguration

func InitConfig() {
	if err := envconfig.Process("", &CommonConfig); err != nil {
		log.Panicf("CommongConfig failed. Error: %v", err)
	}
	log.Printf("CommonConfig: %+v", CommonConfig)
	if err := envconfig.Process("MQTT", &MQTTConfig); err != nil {
		log.Panicf("MQTTConfig failed. Error: %v", err)
	}
	log.Printf("MQTTConfig: %+v", MQTTConfig)

	if err := envconfig.Process("KAFKA", &KafkaConfig); err != nil {
		log.Panicf("KafkaConfig failed. Error: %v", err)
	}
	log.Printf("KafkaConfig: %+v", KafkaConfig)
}
