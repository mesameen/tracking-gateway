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
	Broker string `default:"tcp://localhost:1883"`
	Topic  string `default:"$share/tracking-gateway/devices/status"`
}

var CommonConfig CommonConfiguration
var MQTTConfig MQTTConfiguration

func InitConfig() {
	if err := envconfig.Process("", &CommonConfig); err != nil {
		log.Panicf("CommongConfig failed. Error: %v", err)
	}
	log.Printf("CommonConfig: %+v", CommonConfig)
	if err := envconfig.Process("MQTT", &MQTTConfig); err != nil {
		log.Panicf("MQTTConfig failed. Error: %v", err)
	}
	log.Printf("MQTTConfig: %+v", MQTTConfig)
}
