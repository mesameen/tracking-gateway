package kafkautil

import (
	"github.com/IBM/sarama"
	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
)

func connectToClient() (sarama.Client, sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = config.CommonConfig.PodName
	// Consumer config
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewClient([]string{config.KafkaConfig.Brokers}, kafkaConfig)
	if err != nil {
		logger.Errorf("Failed to connect kafka client. Error: %v", err)
		return nil, nil, err
	}
	cg, err := sarama.NewConsumerGroup([]string{config.KafkaConfig.Brokers}, config.KafkaConfig.ConsumerGroup, kafkaConfig)
	if err != nil {
		logger.Errorf("Failed to connect to kafka consumer group. Error: %v", err)
		return nil, nil, err
	}
	return client, cg, nil
}
