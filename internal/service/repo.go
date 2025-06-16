package service

import (
	"context"
	"fmt"

	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/mqttprovider"
)

const (
	MQTT = "mqtt"
)

type Repo interface {
	StartConsume(ctx context.Context) error
	Close(ctx context.Context) error
}

func NewService(ctx context.Context) (Repo, error) {
	switch config.CommonConfig.QueueName {
	case MQTT:
		return mqttprovider.Init(ctx)
	default:
		return nil, fmt.Errorf("unsupported Queue: %s", config.CommonConfig.QueueName)
	}
}
