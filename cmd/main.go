package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
	"github.com/mesameen/tracking-gateway/internal/service"
)

func main() {
	config.InitConfig()
	if err := logger.InitiLogger(); err != nil {
		log.Panic(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	prov, err := service.NewService(ctx)
	if err != nil {
		logger.Panicf("Failed to start mqtt provider. Error: %v", err)
	}
	// started consuming messages
	err = prov.StartConsume(ctx)
	if err != nil {
		logger.Panicf("Failed to consume mqtt messages. Error: %v", err)
	}
	prov.Close(ctx)
	logger.Infof("Shutting down tracking gateway Job .....")
}
