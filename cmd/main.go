package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mesameen/tracking-gateway/internal/config"
	"github.com/mesameen/tracking-gateway/internal/logger"
	"github.com/mesameen/tracking-gateway/internal/mqttprovider"
)

func main() {
	config.InitConfig()
	if err := logger.InitiLogger(); err != nil {
		log.Panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	prov, err := mqttprovider.Init(ctx)
	if err != nil {
		logger.Panicf("Failed to start mqtt provider. Error: %v", err)
	}
	onInterrupt(cancel)
	// started consuming messages
	err = prov.StartConsume(ctx)
	if err != nil {
		logger.Panicf("Failed to consume mqtt messages. Error: %v", err)
	}
	prov.Close(ctx)
}

func onInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Infof("Shutting down data transfer Job .....")
		cancel()
	}()
}
