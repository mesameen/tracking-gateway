package logger

import (
	"fmt"

	"go.uber.org/zap"
)

var log *zap.Logger

func InitiLogger() error {
	var err error
	log, err = zap.NewDevelopment()
	if err != nil {
		return err
	}
	return nil
}

func Infof(format string, values ...interface{}) {
	log.Info(fmt.Sprintf(format, values...))
}

func Errorf(format string, values ...interface{}) {
	log.Error(fmt.Sprintf(format, values...))
}

func Panicf(format string, values ...interface{}) {
	log.Panic(fmt.Sprintf(format, values...))
}

func Debugf(format string, values ...interface{}) {
	log.Debug(fmt.Sprintf(format, values...))
}
