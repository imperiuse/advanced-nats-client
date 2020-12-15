// +build !prod

package logger

import "go.uber.org/zap"

type Logger = *zap.Logger

var Log Logger = func() Logger {
	l, _ := zap.NewDevelopment()
	return l
}()
