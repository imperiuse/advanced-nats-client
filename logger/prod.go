// +build !enable_zap_logger

package logger

import "go.uber.org/zap"

type Logger = *zap.Logger

var Log = func() Logger {
	l, _ := zap.NewProduction()
	return l
}()
