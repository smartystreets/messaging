package handlers

import (
	"log"
	"time"

	"github.com/smartystreets/messaging/v3"
)

var RetryOptions retrySingleton

type retrySingleton struct{}
type retryOption func(*defaultRetry)

func (retrySingleton) Timeout(value time.Duration) retryOption {
	return func(this *defaultRetry) { this.timeout = value }
}
func (retrySingleton) MaxAttempts(value uint32) retryOption {
	return func(this *defaultRetry) { this.maxAttempts = int(value) }
}
func (retrySingleton) Logger(value messaging.Logger) retryOption {
	return func(this *defaultRetry) { this.logger = value }
}
func (retrySingleton) Monitor(value RetryMonitor) retryOption {
	return func(this *defaultRetry) { this.notify = value }
}
func (retrySingleton) LogStackTrace(value bool) retryOption {
	return func(this *defaultRetry) { this.stackTrace = value }
}

func (retrySingleton) defaults(options ...retryOption) []retryOption {
	const defaultRetryTimeout = time.Second * 5
	const defaultMaxAttempts = 1<<32 - 1
	var defaultLogger = log.New(log.Writer(), log.Prefix(), log.Flags())

	return append([]retryOption{
		RetryOptions.Timeout(defaultRetryTimeout),
		RetryOptions.MaxAttempts(defaultMaxAttempts),
		RetryOptions.Logger(defaultLogger),
		RetryOptions.Monitor(nopMonitor{}),
		RetryOptions.LogStackTrace(true),
	}, options...)
}

type nopMonitor struct{}

func (nopMonitor) Attempt(_ int, _ interface{}) {}
