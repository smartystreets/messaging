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
	const defaultLogStackTrace = true
	var defaultLogger = log.New(log.Writer(), log.Prefix(), log.Flags())
	var defaultMonitor = nopRetryMonitor{}

	return append([]retryOption{
		RetryOptions.Timeout(defaultRetryTimeout),
		RetryOptions.MaxAttempts(defaultMaxAttempts),
		RetryOptions.LogStackTrace(defaultLogStackTrace),
		RetryOptions.Logger(defaultLogger),
		RetryOptions.Monitor(defaultMonitor),
	}, options...)
}

type nopRetryMonitor struct{}

func (nopRetryMonitor) Attempt(_ int, _ interface{}) {}
