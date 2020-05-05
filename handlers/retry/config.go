package retry

import (
	"time"

	"github.com/smartystreets/messaging/v3"
)

var Options singleton

type singleton struct{}
type option func(*handler)

func (singleton) Timeout(value time.Duration) option {
	return func(this *handler) { this.timeout = value }
}
func (singleton) MaxAttempts(value uint32) option {
	return func(this *handler) { this.maxAttempts = int(value) }
}
func (singleton) Logger(value messaging.Logger) option {
	return func(this *handler) { this.logger = value }
}
func (singleton) Monitor(value Monitor) option {
	return func(this *handler) { this.notify = value }
}
func (singleton) LogStackTrace(value bool) option {
	return func(this *handler) { this.stackTrace = value }
}

func (singleton) defaults(options ...option) []option {
	const defaultRetryTimeout = time.Second * 5
	const defaultMaxAttempts = 1<<32 - 1
	const defaultLogStackTrace = true
	var defaultLogger = nop{}
	var defaultMonitor = nop{}

	return append([]option{
		Options.Timeout(defaultRetryTimeout),
		Options.MaxAttempts(defaultMaxAttempts),
		Options.LogStackTrace(defaultLogStackTrace),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...interface{}) {}
func (nop) Println(_ ...interface{})          {}

func (nop) Attempt(_ int, _ interface{}) {}
