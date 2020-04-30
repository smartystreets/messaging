package streaming

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type workerConfig struct {
	Stream       messaging.Stream
	Subscription Subscription
	Handler      messaging.Handler
	SoftContext  context.Context
	HardContext  context.Context
}
