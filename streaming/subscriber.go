package streaming

import (
	"context"
	"io"

	"github.com/smartystreets/messaging/v3"
)

type defaultSubscriber struct {
	pool         connectionPool
	subscription Subscription
	softContext  context.Context

	hardContext  context.Context
	hardShutdown context.CancelFunc
}

func newSubscriber(pool connectionPool, subscription Subscription, softContext context.Context) messaging.Listener {
	return defaultSubscriber{pool: pool, subscription: subscription, softContext: softContext}
}

func (this defaultSubscriber) Listen() {
	connection, err := this.pool.Current(this.softContext)
	if err != nil {
		return
	}
	defer this.pool.Release(connection)

	reader, err := connection.Reader(this.softContext)
	if err != nil {
		return
	}
	defer closeResource(reader)

	stream, err := reader.Stream(this.softContext, this.subscription.streamConfig())
	if err != nil {
		return
	}
	defer closeResource(stream)

}

func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
