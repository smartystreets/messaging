package rabbitmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type defaultConnection struct {
	inner  adapter.Connection
	config configuration
}

func newConnection(inner adapter.Connection, config configuration) messaging.Connection {
	// NOTE: using pointer type to allow for pointer equality check
	return &defaultConnection{inner: inner, config: config}
}
func (this defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	if channel, err := this.inner.Channel(); err != nil {
		return nil, err
	} else {
		return newReader(channel, this.config), nil
	}
}

func (this defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	return this.writer(false)
}
func (this defaultConnection) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	return this.writer(true)
}
func (this defaultConnection) writer(transactional bool) (messaging.CommitWriter, error) {
	channel, err := this.inner.Channel()
	if err != nil {
		return nil, err
	}

	if !transactional {
		return newWriter(channel, this.config), nil
	}

	if err := channel.Tx(); err != nil {
		_ = channel.Close()
		return nil, err
	}

	return newWriter(channel, this.config), nil
}

func (this defaultConnection) Close() error {
	return this.inner.Close()
}
