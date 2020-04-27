package rabbitmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type defaultWriter struct {
}

func newWriter(inner adapter.Channel, config configuration) messaging.CommitWriter {
	return defaultWriter{}
}

func (this defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	panic("implement me")
}
func (this defaultWriter) Commit() error {
	panic("implement me")
}
func (this defaultWriter) Rollback() error {
	panic("implement me")
}

func (this defaultWriter) Close() error {
	panic("implement me")
}
