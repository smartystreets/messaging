package sqlmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dispatchReceiver struct {
	active adapter.Transaction
}

func newDispatchReceiver(active adapter.Transaction, config configuration) messaging.CommitWriter {
	return &dispatchReceiver{active: active}
}

func (this *dispatchReceiver) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	panic("implement me")
}

func (this *dispatchReceiver) Commit() error {
	panic("implement me")
}

func (this *dispatchReceiver) Rollback() error {
	panic("implement me")
}

func (this *dispatchReceiver) Close() error {
	panic("implement me")
}
