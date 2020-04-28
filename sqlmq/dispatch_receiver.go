package sqlmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dispatchReceiver struct {
	ctx    context.Context
	tx     adapter.Transaction
	output chan messaging.Dispatch
	store  messageStore

	buffer []messaging.Dispatch
}

func newDispatchReceiver(ctx context.Context, tx adapter.Transaction, config configuration) messaging.CommitWriter {
	return &dispatchReceiver{
		ctx:    ctx,
		tx:     tx,
		output: config.Channel,
		store:  config.MessageStore,
	}
}

func (this *dispatchReceiver) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.buffer = append(this.buffer, dispatches...)
	return len(dispatches), nil
}

func (this *dispatchReceiver) Commit() error {
	if err := this.store.Store(this.ctx, this.tx, this.buffer); err != nil {
		return err
	}

	if err := this.tx.Commit(); err != nil {
		return err
	}

	for _, dispatch := range this.buffer {
		select {
		case this.output <- dispatch:
		case <-this.ctx.Done():
			return this.ctx.Err()
		}
	}

	return nil
}

func (this *dispatchReceiver) Rollback() error { return this.tx.Rollback() }
func (this *dispatchReceiver) Close() error    { return nil }
