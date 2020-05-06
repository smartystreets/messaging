package sqlmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dispatchReceiver struct {
	ctx     context.Context
	tx      adapter.Transaction
	output  chan messaging.Dispatch
	store   messageStore
	logger  messaging.Logger
	monitor Monitor

	buffer []messaging.Dispatch
}

func newDispatchReceiver(ctx context.Context, tx adapter.Transaction, config configuration) messaging.CommitWriter {
	return &dispatchReceiver{
		ctx:     ctx,
		tx:      tx,
		output:  config.Channel,
		store:   config.MessageStore,
		logger:  config.Logger,
		monitor: config.Monitor,
	}
}

func (this *dispatchReceiver) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.buffer = append(this.buffer, dispatches...)
	length := len(dispatches)
	this.monitor.MessageReceived(length)
	return length, nil
}

func (this *dispatchReceiver) Commit() error {
	if err := this.store.Store(this.ctx, this.tx, this.buffer); err != nil {
		this.logger.Printf("[WARN] Unable to persist messages to durable storage [%s].", err)
		return err
	}

	if err := this.tx.Commit(); err != nil {
		this.logger.Printf("[WARN] Unable to commit messages to durable storage [%s].", err)
		return err
	}

	this.monitor.MessageStored(len(this.buffer))
	for _, dispatch := range this.buffer {
		select {
		case this.output <- dispatch:
		case <-this.ctx.Done():
			return this.ctx.Err()
		}
	}

	// TODO: clear buffer!!!
	return nil
}

func (this *dispatchReceiver) Rollback() error { return this.tx.Rollback() }
func (this *dispatchReceiver) Close() error    { return nil }
