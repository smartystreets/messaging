package sqlmq

import (
	"context"
	"errors"
	"strings"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dispatchReceiver struct {
	active adapter.Transaction
	output chan messaging.Dispatch
	ctx    context.Context

	buffer []messaging.Dispatch
}

func newDispatchReceiver(active adapter.Transaction, output chan messaging.Dispatch, ctx context.Context) messaging.CommitWriter {
	return &dispatchReceiver{active: active, output: output, ctx: ctx}
}

func (this *dispatchReceiver) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.buffer = append(this.buffer, dispatches...)
	return len(dispatches), nil
}

func (this *dispatchReceiver) Commit() error {
	identity, err := this.storeBuffer()
	if err != nil {
		return err
	}

	if err = this.active.Commit(); err != nil {
		return err
	}

	return this.sendWritten(identity)
}
func (this *dispatchReceiver) storeBuffer() (uint64, error) {
	if len(this.buffer) == 0 {
		return 0, nil
	}

	statement, args := this.buildExecArgs()
	result, err := this.active.ExecContext(this.ctx, statement, args...)
	if err != nil {
		return 0, err
	}

	affected, _ := result.RowsAffected()
	if affected != int64(len(this.buffer)) {
		return 0, errRowsAffected
	}

	identity, _ := result.LastInsertId()
	if identity <= 0 {
		return 0, errIdentityFailure
	}

	return uint64(identity), nil
}
func (this *dispatchReceiver) buildExecArgs() (string, []interface{}) {
	builder := &strings.Builder{}
	args := make([]interface{}, 0, len(this.buffer)*2)

	_, _ = builder.WriteString("INSERT INTO Messages (type, payload) VALUES ")
	for i, dispatch := range this.buffer {
		args = append(args, dispatch.MessageType, dispatch.Payload)
		if i == len(this.buffer)-1 {
			_, _ = builder.WriteString("(?,?);")
		} else {
			_, _ = builder.WriteString("(?,?),")
		}
	}

	return builder.String(), args
}
func (this *dispatchReceiver) sendWritten(identity uint64) error {
	length := uint64(len(this.buffer))

	for i, dispatch := range this.buffer {
		dispatch.MessageID = identity - (length - 1 - uint64(i))
		select {
		case this.output <- dispatch:
		case <-this.ctx.Done():
			return this.ctx.Err()
		}
	}

	return nil
}

func (this *dispatchReceiver) Rollback() error {
	return this.active.Rollback()
}
func (this *dispatchReceiver) Close() error { return nil }

var errRowsAffected = errors.New("the number of modified rows was not expected compared to the number of writes performed")
var errIdentityFailure = errors.New("unable to determine the identity of the inserted row(s)")
