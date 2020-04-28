package sqlmq

import (
	"context"
	"errors"
	"strings"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dispatchStore struct {
}

func (this *dispatchStore) Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error {
	length := uint64(len(dispatches))
	if length == 0 {
		return nil
	}

	statement, args := this.buildExecArgs(dispatches)
	result, err := writer.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	if affected != int64(length) {
		return errRowsAffected
	}

	identity, _ := result.LastInsertId()
	if identity <= 0 {
		return errIdentityFailure
	}

	for i := uint64(0); i < length; i++ {
		dispatches[i].MessageID = uint64(identity) - (length - 1 - i)
	}

	return nil
}
func (this *dispatchStore) buildExecArgs(dispatches []messaging.Dispatch) (string, []interface{}) {
	builder := &strings.Builder{}
	args := make([]interface{}, 0, len(dispatches)*2)

	_, _ = builder.WriteString("INSERT INTO Messages (type, payload) VALUES ")
	for i, dispatch := range dispatches {
		args = append(args, dispatch.MessageType, dispatch.Payload)
		if i == len(dispatches)-1 {
			_, _ = builder.WriteString("(?,?);")
		} else {
			_, _ = builder.WriteString("(?,?),")
		}
	}

	return builder.String(), args
}

var (
	errRowsAffected    = errors.New("the number of modified rows was not expected compared to the number of writes performed")
	errIdentityFailure = errors.New("unable to determine the identity of the inserted row(s)")
)
