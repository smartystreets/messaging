package sqlmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type messageStore interface {
	Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error
	Load(ctx context.Context, id uint64) ([]messaging.Dispatch, error)
	Confirm(ctx context.Context, dispatches []messaging.Dispatch) error
}
