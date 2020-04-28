package sqlmq

import (
	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type messageStore interface {
	Store(writer adapter.Writer, dispatches []messaging.Dispatch) error
}
