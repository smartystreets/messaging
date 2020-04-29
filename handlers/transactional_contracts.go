package handlers

import (
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

type TransactionMonitor interface {
	BeginFailure(error)
	Commit()
	CommitFailure(error)
	Rollback()
}

type handlerFactory func(state TransactionState) messaging.Handler
type TransactionState struct {
	Tx     *sql.Tx
	Writer messaging.Writer
}
