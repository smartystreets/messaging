package handlers

import (
	"context"
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

type Transactional struct {
	connector messaging.Connector
	factory   handlerFactory
	monitor   TransactionMonitor
	logger    messaging.Logger
}

func NewTransactional(connector messaging.Connector, options ...txOption) messaging.Handler {
	this := Transactional{connector: connector}
	for _, option := range TransactionalOptions.defaults(options...) {
		option(&this)
	}
	return this
}

func (this Transactional) Handle(ctx context.Context, messages ...interface{}) {
	connection, err := this.connector.Connect(ctx)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.BeginFailure(err)
		panic(err)
	}

	txContext := newTransactionalContext(ctx, connection)
	defer func() { this.finally(txContext, recover()) }()
	writer, err := connection.CommitWriter(txContext)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.BeginFailure(err)
		panic(err)
	}

	txContext.Writer = writer
	handler := this.factory(txContext.State())
	handler.Handle(ctx, messages...)
	if err := writer.Commit(); err != nil {
		this.logger.Printf("[WARN] Unable to commit transaction [%s].", err)
		this.monitor.CommitFailure(err)
		panic(err)
	}

	this.monitor.Commit()
}
func (this Transactional) finally(ctx *transactionalContext, err interface{}) {
	defer func() { _ = ctx.Close() }()
	if err == nil {
		return
	}

	if ctx.Writer != nil {
		_ = ctx.Writer.Rollback()
		this.monitor.Rollback()
	}

	panic(err)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type transactionalContext struct {
	context.Context
	Tx         *sql.Tx
	Writer     messaging.CommitWriter
	Connection messaging.Connection
}

func newTransactionalContext(ctx context.Context, connection messaging.Connection) *transactionalContext {
	return &transactionalContext{Context: ctx, Connection: connection}
}
func (this *transactionalContext) Store(tx *sql.Tx) { this.Tx = tx }
func (this *transactionalContext) State() TransactionState {
	return TransactionState{Tx: this.Tx, Writer: this.Writer}
}
func (this *transactionalContext) Close() error {
	if this.Writer != nil {
		_ = this.Writer.Close()
	}
	return this.Connection.Close()
}
