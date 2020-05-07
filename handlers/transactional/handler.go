package transactional

import (
	"context"
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

type handler struct {
	connector messaging.Connector
	factory   handlerFunc
	monitor   Monitor
	logger    messaging.Logger
}

func (this handler) Handle(ctx context.Context, messages ...interface{}) {
	connection, err := this.connector.Connect(ctx)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.Begin(err)
		panic(err)
	}

	txCtx := newContext(ctx, connection)
	defer func() { this.finally(txCtx, recover()) }()
	writer, err := connection.CommitWriter(txCtx)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.Begin(err)
		panic(err)
	}

	this.monitor.Begin(nil)
	txCtx.Writer = writer
	handler := this.factory(txCtx.State())
	handler.Handle(ctx, messages...)
	if err := writer.Commit(); err != nil {
		this.logger.Printf("[WARN] Unable to commit transaction [%s].", err)
		this.monitor.Commit(err)
		panic(err)
	}

	this.monitor.Commit(nil)
}
func (this handler) finally(ctx *transactionalContext, err interface{}) {
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

func newContext(ctx context.Context, connection messaging.Connection) *transactionalContext {
	return &transactionalContext{Context: ctx, Connection: connection}
}
func (this *transactionalContext) Store(tx *sql.Tx) { this.Tx = tx } // used by sqlmq
func (this *transactionalContext) State() State {
	return State{Tx: this.Tx, Writer: this.Writer}
}
func (this *transactionalContext) Close() error {
	if this.Writer != nil {
		_ = this.Writer.Close()
	}
	return this.Connection.Close()
}