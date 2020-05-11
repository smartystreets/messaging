package sqlmq

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type dynamicHandle struct{ dynamic atomic.Value }

func newDynamicHandle(dynamic atomic.Value) dynamicHandle { return dynamicHandle{dynamic: dynamic} }

func (this dynamicHandle) DBHandle() *sql.DB {
	return this.target().DBHandle()
}
func (this dynamicHandle) QueryContext(ctx context.Context, statement string, args ...interface{}) (adapter.QueryResult, error) {
	return this.target().QueryContext(ctx, statement, args...)
}
func (this dynamicHandle) QueryRowContext(ctx context.Context, statement string, args ...interface{}) adapter.RowScanner {
	return this.target().QueryRowContext(ctx, statement, args...)
}
func (this dynamicHandle) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	return this.target().ExecContext(ctx, statement, args...)
}
func (this dynamicHandle) BeginTx(ctx context.Context, options *sql.TxOptions) (adapter.Transaction, error) {
	return this.target().BeginTx(ctx, options)
}
func (this dynamicHandle) Close() error {
	return this.target().Close()
}

func (this dynamicHandle) target() adapter.Handle {
	return this.dynamic.Load().(adapter.Handle)
}
