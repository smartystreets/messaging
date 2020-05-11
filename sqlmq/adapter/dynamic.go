package adapter

import (
	"context"
	"database/sql"
	"sync/atomic"
)

type dynamicDB struct{ dynamic atomic.Value }

func newDynamic(dynamic atomic.Value) dynamicDB { return dynamicDB{dynamic: dynamic} }

func (this dynamicDB) DBHandle() *sql.DB {
	return this.target()
}
func (this dynamicDB) QueryContext(ctx context.Context, statement string, args ...interface{}) (QueryResult, error) {
	return this.target().QueryContext(ctx, statement, args...)
}
func (this dynamicDB) QueryRowContext(ctx context.Context, statement string, args ...interface{}) RowScanner {
	return this.target().QueryRowContext(ctx, statement, args...)
}
func (this dynamicDB) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	return this.target().ExecContext(ctx, statement, args...)
}
func (this dynamicDB) BeginTx(ctx context.Context, options *sql.TxOptions) (Transaction, error) {
	if tx, err := this.target().BeginTx(ctx, options); err != nil {
		return nil, err
	} else {
		return sqlTx{Tx: tx}, nil
	}
}
func (this dynamicDB) Close() error {
	return this.target().Close()
}

func (this dynamicDB) target() *sql.DB {
	return this.dynamic.Load().(*sql.DB)
}
