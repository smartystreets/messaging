package adapter

import (
	"context"
	"database/sql"
	"io"
	"sync/atomic"
)

func New(value *sql.DB) Handle { return sqlDB{DB: value} }
func Open(driver, dataSource string) Handle {
	if handle, err := sql.Open(driver, dataSource); err != nil {
		panic(err)
	} else {
		return New(handle)
	}
}
func Dynamic(value atomic.Value) Handle { return newDynamic(value) }

type Handle interface {
	ReadWriter
	BeginTx(ctx context.Context, options *sql.TxOptions) (Transaction, error)
	io.Closer

	DBHandle() *sql.DB
}

type Reader interface {
	QueryContext(ctx context.Context, statement string, args ...interface{}) (QueryResult, error)
	QueryRowContext(ctx context.Context, statement string, args ...interface{}) RowScanner
}
type QueryResult interface {
	RowScanner
	Next() bool
	Err() error
	io.Closer
}
type RowScanner interface {
	Scan(...interface{}) error
}

type Writer interface {
	ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error)
}
type ReadWriter interface {
	Reader
	Writer
}

type Transaction interface {
	ReadWriter
	Transactional

	TxHandle() *sql.Tx
}
type Transactional interface {
	Commit() error
	Rollback() error
}
