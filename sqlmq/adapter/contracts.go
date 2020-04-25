package adapter

import (
	"context"
	"database/sql"
	"io"
)

func New(value *sql.DB) Handle { return sqlDB{DB: value} }

type Handle interface {
	ReadWriter
	BeginTx(ctx context.Context, options *sql.TxOptions) (Transaction, error)
	io.Closer

	Handle() *sql.DB
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

	Handle() *sql.Tx
}
type Transactional interface {
	Commit() error
	Rollback() error
}
