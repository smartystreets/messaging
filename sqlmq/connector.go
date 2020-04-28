package sqlmq

import (
	"context"
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct{ config configuration }

func newConnector(config configuration) messaging.Connector {
	return defaultConnector{config: config}
}
func (this defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	return defaultConnection{config: this.config}, nil
}
func (this defaultConnector) Close() error {
	return this.config.StorageHandle.Close()
}

type defaultConnection struct{ config configuration }

func (this defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	panic("not supported")
}
func (this defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	panic("not supported")
}
func (this defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	tx, err := this.config.StorageHandle.BeginTx(ctx, &this.config.SQLTxOptions)
	if err != nil {
		return nil, err
	}

	if state, ok := ctx.Value(txStateContextKey).(*TxState); ok {
		state.Tx = tx.TxHandle() // allow caller to get a handle on the transaction
	}

	return newDispatchReceiver(tx, this.config.Channel, ctx), nil
}

func (this defaultConnection) Close() error { return nil }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type TxState struct {
	Tx     *sql.Tx
	Writer messaging.Writer
}

const txStateContextKey = "sql.tx"
