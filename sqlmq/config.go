package sqlmq

import (
	"database/sql"

	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type configuration struct {
	StorageHandle adapter.Handle
	SQLTxOptions  sql.TxOptions
}
