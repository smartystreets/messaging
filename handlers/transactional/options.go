package transactional

import (
	"log"

	"github.com/smartystreets/messaging/v3"
)

var Options singleton

type singleton struct{}
type option func(*handler)

func (singleton) Logger(value messaging.Logger) option {
	return func(this *handler) { this.logger = value }
}
func (singleton) Monitor(value Monitor) option {
	return func(this *handler) { this.monitor = value }
}

func (singleton) defaults(options ...option) []option {
	var defaultLogger = log.New(log.Writer(), log.Prefix(), log.Flags())
	var defaultMonitor = nopTxMonitor{}

	return append([]option{
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nopTxMonitor struct{}

func (nopTxMonitor) BeginFailure(_ error)  {}
func (nopTxMonitor) Commit()               {}
func (nopTxMonitor) CommitFailure(_ error) {}
func (nopTxMonitor) Rollback()             {}
