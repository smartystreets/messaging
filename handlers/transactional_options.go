package handlers

import (
	"log"

	"github.com/smartystreets/messaging/v3"
)

var TransactionalOptions txSingleton

type txSingleton struct{}
type txOption func(*Transactional)

func (txSingleton) Factory(value handlerFactory) txOption {
	return func(this *Transactional) { this.factory = value }
}
func (txSingleton) Logger(value messaging.Logger) txOption {
	return func(this *Transactional) { this.logger = value }
}
func (txSingleton) Monitor(value TransactionMonitor) txOption {
	return func(this *Transactional) { this.monitor = value }
}

func (txSingleton) defaults(options ...txOption) []txOption {
	var defaultLogger = log.New(log.Writer(), log.Prefix(), log.Flags())
	var defaultMonitor = nopTxMonitor{}

	return append([]txOption{
		TransactionalOptions.Logger(defaultLogger),
		TransactionalOptions.Monitor(defaultMonitor),
	}, options...)
}

type nopTxMonitor struct{}

func (nopTxMonitor) BeginFailure(_ error)  {}
func (nopTxMonitor) Commit()               {}
func (nopTxMonitor) CommitFailure(_ error) {}
func (nopTxMonitor) Rollback()             {}
