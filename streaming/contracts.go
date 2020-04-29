package streaming

import (
	"github.com/smartystreets/messaging/v3"
)

type Subscription struct {
	Name     string
	Queue    string
	Topics   []string
	Handlers []messaging.Handler
}

type ShutdownStrategy int

const (
	ShutdownStrategyImmediate ShutdownStrategy = iota
	ShutdownStrategyCurrentBatch
	ShutdownStrategyDrain
)
