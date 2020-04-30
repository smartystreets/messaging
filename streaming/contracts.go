package streaming

import (
	"time"

	"github.com/smartystreets/messaging/v3"
)

type Subscription struct {
	Name              string
	Queue             string
	Topics            []string
	Handlers          []messaging.Handler
	BufferSize        uint16
	EstablishTopology bool
	MaxBatchSize      int
	HandleDelivery    bool
	BufferTimeout     time.Duration // the amount of time to rest and buffer between batches (instead of going as quickly as possible)
	ShutdownStrategy  ShutdownStrategy
}

func (this Subscription) streamConfig() messaging.StreamConfig {
	return messaging.StreamConfig{
		EstablishTopology: this.EstablishTopology,
		ExclusiveStream:   len(this.Handlers) <= 1,
		BufferSize:        this.BufferSize,
		Queue:             this.Queue,
		Topics:            this.Topics,
	}
}

type ShutdownStrategy int

const (
	ShutdownStrategyCurrentBatch ShutdownStrategy = iota
	ShutdownStrategyImmediate
	ShutdownStrategyDrain
)
