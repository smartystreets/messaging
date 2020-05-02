package streaming

import (
	"context"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type Subscription struct {
	name              string
	queue             string
	topics            []string
	handlers          []messaging.Handler
	bufferSize        uint16
	establishTopology bool
	maxBatchSize      uint16
	handleDelivery    bool
	bufferTimeout     time.Duration // the amount of time to rest and buffer between batches (instead of going as quickly as possible)
	shutdownStrategy  ShutdownStrategy
	shutdownTimeout   time.Duration
}

func NewSubscription(options ...subscriptionOption) Subscription {
	this := Subscription{}
	SubscriptionOptions.apply(options...)(&this)
	return this
}

func (this Subscription) streamConfig() messaging.StreamConfig {
	return messaging.StreamConfig{
		EstablishTopology: this.establishTopology,
		ExclusiveStream:   len(this.handlers) <= 1,
		BufferSize:        this.bufferSize,
		Queue:             this.queue,
		Topics:            this.topics,
	}
}
func (this Subscription) hardShutdown(potentialParent context.Context) (context.Context, context.CancelFunc) {
	if this.shutdownStrategy == ShutdownStrategyImmediate {
		return potentialParent, func() {}
	}

	return context.WithCancel(context.Background())
}

type ShutdownStrategy int

const (
	ShutdownStrategyCurrentBatch ShutdownStrategy = iota
	ShutdownStrategyImmediate
	ShutdownStrategyDrain
)
