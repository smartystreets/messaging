package streaming

import (
	"context"
	"time"

	"github.com/smartystreets/messaging/v3"
)

func NewSubscription(queue string, options ...subscriptionOption) Subscription {
	this := Subscription{queue: queue}
	SubscriptionOptions.apply(options...)(&this)
	return this
}

var SubscriptionOptions subscriptionSingleton

type subscriptionSingleton struct{}
type subscriptionOption func(*Subscription)

func (subscriptionSingleton) Name(value string) subscriptionOption {
	return func(this *Subscription) { this.name = value }
}
func (subscriptionSingleton) AddWorkers(values ...messaging.Handler) subscriptionOption {
	return func(this *Subscription) { this.handlers = append(this.handlers, values...) }
}
func (subscriptionSingleton) AddLegacyWorkers(values ...legacyHandler) subscriptionOption {
	return func(this *Subscription) {
		for _, handler := range values {
			this.handlers = append(this.handlers, legacyAdapter{inner: handler})
		}
	}
}
func (subscriptionSingleton) BufferSize(value uint16) subscriptionOption {
	return func(this *Subscription) { this.bufferSize = value }
}
func (subscriptionSingleton) MaxBatchSize(value uint16) subscriptionOption {
	return func(this *Subscription) { this.maxBatchSize = value }
}
func (subscriptionSingleton) BufferDelayBetweenBatches(value time.Duration) subscriptionOption {
	return func(this *Subscription) { this.bufferTimeout = value }
}
func (subscriptionSingleton) EstablishTopology(value bool) subscriptionOption {
	return func(this *Subscription) { this.establishTopology = value }
}
func (subscriptionSingleton) Topics(values ...string) subscriptionOption {
	return func(this *Subscription) { this.topics = values }
}
func (subscriptionSingleton) FullDeliveryToHandler(value bool) subscriptionOption {
	return func(this *Subscription) { this.handleDelivery = value }
}
func (subscriptionSingleton) ShutdownStrategy(strategy ShutdownStrategy, timeout time.Duration) subscriptionOption {
	return func(this *Subscription) {
		switch strategy {
		case ShutdownStrategyImmediate, ShutdownStrategyCurrentBatch, ShutdownStrategyDrain:
			break
		default:
			panic("unrecognized shutdown strategy")
		}

		this.shutdownStrategy = strategy
		if strategy == ShutdownStrategyImmediate {
			timeout = 0
		}

		this.shutdownTimeout = timeout
	}
}

func (subscriptionSingleton) apply(options ...subscriptionOption) subscriptionOption {
	return func(this *Subscription) {
		for _, option := range SubscriptionOptions.defaults(options...) {
			option(this)
		}

		if length := len(this.handlers); length > int(this.bufferSize) {
			this.bufferSize = uint16(length)
		}

		if len(this.handlers) == 0 {
			panic("no workers configured")
		}
	}
}
func (subscriptionSingleton) defaults(options ...subscriptionOption) []subscriptionOption {
	const defaultBufferSize = 1
	const defaultMaxBatchSize = 1
	const defaultBatchDelay = 0
	const defaultEstablishTopology = true
	const defaultPassFullDeliveryToHandler = false
	const defaultShutdownStrategy = ShutdownStrategyDrain
	const defaultShutdownTimeout = time.Second * 5

	return append([]subscriptionOption{
		SubscriptionOptions.BufferSize(defaultBufferSize),
		SubscriptionOptions.MaxBatchSize(defaultMaxBatchSize),
		SubscriptionOptions.BufferDelayBetweenBatches(defaultBatchDelay),
		SubscriptionOptions.EstablishTopology(defaultEstablishTopology),
		SubscriptionOptions.FullDeliveryToHandler(defaultPassFullDeliveryToHandler),
		SubscriptionOptions.ShutdownStrategy(defaultShutdownStrategy, defaultShutdownTimeout),
	}, options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type legacyHandler interface{ Handle(messages ...interface{}) }
type legacyAdapter struct{ inner legacyHandler }

func (this legacyAdapter) Handle(_ context.Context, messages ...interface{}) {
	this.inner.Handle(messages...)
}
