package streaming

import (
	"context"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestSubscriptionConfigFixture(t *testing.T) {
	gunit.Run(new(SubscriptionConfigFixture), t)
}

type SubscriptionConfigFixture struct {
	*gunit.Fixture

	legacyHandleMessages []interface{}
}

func (this *SubscriptionConfigFixture) Setup() {
}

func (this *SubscriptionConfigFixture) TestWhenNoHandlersAreConfigured_ItShouldPanic() {
	this.So(func() { NewSubscription() }, should.Panic)
}

func (this *SubscriptionConfigFixture) TestWhenLegacyHandlerIsProvided_HandlerShouldBeAdapted() {
	subscription := NewSubscription(SubscriptionOptions.AddLegacyWorkers(this))

	subscription.handlers[0].Handle(context.Background(), 0, 1, 2)

	this.So(this.legacyHandleMessages, should.Resemble, []interface{}{0, 1, 2})
}
func (this *SubscriptionConfigFixture) Handle(messages ...interface{}) {
	this.legacyHandleMessages = messages
}

func (this *SubscriptionConfigFixture) TestWhenValuesAreProvided_SubscriptionShouldHaveValues() {
	subscription := NewSubscription(
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.Name("name"),
		SubscriptionOptions.Queue("queue"),
		SubscriptionOptions.Topics("topic1", "topic2"),
		SubscriptionOptions.MaxBatchSize(1),
		SubscriptionOptions.BufferSize(2),
		SubscriptionOptions.BufferDelayBetweenBatches(3),
		SubscriptionOptions.EstablishTopology(true),
		SubscriptionOptions.FullDeliveryToHandler(true),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyCurrentBatch, 4),
	)

	this.So(subscription, should.Resemble, Subscription{
		name:              "name",
		queue:             "queue",
		topics:            []string{"topic1", "topic2"},
		handlers:          []messaging.Handler{nil},
		bufferSize:        2,
		establishTopology: true,
		maxBatchSize:      1,
		handleDelivery:    true,
		bufferTimeout:     3,
		shutdownStrategy:  ShutdownStrategyCurrentBatch,
		shutdownTimeout:   4,
	})
}

func (this *SubscriptionConfigFixture) TestWhenUnrecognizedShutdownStrategyIsProvided_ItShouldPanic() {
	unknown := ShutdownStrategy(42)

	this.So(func() {
		NewSubscription(SubscriptionOptions.AddWorkers(nil), SubscriptionOptions.ShutdownStrategy(unknown, 0))
	}, should.Panic)
}
func (this *SubscriptionConfigFixture) TestWhenShutdownStrategyIsImmediate_TimeoutIsSetToZero() {
	subscription := NewSubscription(
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyImmediate, 42))

	this.So(subscription.shutdownStrategy, should.Equal, ShutdownStrategyImmediate)
	this.So(subscription.shutdownTimeout, should.Equal, 0)
}

func (this *SubscriptionConfigFixture) TestWhenNumberOfHandlersIsLargerThanBufferSize_BufferSizeSetToNumberOfHandlers() {
	subscription := NewSubscription(
		SubscriptionOptions.AddWorkers(nil, nil, nil, nil),
		SubscriptionOptions.BufferSize(2))

	this.So(subscription.bufferSize, should.Equal, len(subscription.handlers))
}
