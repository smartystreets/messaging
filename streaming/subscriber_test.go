package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestSubscriberFixture(t *testing.T) {
	gunit.Run(new(SubscriberFixture), t)
}

type SubscriberFixture struct {
	*gunit.Fixture

	subscription Subscription
	softContext  context.Context
	subscriber   messaging.Listener

	workerFactoryConfig workerConfig

	currentCount   int
	currentContext context.Context
	currentError   error

	releasedConnections []messaging.Connection

	readerCount int
	readerCtx   context.Context
	readerError error

	closeCount int

	streamCount   int
	streamContext context.Context
	streamConfig  messaging.StreamConfig
	streamError   error

	listenCount int
}

func (this *SubscriberFixture) Setup() {
	this.subscription = Subscription{
		Queue:             "queue",
		Topics:            []string{"topic1", "topic2"},
		EstablishTopology: true,
		BufferSize:        16,
		Handlers:          []messaging.Handler{nil},
		workerFactory:     this.workerFactory,
	}
	this.softContext = context.Background()
	this.initializeSubscriber()
}
func (this *SubscriberFixture) initializeSubscriber() {
	this.subscriber = newSubscriber(this, this.subscription, this.softContext)
}
func (this *SubscriberFixture) workerFactory(config workerConfig) messaging.Listener {
	this.workerFactoryConfig = config
	return this
}

func (this *SubscriberFixture) TestWhenOpeningAConnectionFails_ListenShouldReturn() {
	this.currentError = errors.New("")

	this.subscriber.Listen()

	this.So(this.currentContext, should.Equal, this.softContext)
	this.So(this.currentCount, should.Equal, 1)
}
func (this *SubscriberFixture) TestWhenOpeningReaderFails_ListenShouldReturn() {
	this.readerError = errors.New("")

	this.subscriber.Listen()

	this.So(this.readerCtx, should.Equal, this.softContext)
	this.So(this.readerCount, should.Equal, 1)
	this.So(this.releasedConnections, should.Resemble, []messaging.Connection{this})
}
func (this *SubscriberFixture) TestWhenOpeningStreamFails_ListenShouldReturn() {
	this.streamError = errors.New("")

	this.subscriber.Listen()

	this.So(this.streamContext, should.Equal, this.softContext)
	this.So(this.streamCount, should.Equal, 1)
	this.So(this.streamConfig, should.Resemble, messaging.StreamConfig{
		EstablishTopology: true,
		ExclusiveStream:   true, // single handler
		BufferSize:        this.subscription.BufferSize,
		Queue:             this.subscription.Queue,
		Topics:            this.subscription.Topics,
	})
	this.So(this.closeCount, should.Equal, 1) // reader
}
func (this *SubscriberFixture) TestWhenListenConcludes_AllResourcesShouldBeClosed() {
	this.subscriber.Listen()

	this.So(this.closeCount, should.Equal, 2) // reader and stream
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ConnectionPool
func (this *SubscriberFixture) Current(ctx context.Context) (messaging.Connection, error) {
	this.currentCount++
	this.currentContext = ctx
	return this, this.currentError
}
func (this *SubscriberFixture) Release(connection messaging.Connection) {
	this.releasedConnections = append(this.releasedConnections, connection)
}

// Connection
func (this *SubscriberFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	this.readerCount++
	this.readerCtx = ctx
	return this, this.readerError
}
func (this *SubscriberFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *SubscriberFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}

// Reader
func (this *SubscriberFixture) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	this.streamCount++
	this.streamContext = ctx
	this.streamConfig = config
	return this, this.streamError
}

// Stream
func (this *SubscriberFixture) Read(ctx context.Context, delivery *messaging.Delivery) error {
	panic("nop")
}
func (this *SubscriberFixture) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	panic("nop")
}

// Shared between Reader and Stream (connection isn't closed by the Subscriber)
func (this *SubscriberFixture) Close() error {
	this.closeCount++
	return nil
}

// Worker
func (this *SubscriberFixture) Listen() {
	this.listenCount++
}
