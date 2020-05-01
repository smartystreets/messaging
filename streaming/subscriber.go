package streaming

import (
	"context"
	"io"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type defaultSubscriber struct {
	pool         connectionPool
	subscription Subscription
	softContext  context.Context // pretty please be done as soon as possible.
	hardContext  context.Context // listen up, you're done RIGHT NOW!
	hardShutdown context.CancelFunc
	factory      workerFactory
	workersDone  chan struct{}
}

func newSubscriber(pool connectionPool, subscription Subscription, softContext context.Context, factory workerFactory) messaging.Listener {
	hardContext, hardShutdown := subscription.hardShutdown(softContext)
	return defaultSubscriber{
		pool:         pool,
		subscription: subscription,
		softContext:  softContext,
		hardContext:  hardContext,
		hardShutdown: hardShutdown,
		factory:      factory,
		workersDone:  make(chan struct{}),
	}
}

func (this defaultSubscriber) Listen() {
	connection, err := this.pool.Current(this.softContext)
	if err != nil {
		return
	}
	defer this.pool.Release(connection)

	reader, err := connection.Reader(this.softContext)
	if err != nil {
		return
	}
	defer closeResource(reader)

	stream, err := reader.Stream(this.softContext, this.subscription.streamConfig())
	if err != nil {
		return
	}

	go this.listen(stream)
	this.shutdown(stream)
}
func (this defaultSubscriber) listen(stream messaging.Stream) {
	defer close(this.workersDone)

	var waiter sync.WaitGroup
	defer waiter.Wait()
	waiter.Add(len(this.subscription.Handlers))

	for i := range this.subscription.Handlers {
		go func(index int) {
			defer waiter.Done()
			this.consume(index, stream)
		}(i)
	}
}
func (this defaultSubscriber) consume(index int, stream messaging.Stream) {
	worker := this.factory(workerConfig{
		Stream:       stream,
		Subscription: this.subscription,
		Handler:      this.subscription.Handlers[index],
		SoftContext:  this.softContext,
		HardContext:  this.hardContext,
	})
	worker.Listen()
}
func (this defaultSubscriber) shutdown(stream io.Closer) {
	select {
	case <-this.workersDone: // for some reason, workers have concluded before we expected
		closeResource(stream) // for example, the stream might have an error or the other end might have shut it down
	case <-this.softContext.Done():
		closeResource(stream) // stop the stream from bringing in messages and give workers some time to conclude.
		deadline, _ := context.WithTimeout(this.hardContext, this.subscription.ShutdownTimeout)
		select {
		case <-this.workersDone:
			return // no need to wait for full deadline, workers have finished
		case <-deadline.Done():
			this.hardShutdown() // stop workers, they're taking too long
		}
	}
}

func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
