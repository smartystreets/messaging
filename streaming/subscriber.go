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
	softContext  context.Context
	hardContext  context.Context
	hardShutdown context.CancelFunc
	factory      workerFactory
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

	workersCompleted := make(chan struct{})
	go this.listen(stream, workersCompleted)
	this.shutdown(stream, workersCompleted)
}
func (this defaultSubscriber) listen(stream messaging.Stream, workersCompleted chan struct{}) {
	defer close(workersCompleted)

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
func (this defaultSubscriber) shutdown(stream io.Closer, workersCompleted chan struct{}) {
	select {
	case <-workersCompleted:
		closeResource(stream)
	case <-this.softContext.Done():
		closeResource(stream)
		deadline, _ := context.WithTimeout(this.hardContext, this.subscription.ShutdownTimeout)
		select {
		case <-workersCompleted:
			return
		case <-deadline.Done():
			this.hardShutdown()
		}
	}
}

func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
