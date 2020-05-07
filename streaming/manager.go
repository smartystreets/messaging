package streaming

import (
	"context"
	"io"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type subscriberFactory func(context.Context, Subscription) messaging.Listener
type defaultManager struct {
	softContext    context.Context
	softShutdown   context.CancelFunc
	subscriptions  []Subscription
	connectionPool io.Closer
	factory        subscriberFactory
}

func newManager(pool io.Closer, subscriptions []Subscription, factory subscriberFactory) messaging.ListenCloser {
	softContext, softShutdown := context.WithCancel(context.Background())
	return defaultManager{
		softContext:    softContext,
		softShutdown:   softShutdown,
		subscriptions:  subscriptions,
		connectionPool: pool,
		factory:        factory,
	}
}

func (this defaultManager) Listen() {
	defer closeResource(this.connectionPool)

	var waiter sync.WaitGroup
	waiter.Add(len(this.subscriptions))
	defer waiter.Wait()

	for i := range this.subscriptions {
		go func(index int) {
			defer waiter.Done()
			this.listen(index)
		}(i)
	}
}
func (this defaultManager) listen(index int) {
	subscriber := this.factory(this.softContext, this.subscriptions[index])
	subscriber.Listen()
}

func (this defaultManager) Close() error {
	this.softShutdown()
	return nil
}
func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
