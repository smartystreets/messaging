package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type defaultWorker struct {
	stream                messaging.Stream
	soft                  context.Context
	hard                  context.Context
	handler               messaging.Handler
	channelBuffer         chan messaging.Delivery
	messageBatch          []interface{}
	outstandingDeliveries []messaging.Delivery
	handleDelivery        bool
	bufferTimeout         time.Duration
	strategy              ShutdownStrategy
}

func newWorker(stream messaging.Stream, subscription Subscription, index int, soft, hard context.Context) *defaultWorker {
	buffer := make(chan messaging.Delivery, subscription.BufferSize)
	handler := subscription.Handlers[index]
	return &defaultWorker{
		stream:                stream,
		soft:                  soft,
		hard:                  hard,
		handler:               handler,
		channelBuffer:         buffer,
		messageBatch:          make([]interface{}, 0, subscription.MaxBatchSize),
		outstandingDeliveries: make([]messaging.Delivery, 0, subscription.MaxBatchSize),
		handleDelivery:        subscription.HandleDelivery,
		bufferTimeout:         subscription.BufferTimeout,
		strategy:              subscription.ShutdownStrategy,
	}
}

func (this *defaultWorker) Listen() {
	var waiter sync.WaitGroup
	defer waiter.Wait()

	waiter.Add(1)
	go func() {
		defer waiter.Done()
		this.readFromStream()
	}()

	this.deliverToHandler()
}

func (this *defaultWorker) readFromStream() {
	defer close(this.channelBuffer)
	for {
		var delivery messaging.Delivery
		if err := this.stream.Read(this.hard, &delivery); err != nil {
			break
		}

		select {
		case <-this.hard.Done():
			break
		case this.channelBuffer <- delivery:
		}
	}
}

func (this *defaultWorker) deliverToHandler() {
	if this.handler == nil {
		return // this facilitates testing
	}

	for delivery := range this.channelBuffer {
		if this.isComplete(ShutdownStrategyImmediate) {
			break
		}

		this.addToBatch(delivery)
		if this.canBatchMore() {
			continue
		}

		if !this.deliverBatch() {
			break
		}

		if this.isComplete(ShutdownStrategyCurrentBatch) {
			break
		}

		this.sleep()
		this.clearBatch()
	}
}
func (this *defaultWorker) addToBatch(delivery messaging.Delivery) {
	this.outstandingDeliveries = append(this.outstandingDeliveries, delivery)
	if this.handleDelivery {
		this.messageBatch = append(this.messageBatch, delivery)
	} else {
		this.messageBatch = append(this.messageBatch, delivery.Message)
	}
}
func (this *defaultWorker) canBatchMore() bool {
	return len(this.channelBuffer) > 0 && len(this.outstandingDeliveries) < cap(this.outstandingDeliveries)
}
func (this *defaultWorker) deliverBatch() bool {
	this.handler.Handle(this.hard, this.messageBatch...)
	if err := this.stream.Acknowledge(this.hard, this.outstandingDeliveries...); err != nil {
		return false
	}

	return true
}
func (this *defaultWorker) clearBatch() {
	this.messageBatch = this.messageBatch[0:0]
	this.outstandingDeliveries = this.outstandingDeliveries[0:0]
}
func (this *defaultWorker) isComplete(strategy ShutdownStrategy) bool {
	return this.strategy == strategy && !isContextAlive(this.soft)
}
func (this *defaultWorker) sleep() {
	if this.bufferTimeout <= 0 {
		return
	}

	wait, _ := context.WithTimeout(this.soft, this.bufferTimeout)
	<-wait.Done()
}
func isContextAlive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}
