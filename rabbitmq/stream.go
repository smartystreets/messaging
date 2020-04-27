package rabbitmq

import (
	"context"
	"sync"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
	"github.com/streadway/amqp"
)

type defaultStream struct {
	channel    adapter.Channel
	deliveries <-chan amqp.Delivery
	streamID   string
	batchAck   bool
	closer     sync.Once
}

func newStream(channel adapter.Channel, deliveries <-chan amqp.Delivery, id string, exclusive bool) messaging.Stream {
	return &defaultStream{channel: channel, deliveries: deliveries, streamID: id, batchAck: exclusive}
}

func (this *defaultStream) Read(ctx context.Context, delivery *messaging.Delivery) error {
	panic("implement me")
}

func (this *defaultStream) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	panic("implement me")
}

func (this *defaultStream) Close() (err error) {
	this.closer.Do(func() { err = this.channel.CancelConsumer(this.streamID) })
	return err
}
