package rabbitmq

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
	"github.com/streadway/amqp"
)

type defaultReader struct {
	streams []io.Closer
	inner   adapter.Channel
	config  configuration
	mutex   sync.Mutex
	counter uint64
}

func newReader(inner adapter.Channel, config configuration) messaging.Reader {
	return &defaultReader{inner: inner, config: config}
}
func (this *defaultReader) Stream(_ context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if config.ExclusiveStream && len(this.streams) > 0 {
		return nil, ErrMultipleStreams // NOTE: what if the first stream is exclusive???
	}

	if err := establishTopology(this.inner, config); err != nil {
		_ = this.inner.Close()
		return nil, this.tryPanic(err)
	}

	if err := this.inner.BufferSize(config.BufferSize); err != nil {
		_ = this.inner.Close()
		return nil, err
	}

	streamID := strconv.FormatUint(this.counter, 10)
	deliveries, err := this.inner.Consume(streamID, config.Queue)
	if err != nil {
		_ = this.inner.Close()
		return nil, err
	}

	stream := newStream(this.inner, deliveries, streamID, config.ExclusiveStream)
	this.counter++
	this.streams = append(this.streams, stream)
	return stream, nil
}
func establishTopology(channel adapter.Channel, config messaging.StreamConfig) error {
	if !config.EstablishTopology {
		return nil
	}

	if err := channel.DeclareQueue(config.Queue); err != nil {
		return err
	}

	for _, topic := range config.Topics {
		if err := channel.DeclareExchange(topic); err != nil {
			return err
		}
		if err := channel.BindQueue(config.Queue, topic); err != nil {
			return err
		}
	}

	return nil
}

func (this *defaultReader) tryPanic(err error) error {
	if err == nil || !this.config.TopologyFailurePanic {
		return err
	}

	if brokerError, ok := err.(*amqp.Error); ok && brokerError.Code == http.StatusNotAcceptable {
		panic(err)
	}

	return err
}

func (this *defaultReader) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i, stream := range this.streams {
		this.streams[i] = nil
		_ = stream.Close()
	}

	this.streams = this.streams[0:0]
	return this.inner.Close()
}

var ErrMultipleStreams = errors.New("unable to open an exclusive stream, another stream already exists")
