package rabbitmq

import (
	"context"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type defaultReader struct {
}

func newReader(inner adapter.Channel, config configuration) messaging.Reader {
	return &defaultReader{}
}

func (this *defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	panic("implement me1")
}
func (this *defaultReader) Close() error {
	panic("implement me2")
}
