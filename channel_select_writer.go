package messaging

import "errors"

type ChannelSelectWriter struct {
	input chan Dispatch
	inner Writer
}

func NewChannelSelectWriter(writer Writer, options ...ChannelSelectWriterOption) *ChannelSelectWriter {
	this := &ChannelSelectWriter{inner: writer, input: make(chan Dispatch, defaultChannelCapacity)}
	for _, option := range options {
		option(this)
	}
	return this
}

func (this *ChannelSelectWriter) Listen() {
	for buffer := range this.input {
		_ = this.inner.Write(buffer)
	}
}

func (this *ChannelSelectWriter) Write(dispatch Dispatch) error {
	if len(dispatch.Payload) == 0 {
		return nil
	}

	select {
	case this.input <- dispatch:
		return nil
	default:
		return WriteDiscardedError
	}
}

func (this *ChannelSelectWriter) Close() {
	this.inner.Close()
}

var WriteDiscardedError = errors.New("the write was discarded because the channel was full")

const defaultChannelCapacity = 4096

/////////////////////////////////////////////////

type ChannelSelectWriterOption func(*ChannelSelectWriter)

func WithChannelCapacity(capacity int) ChannelSelectWriterOption {
	return func(this *ChannelSelectWriter) { this.input = make(chan Dispatch, capacity) }
}
func WithChannelStart() func(*ChannelSelectWriter) {
	return func(this *ChannelSelectWriter) { go this.Listen() }
}
