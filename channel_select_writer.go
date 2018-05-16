package messaging

import "errors"

type ChannelSelectWriter struct {
	input chan Dispatch
	inner Writer
}

func NewChannelSelectWriter(writer Writer, capacity int) *ChannelSelectWriter {
	return &ChannelSelectWriter{
		input: make(chan Dispatch, capacity),
		inner: writer,
	}
}

func (this *ChannelSelectWriter) Listen() {
	for buffer := range this.input {
		this.inner.Write(buffer)
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
