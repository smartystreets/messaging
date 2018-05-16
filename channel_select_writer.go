package messaging

import "errors"

type ChannelSelectWriter struct {
	input  chan Dispatch
	output Writer
}

func NewChannelSelectWriter(writer Writer, capacity int) *ChannelSelectWriter {
	return &ChannelSelectWriter{
		input:  make(chan Dispatch, capacity),
		output: writer,
	}
}

func (this *ChannelSelectWriter) Listen() {
	for buffer := range this.input {
		this.output.Write(buffer)
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

var WriteDiscardedError = errors.New("the write was discarded because the channel was full")
