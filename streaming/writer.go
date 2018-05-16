package streaming

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/smartystreets/messaging"
)

type Writer struct {
	socket io.WriteCloser
}

func NewWriter(socket io.WriteCloser) *Writer {
	return &Writer{socket: socket}
}

func (this *Writer) Write(dispatch messaging.Dispatch) error {
	payloadSize := len(dispatch.Payload)
	if payloadSize == 0 {
		return nil
	}

	if payloadSize > maxFrameSize {
		return payloadTooLarge
	}

	if err := binary.Write(this.socket, byteOrdering, uint16(payloadSize)); err != nil {
		return err
	}

	if _, err := this.socket.Write(dispatch.Payload); err != nil {
		return err
	}

	return nil
}

func (this *Writer) Close() {
	this.socket.Close()
}

const maxFrameSize = 64*1024 - 2

var (
	payloadTooLarge = errors.New("payload is too large")
	byteOrdering    = binary.LittleEndian
)
