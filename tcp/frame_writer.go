package tcp

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/smartystreets/messaging"
)

type FrameWriter struct {
	socket io.WriteCloser
}

func NewFrameWriter(socket io.WriteCloser) *FrameWriter {
	return &FrameWriter{socket: socket}
}

func (this *FrameWriter) Write(dispatch messaging.Dispatch) error {
	payloadSize := len(dispatch.Payload)
	if payloadSize == 0 {
		return nil
	}

	if payloadSize > maxPayloadSize {
		return payloadTooLarge
	}

	if err := binary.Write(this.socket, binary.LittleEndian, uint16(payloadSize)); err != nil {
		return err
	}

	if _, err := this.socket.Write(dispatch.Payload); err != nil {
		return err
	}

	return nil
}

func (this *FrameWriter) Close() {
	this.socket.Close()
}

const maxPayloadSize = 64*1024 - 1

var payloadTooLarge = errors.New("payload is too large")
