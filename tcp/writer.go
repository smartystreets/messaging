package tcp

import (
	"encoding/binary"
	"errors"
	"net"

	"github.com/smartystreets/messaging"
)

type Writer struct {
	socket net.Conn
}

func NewWriter(socket net.Conn) *Writer {
	return &Writer{socket: socket}
}

func (this *Writer) Write(dispatch messaging.Dispatch) error {
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

const maxPayloadSize = 64*1024 - 1

var payloadTooLarge = errors.New("payload is too large")
