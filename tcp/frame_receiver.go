package tcp

import (
	"encoding/binary"
	"io"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type FrameReceiver struct {
	acknowledgements chan interface{}
	deliveries       chan messaging.Delivery
	source           io.ReadCloser
	clock            *clock.Clock
}

func NewFrameReceiver(source io.ReadCloser, capacity int) *FrameReceiver {
	return &FrameReceiver{
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		source:           source,
	}
}

func (this *FrameReceiver) Listen() {
	defer this.source.Close()
	var length uint16

	for {
		if binary.Read(this.source, binary.LittleEndian, &length) != nil {
			break
		} else if length == 0 {
			continue
		} else if !this.receive(this.source, make([]byte, length)) {
			break
		}
	}
}
func (this *FrameReceiver) receive(reader io.Reader, buffer []byte) bool {
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return false
	}
	this.deliveries <- messaging.Delivery{Timestamp: this.clock.UTCNow(), Payload: buffer}
	return true
}

func (this *FrameReceiver) Close()                                { this.source.Close() }
func (this *FrameReceiver) Deliveries() <-chan messaging.Delivery { return this.deliveries }
func (this *FrameReceiver) Acknowledgements() chan<- interface{}  { return this.acknowledgements }
