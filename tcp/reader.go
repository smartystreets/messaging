package tcp

import (
	"io"
	"net"

	"encoding/binary"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type Reader struct {
	acknowledgements chan interface{}
	deliveries       chan messaging.Delivery
	listener         net.Listener
	clock            *clock.Clock
}

func NewReader(bindAddress string, capacity int) *Reader {
	socket, _ := BindSocket(bindAddress)
	return NewReaderWithListener(socket, capacity)
}
func NewReaderWithListener(listener net.Listener, capacity int) *Reader {
	return &Reader{
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		listener:         listener,
	}
}

func (this *Reader) Listen() {
	go this.acknowledge()
	this.listen()
	close(this.deliveries)
}
func (this *Reader) acknowledge() {
	for range this.acknowledgements {
	}
}

func (this *Reader) listen() {
	if this.listener == nil {
		return
	}

	for {
		if conn, err := this.listener.Accept(); err == nil {
			go this.listenSocket(conn)
		}
	}
}
func (this *Reader) listenSocket(socket net.Conn) {
	defer socket.Close()
	var length uint16

	for {
		if binary.Read(socket, binary.LittleEndian, &length) != nil {
			break
		} else if !this.receiveMessage(socket, make([]byte, length)) {
			break
		}
	}
}
func (this *Reader) receiveMessage(reader io.Reader, buffer []byte) bool {
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return false
	}
	return this.dispatchMessage(buffer)
}
func (this *Reader) dispatchMessage(buffer []byte) bool {
	this.deliveries <- messaging.Delivery{
		Timestamp: this.clock.UTCNow(),
		Payload:   buffer,
	}
	return true
}

func (this *Reader) Close() {
	if this.listener != nil {
		this.listener.Close()
	}
}

func (this *Reader) Deliveries() <-chan messaging.Delivery { return this.deliveries }
func (this *Reader) Acknowledgements() chan<- interface{}  { return this.acknowledgements }
