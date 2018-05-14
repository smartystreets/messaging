package tcp

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type PacketReader struct {
	acknowledgements chan interface{}
	deliveries       chan messaging.Delivery
	listener         net.Listener
	clock            *clock.Clock
}

func NewPacketReader(bindAddress string, capacity int) *PacketReader {
	socket, _ := BindSocket(bindAddress)
	return NewPacketReaderWithListener(socket, capacity)
}
func NewPacketReaderWithListener(listener net.Listener, capacity int) *PacketReader {
	return &PacketReader{
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		listener:         listener,
	}
}

func (this *PacketReader) Listen() {
	go this.acknowledge()
	this.listen()
	close(this.deliveries)
}
func (this *PacketReader) acknowledge() {
	for range this.acknowledgements {
	}
}

func (this *PacketReader) listen() {
	if this.listener == nil {
		return
	}

	for {
		if conn, err := this.listener.Accept(); err == nil {
			go this.listenSocket(conn)
		}
	}
}
func (this *PacketReader) listenSocket(socket net.Conn) {
	defer socket.Close()
	var length uint16

	for {
		if binary.Read(socket, endianness, &length) != nil {
			break
		} else if !this.receiveMessage(socket, make([]byte, length)) {
			break
		}
	}
}
func (this *PacketReader) receiveMessage(reader io.Reader, buffer []byte) bool {
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return false
	}
	return this.dispatchMessage(buffer)
}
func (this *PacketReader) dispatchMessage(buffer []byte) bool {
	this.deliveries <- messaging.Delivery{
		Timestamp: this.clock.UTCNow(),
		Payload:   buffer,
	}
	return true
}

func (this *PacketReader) Close() {
	if this.listener != nil {
		this.listener.Close()
	}
}

func (this *PacketReader) Deliveries() <-chan messaging.Delivery { return this.deliveries }
func (this *PacketReader) Acknowledgements() chan<- interface{}  { return this.acknowledgements }

const (
	readDeadlineDuration = time.Second
	readBufferSize       = 64 * 1024 // 64K bytes
)

var endianness = binary.LittleEndian
