package streaming

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type Reader struct {
	listener         net.Listener
	deliveries       chan messaging.Delivery
	acknowledgements chan interface{}
	open             map[io.Closer]struct{}
	waiter           *sync.WaitGroup
	mutex            *sync.Mutex
	clock            *clock.Clock
}

func NewReader(listener net.Listener, capacity int) *Reader {
	return &Reader{
		listener:         listener,
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		open:             make(map[io.Closer]struct{}),
		waiter:           &sync.WaitGroup{},
		mutex:            &sync.Mutex{},
	}
}

func (this *Reader) Listen() {
	go this.acknowledge()
	for {
		if socket, err := this.listener.Accept(); err == nil {
			this.add(socket)
			go this.parse(socket)
		} else if strings.Contains(err.Error(), closedAcceptSocketError) {
			break
		}
	}
}

func (this *Reader) parse(socket io.ReadCloser) {
	for this.read(socket) {
	}
	this.remove(socket)
}
func (this *Reader) read(socket io.Reader) bool {
	var length uint16 = 0
	if err := binary.Read(socket, byteOrdering, &length); err != nil {
		return false
	} else if length == 0 {
		return true
	}

	buffer := make([]byte, length)
	if _, err := io.ReadFull(socket, buffer); err != nil {
		return false
	} else {
		this.deliveries <- messaging.Delivery{Timestamp: this.clock.UTCNow(), Payload: buffer}
		return true
	}
}

func (this *Reader) Close() {
	this.listener.Close() // stop incoming traffic

	this.mutex.Lock()
	for socket := range this.open {
		socket.Close()
	}
	this.mutex.Unlock()

	this.waiter.Wait()
	close(this.deliveries)
}

func (this *Reader) add(socket io.Closer) {
	this.waiter.Add(1)
	this.mutex.Lock()
	this.open[socket] = struct{}{}
	this.mutex.Unlock()
}
func (this *Reader) remove(socket io.Closer) {
	this.mutex.Lock()
	delete(this.open, socket)
	this.mutex.Unlock()
	this.waiter.Done()
}

func (this *Reader) acknowledge() {
	for range this.acknowledgements {
	}
}

func (this *Reader) Deliveries() <-chan messaging.Delivery { return this.deliveries }
func (this *Reader) Acknowledgements() chan<- interface{}  { return this.acknowledgements }

// https://github.com/golang/go/issues/4373
// https://github.com/golang/go/issues/19252
const closedAcceptSocketError = "use of closed network connection"
