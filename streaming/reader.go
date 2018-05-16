package streaming

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type Reader struct {
	listener         net.Listener
	deliveries       chan messaging.Delivery
	acknowledgements chan interface{}
	tracked          map[io.Closer]struct{}
	waiter           *sync.WaitGroup
	mutex            *sync.Mutex
	closed           uint64
	clock            *clock.Clock
}

func NewReader(listener net.Listener, capacity int) *Reader {
	return &Reader{
		listener:         listener,
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		tracked:          make(map[io.Closer]struct{}),
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
	defer this.remove(socket)
	for this.read(socket) {
	}
}
func (this *Reader) read(socket io.Reader) bool {
	var length uint16 = 0
	if err := binary.Read(socket, byteOrdering, &length); err != nil {
		return false
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
	if atomic.AddUint64(&this.closed, 1) == 1 {
		return // only allow close to be called once
	}

	this.listener.Close()   // stop incoming traffic
	this.closeOpenSockets() // FUTURE: we may only want to shut down the listener and not any active streams
	this.waiter.Wait()      // once all sockets are closed
	close(this.deliveries)  // no open sockets guarantees = no more sends to this channel
}

func (this *Reader) add(socket io.Closer) {
	this.waiter.Add(1)
	this.mutex.Lock()
	this.tracked[socket] = struct{}{}
	this.mutex.Unlock()
}
func (this *Reader) remove(socket io.Closer) {
	this.mutex.Lock()
	delete(this.tracked, socket)
	this.mutex.Unlock()
	this.waiter.Done()
}
func (this *Reader) closeOpenSockets() {
	this.mutex.Lock()
	for socket := range this.tracked {
		socket.Close()
	}
	this.mutex.Unlock()
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
