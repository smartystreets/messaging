package streaming

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/smartystreets/messaging"
)

type Reader struct {
	listener         net.Listener
	deliveries       chan messaging.Delivery
	acknowledgements chan interface{}
	sockets          []io.Closer
	waiter           *sync.WaitGroup
}

func NewReader(listener net.Listener, capacity int) *Reader {
	return &Reader{
		listener:         listener,
		acknowledgements: make(chan interface{}, capacity),
		deliveries:       make(chan messaging.Delivery, capacity),
		waiter:           &sync.WaitGroup{},
	}
}

func (this *Reader) Listen() {
	go this.acknowledge()
	this.listen()
	this.waiter.Wait()
	close(this.deliveries)
}
func (this *Reader) listen() {
	for {
		if socket, err := this.listener.Accept(); err == nil {
			this.beginParse(socket)
		} else {
			// TODO: depending upon the type of error, e.g. the bind is closed, break
		}
	}
}
func (this *Reader) beginParse(socket net.Conn) {
	this.sockets = append(this.sockets, socket)
	this.waiter.Add(1)
	go this.parse(socket)
}
func (this *Reader) parse(socket net.Conn) {
	parser := newParser(socket, this.deliveries, readDeadline)
	parser.Parse()
	this.waiter.Done()
}

func (this *Reader) Close() {
	this.listener.Close()
	for _, socket := range this.sockets {
		socket.Close()
	}
	this.sockets = nil
}

func (this *Reader) acknowledge() {
	for range this.acknowledgements {
	}
}

func (this *Reader) Deliveries() <-chan messaging.Delivery { return this.deliveries }
func (this *Reader) Acknowledgements() chan<- interface{}  { return this.acknowledgements }

const readDeadline = time.Second
