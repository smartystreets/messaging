package streaming

import (
	"net"
	"sync"
	"time"

	"github.com/smartystreets/messaging"
)

type Reader struct {
	listener         net.Listener
	deliveries       chan messaging.Delivery
	acknowledgements chan interface{}
	sockets          []*parser
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
	for this.handle(this.listener.Accept()) {
	}
}
func (this *Reader) handle(socket net.Conn, err error) bool {
	if err != nil {
		return false // TODO: depending upon the type of error, e.g. the bind is closed, break
	}

	parser := newParser(socket, this.deliveries, readDeadline)
	this.sockets = append(this.sockets, parser)
	this.waiter.Add(1)
	go this.parse(parser)
	return true
}
func (this *Reader) parse(parser *parser) {
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
