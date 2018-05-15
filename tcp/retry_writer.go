package tcp

import (
	"net"

	"github.com/smartystreets/messaging"
)

type RetryWriter struct {
	mutex   Mutex
	address string
	current messaging.Writer
	dialer  net.Dialer
}

func NewRetryWriter(address string, dialer net.Dialer, options ...WriterOption) *RetryWriter {
	this := &RetryWriter{mutex: nopMutex{}, address: address, dialer: dialer}

	for _, option := range options {
		option(this)
	}

	return this
}

func (this *RetryWriter) Write(dispatch messaging.Dispatch) error {
	if writer, err := this.writer(); err != nil {
		return err
	} else if err = writer.Write(dispatch); err != nil {
		this.Close()
		return err
	} else {
		return nil
	}
}
func (this *RetryWriter) writer() (messaging.Writer, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.current == nil {
		return this.connect()
	}

	return this.current, nil
}
func (this *RetryWriter) connect() (messaging.Writer, error) {
	socket, err := this.dialer.Dial("tcp", this.address)
	if err != nil {
		return nil, err
	}

	this.current = NewFrameWriter(socket)
	return this.current, nil
}
func (this *RetryWriter) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.current != nil {
		this.current.Close()
	}

	this.current = nil
}

//////////////////////////////////////////

type WriterOption func(*RetryWriter)

func WithMutex(mutex Mutex) WriterOption { return func(this *RetryWriter) { this.mutex = mutex } }
