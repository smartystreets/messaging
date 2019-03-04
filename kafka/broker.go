package kafka

import (
	"sync"

	"github.com/smartystreets/messaging"
)

type Broker struct {
	mutex sync.Locker
	state uint64
}

func (this *Broker) Connect() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.state == messaging.Disconnecting {
		return messaging.BrokerShuttingDownError
	} else if this.state == messaging.Disconnected {
		this.updateState(messaging.Connecting)
	}

	return nil
}
func (this *Broker) Disconnect() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.state == messaging.Disconnecting || this.state == messaging.Disconnected {
		return
	}

	this.updateState(messaging.Disconnecting)
	//this.initiateReaderShutdown()
	//this.initiateWriterShutdown()
	//this.completeShutdown()
	this.updateState(messaging.Disconnected)
}

func (this *Broker) OpenReader(_ string, bindings ...string) messaging.Reader {
	return this.OpenTransientReader(bindings)
}
func (this *Broker) OpenTransientReader(bindings []string) messaging.Reader {
	return nil
}

func (this *Broker) OpenWriter() messaging.Writer {
	return nil
}
func (this *Broker) OpenTransactionalWriter() messaging.CommitWriter {
	return nil
}

func (this *Broker) State() uint64 {
	this.mutex.Lock()
	state := this.state
	this.mutex.Unlock()
	return state
}
func (this *Broker) updateState(state uint64) {
	this.state = state
}
