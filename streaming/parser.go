package streaming

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type parser struct {
	source   net.Conn
	target   chan<- messaging.Delivery
	deadline time.Duration
	clock    *clock.Clock
}

func newParser(source net.Conn, target chan<- messaging.Delivery, deadline time.Duration) *parser {
	return &parser{source: source, target: target, deadline: deadline}
}

func (this *parser) Parse() {
	defer this.source.Close()
	var length uint16

	for {
		this.source.SetReadDeadline(this.clock.UTCNow().Add(this.deadline))
		if err := binary.Read(this.source, binary.LittleEndian, &length); isFatalError(err) {
			break
		} else if length == 0 {
			continue
		} else if !this.parse(make([]byte, length)) {
			break
		}
	}
}
func (this *parser) parse(buffer []byte) bool {
	this.source.SetReadDeadline(this.clock.UTCNow().Add(this.deadline))
	if _, err := io.ReadFull(this.source, buffer); isFatalError(err) {
		return false
	}

	this.target <- messaging.Delivery{Timestamp: this.clock.UTCNow(), Payload: buffer}
	return true
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	} else if err == io.EOF {
		return true
	} else if isTimeout(err) {
		return false
	} else {
		return true
	}
}
func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func (this *parser) Close() { this.source.Close() }
