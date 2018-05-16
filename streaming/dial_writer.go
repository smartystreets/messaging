package streaming

import (
	"net"

	"github.com/smartystreets/messaging"
)

type dialer interface {
	Dial(string, string) (net.Conn, error)
}

type DialWriter struct {
	dialer  dialer
	network string
	address string
	current *Writer
}

func NewDialWriter(dialer dialer, network, address string) *DialWriter {
	return &DialWriter{dialer: dialer, network: network, address: address}
}

func (this *DialWriter) Write(dispatch messaging.Dispatch) error {
	if writer, err := this.open(); err != nil {
		this.Close()
		return err
	} else {
		return writer.Write(dispatch)
	}
}
func (this *DialWriter) open() (*Writer, error) {
	if this.current != nil {
		return this.current, nil
	}

	socket, err := this.dialer.Dial(this.network, this.address)
	if err != nil {
		return nil, err
	} else {
		this.current = NewWriter(socket)
		return this.current, nil
	}
}

func (this *DialWriter) Close() {
	current := this.current
	if current != nil {
		this.current.Close()
	}
}
