package streaming

import (
	"github.com/smartystreets/messaging"
	"github.com/smartystreets/transports"
)

type DialWriter struct {
	dialer  transports.Dialer
	network string
	address string
	current *Writer
}

func NewDialWriter() *DialWriter {
	return &DialWriter{}
}

func (this *DialWriter) Write(dispatch messaging.Dispatch) error {
	if writer, err := this.open(); err != nil {
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
