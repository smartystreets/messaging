package tcp

import (
	"crypto/tls"
	"io"
)

type TLSDialer struct {
	config *tls.Config
}

func NewTLSDialer(config *tls.Config) *TLSDialer {
	return &TLSDialer{config: config}
}

func (this *TLSDialer) Dial(address string) (io.ReadWriteCloser, error) {
	return tls.Dial("tcp", address, this.config)
}
