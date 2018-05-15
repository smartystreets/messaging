package tcp

import (
	"compress/gzip"
	"io"
)

type GZipDialer struct {
	inner Dialer
	level int
}

func NewGZipDialer(dialer Dialer, level int) *GZipDialer {
	return &GZipDialer{inner: dialer, level: level}
}

func (this *GZipDialer) Dial(address string) (io.ReadWriteCloser, error) {
	socket, err := this.inner.Dial(address)
	if err != nil {
		return nil, err
	}

	writer, err := gzip.NewWriterLevel(socket, this.level)
	if err != nil {
		return nil, err
	}

	reader, err := gzip.NewReader(socket)
	if err != nil {
		return nil, err
	}

	return gzippedConnection{ReadCloser: reader, WriteCloser: writer}, nil
}

type gzippedConnection struct {
	io.ReadCloser
	io.WriteCloser
}

func (this gzippedConnection) Close() error {
	this.ReadCloser.Close()
	this.WriteCloser.Close()
	return nil
}
