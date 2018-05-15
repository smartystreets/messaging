package tcp

import "io"

type Dialer interface {
	Dial(string) (io.ReadWriteCloser, error)
}

type Mutex interface {
	Lock()
	Unlock()
}
