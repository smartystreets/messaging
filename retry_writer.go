package messaging

import "time"

type RetryWriter struct {
	inner Writer
	max   uint64
	sleep func(uint64)
}

func NewRetryWriter(inner Writer, options ...RetryWriterOption) *RetryWriter {
	this := &RetryWriter{inner: inner, max: 0xFFFFFFFFFFFFFFFF, sleep: defaultSleep}
	for _, option := range options {
		option(this)
	}
	return this
}
func defaultSleep(_ uint64) {
	time.Sleep(time.Second)
}

func (this *RetryWriter) Write(message Dispatch) (err error) {
	for i := uint64(0); i < this.max; i++ {
		if err = this.inner.Write(message); err == nil {
			break
		} else if err == ErrWriterClosed {
			break
		} else {
			this.sleep(i)
		}
	}

	return err
}
func (this *RetryWriter) Close() {
	this.inner.Close()
}

type RetryWriterOption func(this *RetryWriter)

func WithRetryCallback(callback func(uint64)) RetryWriterOption {
	return func(this *RetryWriter) { this.sleep = callback }
}
func WithMaxAttempts(attempts uint64) RetryWriterOption {
	return func(this *RetryWriter) { this.max = attempts }
}
