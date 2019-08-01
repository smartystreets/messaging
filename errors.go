package messaging

import "errors"

var (
	ErrWriterClosed         = errors.New("the writer has been closed and can no longer be used")
	ErrBrokerShuttingDown   = errors.New("broker is still shutting down")
	ErrEmptyDispatch        = errors.New("unable to write an empty dispatch")
	ErrMessageTypeDiscovery = errors.New("unable to discover message type")
	ErrUnroutableDispatch   = errors.New("the dispatch did not contain a destination")
)
