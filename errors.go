package messaging

import "errors"

var (
	WriterClosedError         = errors.New("the writer has been closed and can no longer be used")
	BrokerShuttingDownError   = errors.New("broker is still shutting down")
	EmptyDispatchError        = errors.New("unable to write an empty dispatch")
	MessageTypeDiscoveryError = errors.New("unable to discover message type")
	UnroutableDispatchError   = errors.New("the dispatch did not contain a destination")
)
