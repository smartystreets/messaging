package messaging

import (
	"context"
	"io"
	"time"
)

type Handler interface {
	Handle(ctx context.Context, messages ...interface{})
}

type Connector interface {
	Connect(ctx context.Context) (Connection, error)
	io.Closer
}
type Connection interface {
	Reader(ctx context.Context) (Reader, error)
	Writer(ctx context.Context) (Writer, error)
	CommitWriter(ctx context.Context) (CommitWriter, error)
	io.Closer
}

type Reader interface {
	Stream(ctx context.Context, config StreamConfig) (Stream, error)
	io.Closer
}
type StreamConfig struct {
	EstablishTopology bool
	ExclusiveStream   bool
	BufferSize        uint16
	Queue             string
	Topics            []string
}
type Stream interface {
	Read(ctx context.Context, delivery *Delivery) error
	Acknowledge(ctx context.Context, deliveries ...Delivery) error
	io.Closer
}

type Writer interface {
	Write(ctx context.Context, dispatches ...Dispatch) (int, error)
	io.Closer
}
type CommitWriter interface {
	Writer
	Commit() error
	Rollback() error
}

type Dispatch struct {
	SourceID        uint64
	MessageID       uint64
	CorrelationID   uint64
	Timestamp       time.Time
	Expiration      time.Duration
	Durable         bool
	Topic           string
	Partition       uint64
	MessageType     string
	ContentType     string
	ContentEncoding string
	Payload         []byte
	Headers         map[string]interface{}
	Message         interface{}
}
type Delivery struct {
	DeliveryID      uint64
	SourceID        uint64
	MessageID       uint64
	CorrelationID   uint64
	Timestamp       time.Time
	Durable         bool
	MessageType     string
	ContentType     string
	ContentEncoding string
	Payload         []byte
	Headers         map[string]interface{}
	Message         interface{}
}

type Listener interface {
	Listen()
}
type ListenCloser interface {
	Listener
	io.Closer
}
