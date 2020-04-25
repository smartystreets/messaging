package adapter

import (
	"context"
	"io"
	"net"

	"github.com/streadway/amqp"
)

func New() Connector { return amqpConnector{} }

type Config struct {
	Username    string
	Password    string
	VirtualHost string
}

type Connector interface {
	Connect(ctx context.Context, socket net.Conn, config Config) (Connection, error)
}

type Connection interface {
	Channel() (Channel, error)
	io.Closer
}

type Channel interface {
	DeclareQueue(name string) error
	DeclareExchange(name string) error
	BindQueue(queue, exchange string) error

	BufferSize(value uint16) error
	Consume(id uint64, queue string) (<-chan amqp.Delivery, error)
	Acknowledge(deliveryTag uint64, multiple bool) error
	CancelConsumer(id uint64) error

	Publish(exchange, key string, envelope amqp.Publishing) error
	Transactional() error
	Commit() error
	Rollback() error

	io.Closer
}
