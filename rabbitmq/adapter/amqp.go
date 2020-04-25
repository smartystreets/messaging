package adapter

import (
	"context"
	"net"
	"strconv"

	"github.com/streadway/amqp"
)

type amqpConnector struct{}

func (this amqpConnector) Connect(_ context.Context, socket net.Conn, config Config) (Connection, error) {
	simple := &amqp.PlainAuth{Username: config.Username, Password: config.Password}
	realConfig := amqp.Config{SASL: []amqp.Authentication{simple}, Vhost: config.VirtualHost}

	if connection, err := amqp.Open(socket, realConfig); err != nil {
		return nil, err
	} else {
		return amqpConnection{Connection: connection}, nil
	}
}

type amqpConnection struct{ *amqp.Connection }

func (this amqpConnection) Channel() (Channel, error) {
	if channel, err := this.Connection.Channel(); err != nil {
		return nil, err
	} else {
		return amqpChannel{Channel: channel}, nil
	}
}

type amqpChannel struct{ *amqp.Channel }

func (this amqpChannel) DeclareQueue(name string) error {
	_, err := this.Channel.QueueDeclare(name, true, false, false, false, amqp.Table{})
	return err
}
func (this amqpChannel) DeclareExchange(name string) error {
	return this.Channel.ExchangeDeclare(name, amqp.ExchangeFanout, true, false, false, false, amqp.Table{})
}
func (this amqpChannel) BindQueue(queue, exchange string) error {
	return this.Channel.QueueBind(queue, "", exchange, false, amqp.Table{})
}

func (this amqpChannel) BufferSize(value uint16) error {
	return this.Channel.Qos(int(value), 0, false) // false = per-consumer limit
}
func (this amqpChannel) Consume(id uint64, queue string) (<-chan amqp.Delivery, error) {
	return this.Channel.Consume(queue, strconv.FormatUint(id, 10), false, false, false, false, amqp.Table{})
}
func (this amqpChannel) Acknowledge(deliveryTag uint64, multiple bool) error {
	return this.Channel.Ack(deliveryTag, multiple)
}
func (this amqpChannel) CancelConsumer(id uint64) error {
	return this.Channel.Cancel(strconv.FormatUint(id, 10), false)
}

func (this amqpChannel) Transactional() error {
	return this.Channel.Tx()
}
func (this amqpChannel) Publish(exchange, key string, envelope amqp.Publishing) error {
	return this.Channel.Publish(exchange, key, false, false, envelope)
}
func (this amqpChannel) Commit() error {
	return this.Channel.TxCommit()
}
func (this amqpChannel) Rollback() error {
	return this.Channel.TxRollback()
}
