package rabbitmq

import (
	"context"
	"crypto/tls"
	"net/url"
	"sync"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type BrokerEndpoint struct {
	Address   url.URL
	TLSConfig *tls.Config
}

type defaultConnector struct {
	inner  adapter.Connector
	dialer netDialer
	broker func() BrokerEndpoint
	config configuration

	active []messaging.Connection
	mutex  sync.Mutex
}

func newConnector(config configuration) messaging.Connector {
	return &defaultConnector{
		inner:  config.Connector,
		dialer: config.Dialer,
		broker: config.Endpoint,
		config: config,
	}
}

func (this *defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	hostAddress, config := this.configuration()
	socket, err := this.dialer.DialContext(ctx, "tcp", hostAddress)
	if err != nil {
		return nil, err
	}

	amqpConnection, err := this.inner.Connect(ctx, socket, config)
	if err != nil {
		return nil, err
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.active = append(this.active, newConnection(amqpConnection, this.config))
	return this.active[len(this.active)-1], nil
}
func (this *defaultConnector) configuration() (string, adapter.Config) {
	broker := this.broker()
	username, password := parseAuthentication(broker.Address.User)
	return broker.Address.Host, adapter.Config{
		Username:    username,
		Password:    password,
		VirtualHost: broker.Address.Path,
	}
}
func parseAuthentication(info *url.Userinfo) (string, string) {
	if info == nil {
		return "guest", "guest"
	}

	password, _ := info.Password()
	return info.Username(), password
}

func (this *defaultConnector) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i := range this.active {
		_ = this.active[i].Close()
		this.active[i] = nil
	}
	this.active = this.active[0:0]

	return nil
}

type defaultConnection struct {
	inner  adapter.Connection
	config configuration
}

func newConnection(inner adapter.Connection, config configuration) messaging.Connection {
	// NOTE: using pointer type to allow for pointer equality check
	return &defaultConnection{inner: inner, config: config}
}
func (this defaultConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("implement me")
}
func (this defaultConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("implement me")
}
func (this defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	panic("implement me")
}
func (this defaultConnection) Close() error {
	return this.inner.Close()
}
