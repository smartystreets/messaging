package rabbitmq

import (
	"context"
	"crypto/tls"
	"net"
)

type (
	netDialer interface {
		DialContext(ctx context.Context, network, address string) (net.Conn, error)
	}
	tlsConn interface {
		net.Conn
		Handshake() error
	}
	tlsClientFunc func(conn net.Conn, config *tls.Config) tlsConn
)

type tlsDialer struct {
	netDialer
	endpoint func() BrokerEndpoint
	client   tlsClientFunc
}

func newTLSDialer(dialer netDialer, config configuration) netDialer {
	return tlsDialer{netDialer: dialer, endpoint: config.Endpoint, client: config.TLSClient}
}

func (this tlsDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := this.netDialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	target := this.endpoint()
	if target.TLSConfig == nil || target.Address.Scheme != "amqps" {
		return conn, nil
	}

	if len(target.TLSConfig.ServerName) == 0 {
		target.TLSConfig.ServerName, _, _ = net.SplitHostPort(target.Address.Host)
	}

	tlsConn := this.client(conn, target.TLSConfig)
	if err = tlsConn.Handshake(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return tlsConn, nil
}
