package rabbitmq

import (
	"crypto/tls"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type configuration struct {
	Endpoint             func() BrokerEndpoint
	Address              url.URL
	TLSConfig            *tls.Config
	TLSClient            tlsClientFunc
	Dialer               netDialer
	Connector            adapter.Connector
	Logger               messaging.Logger
	Now                  func() time.Time
	TopologyFailurePanic bool
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) DynamicAddress(value func() BrokerEndpoint) option {
	return func(this *configuration) { this.Endpoint = value }
}
func (singleton) StaticAddress(value string) option {
	return func(this *configuration) { address, _ := url.Parse(value); this.Address = *address }
}
func (singleton) StaticTLSConfig(value *tls.Config) option {
	return func(this *configuration) { this.TLSConfig = value }
}
func (singleton) Connector(value adapter.Connector) option {
	return func(this *configuration) { this.Connector = value }
}
func (singleton) Dialer(value netDialer) option {
	return func(this *configuration) { this.Dialer = value }
}
func (singleton) TLSClient(value tlsClientFunc) option {
	return func(this *configuration) { this.TLSClient = value }
}
func (singleton) PanicOnTopologyError(value bool) option {
	return func(this *configuration) { this.TopologyFailurePanic = value }
}
func (singleton) Logger(value messaging.Logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Now(value func() time.Time) option {
	return func(this *configuration) { this.Now = value }
}
func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}

		if this.Endpoint == nil {
			this.Endpoint = this.defaultBrokerEndpoint
		}

		if this.TLSClient == nil {
			this.TLSClient = this.defaultTLSClient
		}

		if this.Dialer == nil {
			this.Dialer = this.defaultDialer()
		}
	}
}
func (singleton) defaults(options ...option) []option {
	const defaultAddress = "amqp://guest:guest@127.0.0.1:5672/"
	const defaultTopologyFailurePanic = true
	var defaultNow = time.Now
	var defaultLogger = log.New(log.Writer(), log.Prefix(), log.Flags())
	var defaultTLS = &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		SessionTicketsDisabled:   true,
		CipherSuites: []uint16{
			tls.TLS_FALLBACK_SCSV,

			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, // TLS v1.2
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,   // TLS v1.2

			tls.TLS_AES_128_GCM_SHA256,       // TLS v1.3
			tls.TLS_AES_256_GCM_SHA384,       // TLS v1.3
			tls.TLS_CHACHA20_POLY1305_SHA256, // TLS v1.3
		},
	}

	return append([]option{
		Options.StaticAddress(defaultAddress),
		Options.StaticTLSConfig(defaultTLS),
		Options.Connector(adapter.New()),
		Options.PanicOnTopologyError(defaultTopologyFailurePanic),
		Options.Logger(defaultLogger),
		Options.Now(defaultNow),
	}, options...)
}

func (this configuration) defaultBrokerEndpoint() BrokerEndpoint {
	return BrokerEndpoint{Address: this.Address, TLSConfig: this.TLSConfig}
}
func (this configuration) defaultTLSClient(conn net.Conn, config *tls.Config) tlsConn {
	return tls.Client(conn, config)
}
func (this configuration) defaultDialer() netDialer {
	return newTLSDialer(&net.Dialer{}, this)
}
