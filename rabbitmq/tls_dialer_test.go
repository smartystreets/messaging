package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestDialerFixture(t *testing.T) {
	gunit.Run(new(DialerFixture), t)
}

type DialerFixture struct {
	*gunit.Fixture

	backgroundContext context.Context
	dialer            netDialer
	brokerAddress     url.URL
	tlsConfig         *tls.Config

	dialedContext  context.Context
	dialedNetwork  string
	dialedAddress  string
	dialConnection net.Conn
	dialError      error

	dialedTLSConn    net.Conn
	dialedTLSConfig  *tls.Config
	handshakeError   error
	callsToHandshake int
	callsToClose     int
}

func (this *DialerFixture) Setup() {
	this.backgroundContext = context.Background()
	address, _ := url.Parse("amqps://localhost:5672/")
	this.brokerAddress = *address
	this.tlsConfig = &tls.Config{}
	this.dialConnection = this

	this.dialer = newTLSDialer(this, configuration{
		Endpoint:  this.brokerEndpoint,
		TLSClient: this.tlsClient,
	})
}

func (this *DialerFixture) TestWhenDialingFails_ReturnUnderlyingError() {
	this.dialError = errors.New("")
	this.dialConnection = nil

	conn, err := this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(conn, should.BeNil)
	this.So(err, should.Equal, this.dialError)
	this.So(this.dialedContext, should.Equal, this.backgroundContext)
	this.So(this.dialedNetwork, should.Equal, "network")
	this.So(this.dialedAddress, should.Equal, "address")
	this.So(this.callsToHandshake, should.Equal, 0)
}

func (this *DialerFixture) TestWhenNoTLSConfigSpecified_DoNotEstablishTLSConnection() {
	this.tlsConfig = nil

	conn, err := this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(conn, should.Equal, this)
	this.So(err, should.BeNil)
	this.So(this.callsToHandshake, should.Equal, 0)
}

func (this *DialerFixture) TestWhenAddressSchemaDoesNotSpecifyAMQPS_DoNotEstablishTLSConnection() {
	this.brokerAddress.Scheme = "amqp"

	conn, err := this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(conn, should.Equal, this)
	this.So(err, should.BeNil)
	this.So(this.callsToHandshake, should.Equal, 0)
}

func (this *DialerFixture) TestWhenNoServerNameSpecified_UseServerNameFromBrokerAddress() {
	_, _ = this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(this.tlsConfig.ServerName, should.Equal, "localhost")
}

func (this *DialerFixture) TestWhenEstablishingTLSConnection_ReturnTLSConnection() {
	conn, err := this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(conn, should.Equal, this)
	this.So(err, should.BeNil)
	this.So(this.callsToHandshake, should.Equal, 1)
	this.So(this.dialedTLSConn, should.Equal, this)
	this.So(this.dialedTLSConfig, should.Equal, this.tlsConfig)
}

func (this *DialerFixture) TestWhenHandshakeFails_CloseConnectionAndReturnError() {
	this.handshakeError = errors.New("")
	conn, err := this.dialer.DialContext(this.backgroundContext, "network", "address")

	this.So(conn, should.BeNil)
	this.So(err, should.Equal, this.handshakeError)
	this.So(this.callsToHandshake, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DialerFixture) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	this.dialedContext = ctx
	this.dialedNetwork = network
	this.dialedAddress = address
	return this.dialConnection, this.dialError
}

func (this *DialerFixture) brokerEndpoint() BrokerEndpoint {
	return BrokerEndpoint{Address: this.brokerAddress, TLSConfig: this.tlsConfig}
}
func (this *DialerFixture) tlsClient(conn net.Conn, config *tls.Config) tlsConn {
	this.dialedTLSConn = conn
	this.dialedTLSConfig = config
	return this
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DialerFixture) Handshake() error                   { this.callsToHandshake++; return this.handshakeError }
func (this *DialerFixture) Read(b []byte) (n int, err error)   { panic("nop") }
func (this *DialerFixture) Close() error                       { this.callsToClose++; return nil }
func (this *DialerFixture) LocalAddr() net.Addr                { panic("nop") }
func (this *DialerFixture) RemoteAddr() net.Addr               { panic("nop") }
func (this *DialerFixture) SetDeadline(t time.Time) error      { panic("nop") }
func (this *DialerFixture) SetReadDeadline(t time.Time) error  { panic("nop") }
func (this *DialerFixture) SetWriteDeadline(t time.Time) error { panic("nop") }
