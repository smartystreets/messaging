package rabbitmq

import (
	"crypto/tls"
	"net/url"
)

type BrokerEndpoint struct {
	Address   url.URL
	TLSConfig *tls.Config
}
