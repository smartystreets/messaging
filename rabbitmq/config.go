package rabbitmq

type configuration struct {
	Endpoint  func() BrokerEndpoint
	TLSClient tlsClientFunc
}
