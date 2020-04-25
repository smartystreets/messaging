package serialization

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	messaging.Connector
	decoder DeliveryDecoder
	encoder DispatchEncoder
}

func newConnector(config configuration) messaging.Connector {
	return defaultConnector{
		Connector: config.Connector,
		decoder:   config.Decoder,
		encoder:   config.Encoder,
	}
}
func (this defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	if connection, err := this.Connector.Connect(ctx); err != nil {
		return nil, err
	} else {
		// NOTE: pointer receiver allows pointer equality when connection equality between two instances
		return &defaultConnection{Connection: connection, decoder: this.decoder, encoder: this.encoder}, nil
	}
}

type defaultConnection struct {
	messaging.Connection
	decoder DeliveryDecoder
	encoder DispatchEncoder
}

func (this *defaultConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	if reader, err := this.Connection.Reader(ctx); err != nil {
		return nil, err
	} else {
		return defaultReader{Reader: reader, decoder: this.decoder}, nil
	}
}
func (this defaultConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	if writer, err := this.Connection.Writer(ctx); err != nil {
		return nil, err
	} else {
		return defaultWriter{CommitWriter: writer.(messaging.CommitWriter), encoder: this.encoder}, nil
	}
}
func (this defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	if commitWriter, err := this.Connection.CommitWriter(ctx); err != nil {
		return nil, err
	} else {
		return defaultWriter{CommitWriter: commitWriter, encoder: this.encoder}, nil
	}
}

type defaultReader struct {
	messaging.Reader
	decoder DeliveryDecoder
}
type defaultStream struct {
	messaging.Stream
	decoder DeliveryDecoder
}

func (this defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	if inner, err := this.Reader.Stream(ctx, config); err != nil {
		return nil, err
	} else {
		return defaultStream{Stream: inner, decoder: this.decoder}, nil
	}
}
func (this defaultStream) Read(ctx context.Context, delivery *messaging.Delivery) error {
	if err := this.Stream.Read(ctx, delivery); err != nil {
		return err
	}

	return this.decoder.Decode(delivery)
}

type defaultWriter struct {
	messaging.CommitWriter
	encoder DispatchEncoder
}

func (this defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	for i, _ := range dispatches {
		if err := this.encoder.Encode(&dispatches[i]); err != nil {
			return -1, err
		}
	}

	return this.CommitWriter.Write(ctx, dispatches...)
}
