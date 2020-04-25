package serialization

import (
	"reflect"

	"github.com/smartystreets/messaging/v3"
)

func New(connector messaging.Connector, options ...option) messaging.Connector {
	config := configuration{
		Connector:     connector,
		Deserializers: make(map[string]Deserializer),
	}
	Options.apply(options...)(&config)
	return newConnector(config)
}

type configuration struct {
	WriteTypes map[reflect.Type]string
	ReadTypes  map[string]reflect.Type

	Deserializers map[string]Deserializer
	Serializer    Serializer

	Connector messaging.Connector
	Encoder   DispatchEncoder
	Decoder   DeliveryDecoder
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Serializer(value Serializer) option {
	return func(this *configuration) { this.Serializer = value }
}
func (singleton) AddDeserializer(value Deserializer, contentTypes ...string) option {
	return func(this *configuration) {
		for _, contentType := range contentTypes {
			this.Deserializers[contentType] = value
		}
	}
}
func (singleton) Decoder(value DeliveryDecoder) option {
	return func(this *configuration) { this.Decoder = value }
}
func (singleton) Encoder(value DispatchEncoder) option {
	return func(this *configuration) { this.Encoder = value }
}
func (singleton) ReadTypes(value map[string]reflect.Type) option {
	return func(this *configuration) { this.ReadTypes = value }
}
func (singleton) WriteTypes(value map[reflect.Type]string) option {
	return func(this *configuration) { this.WriteTypes = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}

		if this.Decoder == nil {
			this.Decoder = newDeliveryDecoder(*this)
		}
		if this.Encoder == nil {
			this.Encoder = newDispatchEncoder(*this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultSerializer = defaultSerializer{}
	const emptyContentType = ""

	return append([]option{
		Options.Serializer(defaultSerializer),
		Options.AddDeserializer(defaultSerializer, defaultSerializer.ContentType(), emptyContentType),
	}, options...)
}
