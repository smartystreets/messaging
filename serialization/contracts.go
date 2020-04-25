package serialization

import (
	"errors"

	"github.com/smartystreets/messaging/v3"
)

type Serializer interface {
	Serialize(source interface{}) ([]byte, error)
}
type Deserializer interface {
	Deserialize(source []byte, target interface{}) error
}

type DeliveryDecoder interface {
	Decode(*messaging.Delivery) error
}
type DispatchEncoder interface {
	Encode(*messaging.Dispatch) error
}

var (
	ErrSerializationFailure        = errors.New("serialization failure")
	ErrUnknownContentType          = errors.New("the content type provided was not understood")
	ErrMessageTypeNotFound         = errors.New("the message type provided was not understood")
	ErrDeserializeMalformedPayload = errors.New("the payload provided was not understood by the deserializer")
	ErrSerializeUnsupportedType    = errors.New("the type provided cannot be serialized")
)
