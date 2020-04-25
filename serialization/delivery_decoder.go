package serialization

import (
	"fmt"
	"reflect"

	"github.com/smartystreets/messaging/v3"
)

type defaultDeliveryDecoder struct {
	messageTypes map[string]reflect.Type
	contentTypes map[string]Deserializer
}

func newDeliveryDecoder(config configuration) DeliveryDecoder {
	return defaultDeliveryDecoder{messageTypes: config.ReadTypes, contentTypes: config.Deserializers}
}

func (this defaultDeliveryDecoder) Decode(delivery *messaging.Delivery) error {
	if len(delivery.Payload) == 0 || delivery.Message != nil {
		return nil
	}

	instanceType, found := this.messageTypes[delivery.MessageType]
	if !found {
		return wrapError(fmt.Errorf("%w: [%s]", ErrMessageTypeNotFound, delivery.MessageType))
	}

	deserializer, found := this.contentTypes[delivery.ContentType]
	if !found {
		return wrapError(fmt.Errorf("%w: [%s]", ErrUnknownContentType, delivery.ContentType))
	}

	pointer := reflect.New(instanceType)
	if err := deserializer.Deserialize(delivery.Payload, pointer.Interface()); err != nil {
		return wrapError(err)
	}

	delivery.Message = pointer.Elem().Interface()
	return nil
}

func wrapError(err error) error {
	return fmt.Errorf("%w: %s", ErrSerializationFailure, err)
}
