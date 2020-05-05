package serialization

import (
	"fmt"
	"reflect"

	"github.com/smartystreets/messaging/v3"
)

type defaultDeliveryDecoder struct {
	messageTypes map[string]reflect.Type
	contentTypes map[string]Deserializer
	logger       messaging.Logger
}

func newDeliveryDecoder(config configuration) DeliveryDecoder {
	return defaultDeliveryDecoder{
		messageTypes: config.ReadTypes,
		contentTypes: config.Deserializers,
		logger:       config.Logger,
	}
}

func (this defaultDeliveryDecoder) Decode(delivery *messaging.Delivery) error {
	if len(delivery.Payload) == 0 || delivery.Message != nil {
		return nil
	}

	instanceType, found := this.messageTypes[delivery.MessageType]
	if !found {
		this.logger.Printf("[WARN] Unable to decode message of type [%s].", delivery.MessageType)
		return wrapError(fmt.Errorf("%w: [%s]", ErrMessageTypeNotFound, delivery.MessageType))
	}

	deserializer, found := this.contentTypes[delivery.ContentType]
	if !found {
		this.logger.Printf("[WARN] Unable to decode message with Content-Type [%s].", delivery.ContentType)
		return wrapError(fmt.Errorf("%w: [%s]", ErrUnknownContentType, delivery.ContentType))
	}

	pointer := reflect.New(instanceType)
	if err := deserializer.Deserialize(delivery.Payload, pointer.Interface()); err != nil {
		this.logger.Printf("[WARN] Failed to deserialize message of [%s]: %s", delivery.MessageType, err)
		return wrapError(err)
	}

	delivery.Message = pointer.Elem().Interface()
	return nil
}

func wrapError(err error) error {
	return fmt.Errorf("%w: %s", ErrSerializationFailure, err)
}
