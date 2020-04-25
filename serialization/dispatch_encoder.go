package serialization

import (
	"fmt"
	"reflect"

	"github.com/smartystreets/messaging/v3"
)

type defaultDispatchEncoder struct {
	contentType  string
	messageTypes map[reflect.Type]string
	serializer   Serializer
}

func newDispatchEncoder(config configuration) DispatchEncoder {
	return defaultDispatchEncoder{
		contentType:  config.Serializer.ContentType(),
		messageTypes: config.MessageTypes,
		serializer:   config.Serializer,
	}
}

func (this defaultDispatchEncoder) Encode(dispatch *messaging.Dispatch) error {
	if len(dispatch.Payload) > 0 || dispatch.Message == nil {
		return nil // already written or nothing to serialize
	}

	instanceType := reflect.TypeOf(dispatch.Message)
	messageType, found := this.messageTypes[reflect.TypeOf(dispatch.Message)]
	if !found {
		return wrapError(fmt.Errorf("%w: [%s]", ErrMessageTypeNotFound, instanceType.Name()))
	}

	raw, err := this.serializer.Serialize(dispatch.Message)
	if err != nil {
		return wrapError(err)
	}

	dispatch.ContentType = this.contentType
	dispatch.MessageType = messageType
	dispatch.Payload = raw
	if dispatch.Topic == "" {
		dispatch.Topic = messageType
	}

	return nil
}
