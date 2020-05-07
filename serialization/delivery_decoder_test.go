package serialization

import (
	"errors"
	"reflect"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestDeliveryDecoderFixture(t *testing.T) {
	gunit.Run(new(DeliveryDecoderFixture), t)
}

type DeliveryDecoderFixture struct {
	*gunit.Fixture

	readTypes     map[string]reflect.Type
	deserializers map[string]Deserializer
	delivery      messaging.Delivery

	decoder DeliveryDecoder

	deserializeCalls    int
	deserializePayload  []byte
	deserializeInstance interface{}
	deserializeError    error
}

func (this *DeliveryDecoderFixture) Setup() {
	this.readTypes = map[string]reflect.Type{}
	this.deserializers = map[string]Deserializer{}
	this.newDecoder()
}
func (this *DeliveryDecoderFixture) newDecoder() {
	config := configuration{Deserializers: this.deserializers}
	Options.apply(Options.ReadTypes(this.readTypes))(&config)
	this.decoder = newDeliveryDecoder(config)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DeliveryDecoderFixture) TestWhenMessageTypeNotFound_ReturnError() {
	this.delivery.Payload = []byte("payload")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
	// this.So(errors.Is(err, ErrMessageTypeNotFound), should.BeTrue) // TODO
}

func (this *DeliveryDecoderFixture) TestWhenContentTypeNotFound_ReturnError() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "not-found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
	//this.So(errors.Is(err, ErrUnknownContentType), should.BeTrue) // TODO
}

func (this *DeliveryDecoderFixture) TestWhenDeserializationFails_ReturnError() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this
	this.deserializeError = errors.New("error")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
	this.So(this.deserializeCalls, should.Equal, 1)
	this.So(this.deserializePayload, should.Resemble, this.delivery.Payload)
	//this.So(errors.Is(err, ErrUnknownContentType), should.BeTrue)
}

func (this *DeliveryDecoderFixture) TestWhenDecodingSucceeds_PopulateMessageOnDelivery() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.Equal, 42)
	this.So(this.deserializeCalls, should.Equal, 1)
	this.So(this.deserializePayload, should.Resemble, this.delivery.Payload)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryHasNoBody_SkipDecoding() {
	this.delivery.Payload = nil

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.BeZeroValue)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryAlreadyHasMessage_SkipDecoding() {
	this.delivery.Payload = []byte{0x0}
	this.delivery.Message = "exists"

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.deserializeCalls, should.BeZeroValue)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DeliveryDecoderFixture) ContentType() string { panic("not called") }

func (this *DeliveryDecoderFixture) Deserialize(raw []byte, instance interface{}) error {
	this.deserializeCalls++
	this.deserializePayload = raw
	this.deserializeInstance = instance
	if this.deserializeError != nil {
		return this.deserializeError
	}

	*(instance.(*int)) = 42
	return nil
}
