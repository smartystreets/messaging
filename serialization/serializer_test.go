package serialization

import (
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestSerializerFixture(t *testing.T) {
	gunit.Run(new(SerializerFixture), t)
}

type SerializerFixture struct {
	*gunit.Fixture

	serializer defaultSerializer
}

func (this *SerializerFixture) TestSerializeAndDeserialize() {
	var actual = "Hello, World!"
	var expected = ""

	payload, serializeError := this.serializer.Serialize(actual)
	deserializeError := this.serializer.Deserialize(payload, &expected)

	this.So(serializeError, should.BeNil)
	this.So(deserializeError, should.BeNil)
	this.So(expected, should.Equal, actual)
}

func (this *SerializerFixture) TestWhenSerializationFails_ExpectedErrorReturned() {
	source := func() {}

	payload, err := this.serializer.Serialize(source)

	this.So(payload, should.BeNil)
	this.So(errors.Is(err, ErrSerializeUnsupportedType), should.BeTrue)
}

func (this *SerializerFixture) TestWhenDeserializationFails_ExpectedErrorReturned() {
	var target int

	err := this.serializer.Deserialize([]byte("Hello"), &target)

	this.So(errors.Is(err, ErrDeserializeMalformedPayload), should.BeTrue)
}
