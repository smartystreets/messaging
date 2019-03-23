package messaging

import (
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestRegistrationDiscoveryFixture(t *testing.T) {
	gunit.Run(new(RegistrationDiscoveryFixture), t)
}

type RegistrationDiscoveryFixture struct {
	*gunit.Fixture

	discovery *RegistrationDiscovery
}

func (this *RegistrationDiscoveryFixture) Setup() {
	this.discovery = NewRegistrationDiscovery(nil)
}

func (this *RegistrationDiscoveryFixture) TestNilInstance() {
	result, err := this.discovery.Discover(nil)

	this.So(result, should.Equal, "")
	this.So(err, should.Equal, MessageTypeDiscoveryError)
}

func (this *RegistrationDiscoveryFixture) TestAnonymousStructs() {
	anonymous := struct{ Value1, Value2, Value3 string }{}
	result, err := this.discovery.Discover(anonymous)
	this.So(result, should.Equal, "")
	this.So(err, should.Equal, MessageTypeDiscoveryError)
}

func (this *RegistrationDiscoveryFixture) TestEmptyStructs() {
	result, err := this.discovery.Discover(struct{}{})
	this.So(result, should.Equal, "")
	this.So(err, should.Equal, MessageTypeDiscoveryError)
}

func (this *RegistrationDiscoveryFixture) TestRegisteredStruct() {
	this.discovery.RegisterInstance(uint64(0), "my-registered-uint64")
	this.discovery.RegisterInstance(0, "simple-int")
	this.discovery.RegisterInstance(false, "A Boolean")
	this.discovery.RegisterInstance(SampleMessage{}, "SampleMessage")
	this.discovery.RegisterInstance(&SampleMessage{}, "Pointer to Sample Message")

	this.assertDiscovery(uint64(17), "my-registered-uint64")
	this.assertDiscovery(42, "simple-int")
	this.assertDiscovery(true, "A Boolean")
	this.assertDiscovery(SampleMessage{}, "SampleMessage")
	this.assertDiscovery(&SampleMessage{}, "Pointer to Sample Message")
}
func (this *RegistrationDiscoveryFixture) assertDiscovery(instance interface{}, expected string) {
	result, err := this.discovery.Discover(instance)
	this.So(result, should.Equal, expected)
	this.So(err, should.BeNil)
}
