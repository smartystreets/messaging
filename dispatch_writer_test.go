package messaging

import (
	"errors"
	"reflect"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestDispatchWriterFixture(t *testing.T) {
	gunit.Run(new(DispatchWriterFixture), t)
}

type DispatchWriterFixture struct {
	*gunit.Fixture

	inner  *FakeDispatchWriter
	writer *DispatchWriter
}

func (this *DispatchWriterFixture) Setup() {
	this.inner = &FakeDispatchWriter{}
	this.writer = NewDispatchWriter(this.inner, this)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestCloseInvokesInnerWriterClose() {
	this.writer.Close()

	this.So(this.inner.closed, should.Equal, 1)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestWriteUsingDefaults() {
	_ = this.writer.Write(Dispatch{Message: "Hello, World!", Partition: "123"})

	this.So(this.inner.written, should.Resemble, []Dispatch{{
		Destination: "prefix-string",
		MessageType: "prefix.string",
		Partition:   "123",
		Durable:     false,
		Message:     "Hello, World!",
	}})
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestTypeDiscoverErrorsAreReturned() {
	err := this.writer.Write(Dispatch{Message: nil})
	this.So(err, should.Equal, MessageTypeDiscoveryError)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestWriteReturnsInnerError() {
	this.inner.writeError = errors.New("returned to caller")

	err := this.writer.Write(Dispatch{Message: 0})

	this.So(err, should.Equal, this.inner.writeError)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestWriterWithoutCommitShouldReturnNoErrors() {
	this.writer = NewDispatchWriter(nil, nil)

	err := this.writer.Commit()

	this.So(err, should.BeNil)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) TestCommitCallsInnerCommit() {
	this.inner.commitError = errors.New("returned to caller")

	err := this.writer.Commit()

	this.So(err, should.Equal, this.inner.commitError)
	this.So(this.inner.committed, should.Equal, 1)
}

// /////////////////////////////////////////////////////////////////////////////

func (this *DispatchWriterFixture) Discover(message interface{}) (typeName string, destination string, err error) {
	if message == nil {
		return "", "", MessageTypeDiscoveryError
	}
	return "prefix." + reflect.TypeOf(message).Name(), "prefix-" + reflect.TypeOf(message).Name(), nil
}

type FakeDispatchWriter struct {
	writeError  error
	commitError error

	written   []Dispatch
	committed int
	closed    int
}

func (this *FakeDispatchWriter) Write(dispatch Dispatch) error {
	this.written = append(this.written, dispatch)
	return this.writeError
}

func (this *FakeDispatchWriter) Commit() error {
	this.committed++
	return this.commitError
}

func (this *FakeDispatchWriter) Close() {
	this.closed++
}
