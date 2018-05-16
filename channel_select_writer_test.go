package messaging

import (
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestChannelSelectWriterFixture(t *testing.T) {
	gunit.Run(new(ChannelSelectWriterFixture), t)
}

type ChannelSelectWriterFixture struct {
	*gunit.Fixture

	writer     *ChannelSelectWriter
	fakeWriter *FakeChannelWriter
}

func (this *ChannelSelectWriterFixture) Setup() {
	this.fakeWriter = &FakeChannelWriter{}
	this.writer = NewChannelSelectWriter(this.fakeWriter, WithChannelCapacity(2))
}

func (this *ChannelSelectWriterFixture) TestWritesReachInnerWriter() {
	go this.writer.Listen()

	err1 := this.writer.Write(Dispatch{Payload: []byte("Hello,")})
	err2 := this.writer.Write(Dispatch{Payload: []byte("World!")})
	time.Sleep(time.Millisecond)

	this.So(err1, should.BeNil)
	this.So(err2, should.BeNil)

	this.So(this.fakeWriter.Writes(), should.Equal, 2)
	this.So(string(this.fakeWriter.written[0].Payload), should.Equal, "Hello,")
	this.So(string(this.fakeWriter.written[1].Payload), should.Equal, "World!")
}

func (this *ChannelSelectWriterFixture) TestEmptyBuffersNeverReachInnerWriter() {
	go this.writer.Listen()

	err := this.writer.Write(Dispatch{})
	time.Sleep(time.Millisecond)

	this.So(err, should.BeNil)
	this.So(this.fakeWriter.Writes(), should.Equal, 0)
}

func (this *ChannelSelectWriterFixture) TestWritesWhichOverflowChannelAreDiscarded() {
	this.writer.Write(Dispatch{Payload: []byte("a")})
	this.writer.Write(Dispatch{Payload: []byte("b")})

	err := this.writer.Write(Dispatch{Payload: []byte("c")})

	this.So(err, should.Equal, WriteDiscardedError)
}

//////////////////////////////////////////////////////////////////////////

type FakeChannelWriter struct {
	written []Dispatch
}

func (this *FakeChannelWriter) Writes() int { return len(this.written) }
func (this *FakeChannelWriter) Write(dispatch Dispatch) error {
	this.written = append(this.written, dispatch)
	return nil
}
func (this *FakeChannelWriter) Close() {}
