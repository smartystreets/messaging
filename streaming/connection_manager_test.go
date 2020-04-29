package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestConnectionManagerFixture(t *testing.T) {
	gunit.Run(new(ConnectionManagerFixture), t)
}

type ConnectionManagerFixture struct {
	*gunit.Fixture

	ctx  context.Context
	repo connectionManager

	connectCount   int
	connectContext context.Context
	connectError   error
	opened         []*fakeConnection
}

func (this *ConnectionManagerFixture) Setup() {
	this.ctx = context.Background()
	this.repo = newConnectionManager(this)
}

func (this *ConnectionManagerFixture) TestWhenConnectFails_ItShouldReturnError() {
	this.connectError = errors.New("")

	connection, err := this.repo.Current(this.ctx)

	this.So(connection, should.BeNil)
	this.So(err, should.Equal, this.connectError)
	this.So(this.connectContext, should.Equal, this.ctx)
}
func (this *ConnectionManagerFixture) TestWhenConnecting_ReturnUnderlyingConnection() {
	connection, err := this.repo.Current(this.ctx)

	this.So(connection, should.Equal, this.opened[0])
	this.So(err, should.BeNil)
	this.So(this.connectContext, should.Equal, this.ctx)
}
func (this *ConnectionManagerFixture) TestWhenOpeningAConnectionTwice_ItShouldConnectOnceAndGiveSameConnectionInstance() {
	first, _ := this.repo.Current(this.ctx)

	second, err := this.repo.Current(this.ctx)

	this.So(first, should.Equal, second)
	this.So(err, should.BeNil)
	this.So(this.connectCount, should.Equal, 1)
}

func (this *ConnectionManagerFixture) TestWhenReleasingAConnection_ItShouldCloseTheReleasedConnection() {
	connection, _ := this.repo.Current(this.ctx)

	this.repo.Release(connection)

	this.So(this.opened[0].closeCount, should.Equal, 1)
}
func (this *ConnectionManagerFixture) TestWhenReleasingANilConnection_Nop() {
	this.So(func() { this.repo.Release(nil) }, should.NotPanic)
}
func (this *ConnectionManagerFixture) TestWhenReleasingCurrentConnection_ItShouldCauseCurrentToReturnNewConnection() {
	first, _ := this.repo.Current(this.ctx)
	this.repo.Release(first)

	second, _ := this.repo.Current(this.ctx)

	this.So(second, should.NotBeNil)
	this.So(first, should.NotEqual, second)
	this.So(this.connectCount, should.Equal, 2)
}
func (this *ConnectionManagerFixture) TestWhenReleasingPriorConnection_ItShouldStillGiveBackCurrentConnection() {
	first, _ := this.repo.Current(this.ctx)
	this.repo.Release(first)
	secondA, _ := this.repo.Current(this.ctx)
	this.repo.Release(first) // release first again

	secondB, _ := this.repo.Current(this.ctx)

	this.So(secondA, should.Equal, secondB)
	this.So(secondB, should.NotBeNil)
	this.So(this.connectCount, should.Equal, 2)
}

func (this *ConnectionManagerFixture) TestWhenClosing_ReleaseCurrentConnectionIfAny() {
	_, _ = this.repo.Current(this.ctx)

	_ = this.repo.Close()

	this.So(this.opened[0].closeCount, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionManagerFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	if this.connectError != nil {
		return nil, this.connectError
	}

	connection := &fakeConnection{}
	this.opened = append(this.opened, connection)
	return connection, nil
}
func (this *ConnectionManagerFixture) Close() error {
	panic("nop")
}

type fakeConnection struct {
	closeCount int
}

func (this *fakeConnection) Close() error {
	this.closeCount++
	return nil
}
func (this *fakeConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *fakeConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *fakeConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}
