package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestDispatchStoreFixture(t *testing.T) {
	gunit.Run(new(DispatchStoreFixture), t)
}

type DispatchStoreFixture struct {
	*gunit.Fixture

	store messageStore

	execCalls     int
	execContext   context.Context
	execStatement string
	execArgs      []interface{}
	execResult    sql.Result
	execError     error

	lastInsertID      int64
	rowsAffectedValue int64
}

func (this *DispatchStoreFixture) Setup() {
	this.store = &dispatchStore{}
}

func (this *DispatchStoreFixture) TestWhenNoDispatchesToWrite_DoNotPerformWriteOperation() {
	err := this.store.Store(this, nil)

	this.So(err, should.BeNil)
	this.So(this.execCalls, should.BeZeroValue)
}
func (this *DispatchStoreFixture) TestWhenStoring_WriteToUnderlyingStoreAndMarkDispatchesWithMessageID() {
	this.rowsAffectedValue = 3
	this.lastInsertID = 44
	writes := []messaging.Dispatch{
		{MessageType: "1", Payload: []byte("a")},
		{MessageType: "2", Payload: []byte("b")},
		{MessageType: "3", Payload: []byte("c")},
	}

	err := this.store.Store(this, writes)

	this.So(err, should.BeNil)

	this.So(this.execContext, should.BeNil)
	this.So(this.execStatement, should.Equal, "INSERT INTO Messages (type, payload) VALUES (?,?),(?,?),(?,?);")
	this.So(this.execArgs, should.Resemble, []interface{}{
		"1", []byte("a"),
		"2", []byte("b"),
		"3", []byte("c"),
	})

	this.So(writes, should.Resemble, []messaging.Dispatch{
		{MessageID: 42, MessageType: "1", Payload: []byte("a")},
		{MessageID: 43, MessageType: "2", Payload: []byte("b")},
		{MessageID: 44, MessageType: "3", Payload: []byte("c")},
	})
}
func (this *DispatchStoreFixture) TestWhenStoreWriteFails_ReturnErrorDoNotCommitOrSendToOutputChannel() {
	this.execError = errors.New("")

	err := this.store.Store(this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, this.execError)
}
func (this *DispatchStoreFixture) TestWhenRowsAffectedDoesNotMatchWrites_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	this.rowsAffectedValue = 0
	this.lastInsertID = 1

	err := this.store.Store(this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, errRowsAffected)
}
func (this *DispatchStoreFixture) TestWhenLastInsertIDCannotBeDetermined_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	this.rowsAffectedValue = 1
	this.lastInsertID = 0

	err := this.store.Store(this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, errIdentityFailure)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchStoreFixture) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	this.execCalls++
	this.execContext = ctx
	this.execStatement = statement
	this.execArgs = args
	return this, this.execError
}
func (this *DispatchStoreFixture) LastInsertId() (int64, error) {
	return this.lastInsertID, nil
}
func (this *DispatchStoreFixture) RowsAffected() (int64, error) {
	return this.rowsAffectedValue, nil
}
