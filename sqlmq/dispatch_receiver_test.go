package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

func TestDispatchReceiverFixture(t *testing.T) {
	gunit.Run(new(DispatchReceiverFixture), t)
}

type DispatchReceiverFixture struct {
	*gunit.Fixture

	ctx         context.Context
	ctxShutdown context.CancelFunc
	channel     chan messaging.Dispatch
	writer      messaging.CommitWriter

	commitCalls   int
	commitError   error
	rollbackCalls int
	rollbackError error

	execContext   context.Context
	execStatement string
	execArgs      []interface{}
	execResult    sql.Result
	execError     error

	lastInsertID      int64
	rowsAffectedValue int64
}

func (this *DispatchReceiverFixture) Setup() {
	this.ctx, this.ctxShutdown = context.WithCancel(context.Background())
	this.channel = make(chan messaging.Dispatch, 16)
	this.writer = newDispatchReceiver(this, this.channel, this.ctx)
}

func (this *DispatchReceiverFixture) TestWhenWritingDispatches_ReturnNumberOfWrites() {
	written, err := this.writer.Write(nil, []messaging.Dispatch{{}, {}, {}, {}, {}}...)

	this.So(written, should.Equal, 5)
	this.So(err, should.BeNil)
}

func (this *DispatchReceiverFixture) TestWhenCommitting_FlushBufferToStorageThenCommitAndSendBufferToOutputChannel() {
	this.rowsAffectedValue = 3
	this.lastInsertID = 44

	_, _ = this.writer.Write(nil, []messaging.Dispatch{
		{MessageType: "1", Payload: []byte("a")},
		{MessageType: "2", Payload: []byte("b")},
		{MessageType: "3", Payload: []byte("c")},
	}...)

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(this.execContext, should.Equal, this.ctx)
	this.So(this.execStatement, should.Equal, "INSERT INTO Messages (type, payload) VALUES (?,?),(?,?),(?,?);")
	this.So(this.execArgs, should.Resemble, []interface{}{
		"1", []byte("a"),
		"2", []byte("b"),
		"3", []byte("c"),
	})
	this.So(<-this.channel, should.Resemble, messaging.Dispatch{MessageID: 42, MessageType: "1", Payload: []byte("a")})
	this.So(<-this.channel, should.Resemble, messaging.Dispatch{MessageID: 43, MessageType: "2", Payload: []byte("b")})
	this.So(<-this.channel, should.Resemble, messaging.Dispatch{MessageID: 44, MessageType: "3", Payload: []byte("c")})
}
func (this *DispatchReceiverFixture) TestWhenUnderlyingCommitWriteFails_ReturnErrorDoNotCommitOrSendToOutputChannel() {
	this.execError = errors.New("")
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, this.execError)
	this.So(this.commitCalls, should.Equal, 0)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenUnderlyingCommitFails_ReturnErrorAndDoNotSendToOutputChannel() {
	this.commitError = errors.New("")
	this.lastInsertID = 1
	this.rowsAffectedValue = 1
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, this.commitError)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenRowsAffectedDoesNotMatchWrites_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, errRowsAffected)
	this.So(this.commitCalls, should.Equal, 0)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenLastInsertIDCannotBeDetermined_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	this.rowsAffectedValue = 1

	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, errIdentityFailure)
	this.So(this.commitCalls, should.Equal, 0)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenNoDispatchesWritten_CommitShouldStillInvokeUnderlyingCommit() {
	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenTransactionContextCancelled_DoNotBlockWhenWritingToOutputChannel() {
	this.rowsAffectedValue = 1
	this.lastInsertID = 1
	for i := 0; i < cap(this.channel); i++ {
		this.channel <- messaging.Dispatch{}
	}
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})
	this.ctxShutdown()

	err := this.writer.Commit()

	this.So(err, should.Equal, context.Canceled)
	this.So(len(this.channel), should.Equal, cap(this.channel))
}

func (this *DispatchReceiverFixture) TestWhenRollingBack_InvokeUnderlyingTransactionRollback() {
	this.rollbackError = errors.New("")

	err := this.writer.Rollback()

	this.So(err, should.Equal, this.rollbackError)
}
func (this *DispatchReceiverFixture) TestWhenClosing_Nop() {
	err := this.writer.Close()

	this.So(err, should.BeNil)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchReceiverFixture) Commit() error   { this.commitCalls++; return this.commitError }
func (this *DispatchReceiverFixture) Rollback() error { return this.rollbackError }
func (this *DispatchReceiverFixture) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	this.execContext = ctx
	this.execStatement = statement
	this.execArgs = args
	return this, this.execError
}
func (this *DispatchReceiverFixture) LastInsertId() (int64, error) {
	return this.lastInsertID, nil
}
func (this *DispatchReceiverFixture) RowsAffected() (int64, error) {
	return this.rowsAffectedValue, nil
}

func (this *DispatchReceiverFixture) QueryContext(ctx context.Context, statement string, args ...interface{}) (adapter.QueryResult, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) QueryRowContext(ctx context.Context, statement string, args ...interface{}) adapter.RowScanner {
	panic("nop")
}
func (this *DispatchReceiverFixture) TxHandle() *sql.Tx {
	panic("nop")
}
