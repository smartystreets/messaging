package handlers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestTransactionFixture(t *testing.T) {
	gunit.Run(new(TransactionFixture), t)
}

type TransactionFixture struct {
	*gunit.Fixture

	ctx      context.Context
	messages []interface{}
	handler  messaging.Handler
	sqlTx    *sql.Tx

	connectCount   int
	connectContext context.Context
	connectError   error

	commitWriterCount   int
	commitWriterContext context.Context
	commitWriterError   error

	commitCount   int
	commitError   error
	rollbackCount int
	rollbackError error

	closeCount int

	factoryState TransactionState
	factoryCount int
	factoryError error

	handleContext  context.Context
	handleMessages []interface{}
	handleError    error

	monitor              *FakeMonitor
	monitorCommitCount   int
	monitorRollbackCount int
	monitorBeginErrors   []error
	monitorCommitErrors  []error
}

func (this *TransactionFixture) Setup() {
	this.ctx = context.Background()
	this.messages = []interface{}{1, 2, 3}
	this.sqlTx = &sql.Tx{}
	this.monitor = &FakeMonitor{fixture: this}
	this.handler = NewTransactional(this,
		TransactionalOptions.Monitor(this.monitor),
		TransactionalOptions.Factory(this.factory),
	)
}
func (this *TransactionFixture) handle() {
	this.handler.Handle(this.ctx, this.messages...)
}

func (this *TransactionFixture) TestWhenOpeningConnectionFails_ItShouldPanic() {
	this.connectError = errors.New("")

	this.So(this.handle, should.PanicWith, this.connectError)
	this.So(this.connectContext, should.Equal, this.ctx)
	this.So(this.monitorBeginErrors, should.Resemble, []error{this.connectError})
}
func (this *TransactionFixture) TestWhenOpeningCommitWriterFails_ItShouldPanic() {
	this.commitWriterError = errors.New("")

	this.So(this.handle, should.PanicWith, this.commitWriterError)
	this.So(this.monitorBeginErrors, should.Resemble, []error{this.commitWriterError})
	this.So(this.commitWriterContext, should.Resemble, &transactionalContext{
		Context:    this.ctx,
		Connection: this,
		Tx:         this.sqlTx,
	})
	this.So(this.closeCount, should.Equal, 1) // close connection
}

func (this *TransactionFixture) TestWhenInvokingFactoryPanics_ItShouldPanicAndRollBack() {
	this.factoryError = errors.New("")

	this.So(this.handle, should.PanicWith, this.factoryError)
	this.So(this.factoryState, should.Resemble, TransactionState{Tx: this.sqlTx, Writer: this})
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 1)
}
func (this *TransactionFixture) TestWhenInvokingHandlerPanics_ItShouldPanicAndRollback() {
	this.handleError = errors.New("")

	this.So(this.handle, should.PanicWith, this.handleError)
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 1)
	this.So(this.handleContext, should.Equal, this.ctx)
	this.So(this.handleMessages, should.Resemble, this.messages)
}

func (this *TransactionFixture) TestWhenHandlerCompletesSuccessfully_ItShouldCommit() {
	this.handle()

	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 0)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.monitorCommitCount, should.Equal, 1)
	this.So(this.monitorRollbackCount, should.Equal, 0)
}
func (this *TransactionFixture) TestWhenCommitOperationFails_ItShouldPanicAndRollBack() {
	this.commitError = errors.New("")

	this.So(this.handle, should.PanicWith, this.commitError)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.rollbackCount, should.Equal, 1)
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.monitorCommitErrors, should.Resemble, []error{this.commitError})
	this.So(this.monitorCommitCount, should.Equal, 0)
	this.So(this.monitorRollbackCount, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *TransactionFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	return this, this.connectError
}
func (this *TransactionFixture) Close() error {
	this.closeCount++
	return nil
}

func (this *TransactionFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	this.commitWriterCount++
	this.commitWriterContext = ctx
	ctx.(*transactionalContext).Store(this.sqlTx)
	return this, this.commitWriterError
}
func (this *TransactionFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *TransactionFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}

func (this *TransactionFixture) Commit() error {
	this.commitCount++
	return this.commitError
}
func (this *TransactionFixture) Rollback() error {
	this.rollbackCount++
	return this.rollbackError
}
func (this *TransactionFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	panic("nop")
}

func (this *TransactionFixture) factory(state TransactionState) messaging.Handler {
	this.factoryCount++
	this.factoryState = state
	if this.factoryError != nil {
		panic(this.factoryError)
	}
	return this
}
func (this *TransactionFixture) Handle(ctx context.Context, messages ...interface{}) {
	this.handleContext = ctx
	this.handleMessages = messages
	if this.handleError != nil {
		panic(this.handleError)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type FakeMonitor struct{ fixture *TransactionFixture }

func (this *FakeMonitor) BeginFailure(err error) {
	this.fixture.monitorBeginErrors = append(this.fixture.monitorBeginErrors, err)
}
func (this *FakeMonitor) Rollback() { this.fixture.monitorRollbackCount++ }
func (this *FakeMonitor) Commit()   { this.fixture.monitorCommitCount++ }
func (this *FakeMonitor) CommitFailure(err error) {
	this.fixture.monitorCommitErrors = append(this.fixture.monitorCommitErrors, err)
}
