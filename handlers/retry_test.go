package handlers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestRetryFixture(t *testing.T) {
	gunit.Run(new(RetryFixture), t)
}

type RetryFixture struct {
	*gunit.Fixture

	handler messaging.Handler

	expectedContext       context.Context
	shutdown              context.CancelFunc
	expectedMessages      []interface{}
	receivedContext       context.Context
	receivedMessages      []interface{}
	handleError           error
	handleCalls           int
	noErrorAfterAttempt   int
	shutdownAfterAttempt  int
	loggedMessages        []string
	monitoredAttemptCount int
	monitoredErrors       []interface{}
}

func (this *RetryFixture) Setup() {
	this.expectedContext = context.WithValue(context.Background(), reflect.TypeOf(this), this)
	this.expectedContext, this.shutdown = context.WithCancel(this.expectedContext)
	this.expectedMessages = []interface{}{1, 2, 3}
	this.handler = NewRetry(this,
		RetryOptions.Logger(this),
		RetryOptions.Monitor(this),
	)
}
func (this *RetryFixture) Teardown() {
	this.shutdown()
}

func (this *RetryFixture) handle() {
	this.handler.Handle(this.expectedContext, this.expectedMessages...)
}
func (this *RetryFixture) assertCallToInnerHandler() {
	this.So(this.receivedMessages, should.Resemble, this.expectedMessages)
	this.So(this.receivedContext.Value(reflect.TypeOf(this)), should.Equal, this)
}

func (this *RetryFixture) TestInnerHandlerCalledProperly() {
	this.handle()

	this.assertCallToInnerHandler()
}
func (this *RetryFixture) TestCancelledContextDoesNotCallInnerHandler() {
	this.shutdown()

	this.handle()

	this.So(this.receivedContext, should.BeNil)
	this.So(this.receivedMessages, should.BeNil)
}
func (this *RetryFixture) TestSleepBetweenRetries() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 2
	this.handler = NewRetry(this,
		RetryOptions.MaxAttempts(1),
		RetryOptions.Timeout(time.Millisecond),
		RetryOptions.Monitor(this),
		RetryOptions.Logger(this),
	)

	start := time.Now().UTC()
	this.handle()

	this.So(start, should.HappenWithin, time.Millisecond*5, time.Now().UTC())
	this.So(this.monitoredAttemptCount, should.Equal, 1)
	this.So(this.monitoredErrors, should.Resemble, []interface{}{this.handleError, nil})
	this.So(this.loggedMessages[0], should.ContainSubstring, "debug.Stack")
}
func (this *RetryFixture) TestPanicOnTooManyFailedAttempts() {
	this.handleError = errors.New("failed")
	this.handler = NewRetry(this,
		RetryOptions.MaxAttempts(1),
		RetryOptions.Timeout(time.Millisecond),
		RetryOptions.Monitor(this),
		RetryOptions.Logger(this),
	)

	this.So(this.handle, should.PanicWith, ErrMaxRetriesExceeded)

	this.So(this.handleCalls, should.Equal, 2)
	this.So(this.monitoredAttemptCount, should.Equal, 1)
	this.So(this.monitoredErrors, should.Resemble, []interface{}{this.handleError, this.handleError})
}
func (this *RetryFixture) TestNoMoreRetriesOnCancelledContext() {
	this.handleError = errors.New("failed")
	this.shutdownAfterAttempt = 1

	this.So(this.handle, should.NotPanic)

	this.So(this.handleCalls, should.Equal, 1)
}

func (this *RetryFixture) TestDontLogStackTrace() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 2
	this.handler = NewRetry(this,
		RetryOptions.Timeout(time.Millisecond),
		RetryOptions.LogStackTrace(false),
		RetryOptions.Logger(this),
	)

	this.So(this.handle, should.NotPanic)

	this.So(this.loggedMessages[0], should.NotContainSubstring, "debug.Stack")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *RetryFixture) Handle(ctx context.Context, messages ...interface{}) {
	this.handleCalls++
	this.receivedContext = ctx
	this.receivedMessages = append(this.receivedMessages, messages...)

	if this.shutdownAfterAttempt > 0 && this.handleCalls >= this.shutdownAfterAttempt {
		this.shutdown()
	}

	if this.noErrorAfterAttempt > 0 && this.handleCalls >= this.noErrorAfterAttempt {
		return
	}

	if this.handleError != nil {
		panic(this.handleError)
	}
}

func (this *RetryFixture) Println(args ...interface{}) {
	this.loggedMessages = append(this.loggedMessages, fmt.Sprintln(args...))
}
func (this *RetryFixture) Printf(format string, args ...interface{}) {
	this.loggedMessages = append(this.loggedMessages, fmt.Sprintf(format, args...))
}

func (this *RetryFixture) Attempt(attempt int, err interface{}) {
	this.monitoredAttemptCount = attempt
	this.monitoredErrors = append(this.monitoredErrors, err)
}
