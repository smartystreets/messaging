package handlers

import (
	"errors"

	"github.com/smartystreets/messaging/v3"
)

func NewRetry(inner messaging.Handler, options ...retryOption) messaging.Handler {
	this := defaultRetry{Handler: inner}

	for _, option := range RetryOptions.defaults(options...) {
		option(&this)
	}

	return this
}

type RetryMonitor interface {
	Attempt(attempt int, resultError interface{})
}

var ErrMaxRetriesExceeded = errors.New("maximum number of retry attempts exceeded")
