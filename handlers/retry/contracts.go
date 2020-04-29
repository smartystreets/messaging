package retry

import (
	"errors"

	"github.com/smartystreets/messaging/v3"
)

func New(inner messaging.Handler, options ...option) messaging.Handler {
	this := handler{Handler: inner}

	for _, option := range Options.defaults(options...) {
		option(&this)
	}

	return this
}

type Monitor interface {
	Attempt(attempt int, resultError interface{})
}

var ErrMaxRetriesExceeded = errors.New("maximum number of retry attempts exceeded")
