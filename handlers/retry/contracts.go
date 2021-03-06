package retry

import "errors"

type monitor interface {
	HandleAttempted(attempt int, resultError interface{})
}
type logger interface {
	Printf(format string, args ...interface{})
}

var ErrMaxRetriesExceeded = errors.New("maximum number of retry attempts exceeded")
