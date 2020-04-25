package streaming

import (
	"github.com/smartystreets/messaging/v3"
)

type Subscription struct {
	Name     string
	Queue    string
	Topics   []string
	Handlers []messaging.Handler
}
