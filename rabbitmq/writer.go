package rabbitmq

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
	"github.com/streadway/amqp"
)

type defaultWriter struct {
	inner                adapter.Channel
	panicOnTopologyError bool
	now                  func() time.Time
}

func newWriter(inner adapter.Channel, config configuration) messaging.CommitWriter {
	return defaultWriter{inner: inner, panicOnTopologyError: config.TopologyFailurePanic, now: config.Now}
}

func (this defaultWriter) Write(_ context.Context, messages ...messaging.Dispatch) (count int, err error) {
	now := this.now().UTC()

	for _, message := range messages {
		count++
		converted := toAMQPDispatch(message, now)
		partition := strconv.FormatUint(message.Partition, 10)
		if err := this.inner.Publish(message.Topic, partition, converted); err != nil {
			return count - 1, err // writes are async, only channel unavailability causes errors here
		}
	}

	return count, nil
}
func toAMQPDispatch(dispatch messaging.Dispatch, now time.Time) amqp.Publishing {
	if dispatch.Timestamp.IsZero() {
		dispatch.Timestamp = now
	}

	return amqp.Publishing{
		AppId:           strconv.FormatUint(dispatch.SourceID, 10),
		MessageId:       strconv.FormatUint(dispatch.MessageID, 10),
		CorrelationId:   strconv.FormatUint(dispatch.CorrelationID, 10),
		Type:            dispatch.MessageType,
		ContentType:     dispatch.ContentType,
		ContentEncoding: dispatch.ContentEncoding,
		Timestamp:       dispatch.Timestamp,
		Expiration:      computeExpiration(dispatch.Expiration),
		DeliveryMode:    computePersistence(dispatch.Durable),
		Body:            dispatch.Payload,
	}
}
func computeExpiration(expiration time.Duration) string {
	if expiration == 0 {
		return ""
	} else if seconds := int(expiration.Seconds()); seconds <= 0 {
		return "1"
	} else {
		return strconv.FormatUint(uint64(seconds), 10)
	}
}
func computePersistence(durable bool) uint8 {
	if durable {
		return amqp.Persistent
	}

	return amqp.Transient
}

func (this defaultWriter) Commit() error {
	return this.tryPanic(this.inner.TxCommit())
}
func (this defaultWriter) Rollback() error {
	return this.inner.TxRollback()
}
func (this defaultWriter) tryPanic(err error) error {
	if err == nil || !this.panicOnTopologyError {
		return err
	}

	if brokerError, ok := err.(*amqp.Error); ok && brokerError.Code == http.StatusNotFound {
		panic(err)
	}

	return err
}

func (this defaultWriter) Close() error {
	return this.inner.Close()
}
