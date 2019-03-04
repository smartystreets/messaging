package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/smartystreets/messaging"
)

type Writer struct {
	producer sarama.SyncProducer
}

func NewWriter(client sarama.Client) (*Writer, error) {
	if producer, err := sarama.NewSyncProducerFromClient(client); err != nil {
		return nil, err
	} else {
		return &Writer{producer: producer}, nil
	}
}

func (this *Writer) Write(dispatch messaging.Dispatch) error {
	message := &sarama.ProducerMessage{
		Topic: dispatch.Destination,
		Value: sarama.ByteEncoder(dispatch.Payload),
	}

	return this.write(message)
}
func (this *Writer) write(message *sarama.ProducerMessage) error {
	_, _, err := this.producer.SendMessage(message)
	return err
}

func (this *Writer) Close() {
}
