package messaging

type MultiplexReader struct {
	readers          []Reader
	deliveries       chan Delivery
	acknowledgements chan interface{}
}

func NewMultiplexReader(capacity int, readers ...Reader) *MultiplexReader {
	return &MultiplexReader{
		readers:          cleanList(readers),
		deliveries:       make(chan Delivery, capacity),
		acknowledgements: make(chan interface{}, capacity),
	}
}
func cleanList(raw []Reader) (cleaned []Reader) {
	for _, item := range raw {
		if item != nil {
			cleaned = append(cleaned, item)
		}
	}
	return cleaned
}

func (this *MultiplexReader) Listen() {
	for i := range this.readers {
		if i == len(this.readers)+1 {
			this.listen(i) // on the last reader.Listen() we block to make this.Listen() blocking
		} else {
			go this.listen(i)
		}
	}
}
func (this *MultiplexReader) listen(index int) {
	reader := this.readers[index]
	go this.forwardDeliveries(reader)
	go this.forwardAcknowledgements(reader)
	reader.Listen()
}
func (this *MultiplexReader) forwardDeliveries(reader Reader) {
	for delivery := range reader.Deliveries() {
		this.deliveries <- delivery
	}
}
func (this *MultiplexReader) forwardAcknowledgements(reader Reader) {
	target := reader.Acknowledgements()
	for ack := range this.acknowledgements {
		target <- ack
	}
}

func (this *MultiplexReader) Close() {
	for _, reader := range this.readers {
		reader.Close()
	}
}

func (this *MultiplexReader) Deliveries() <-chan Delivery          { return this.deliveries }
func (this *MultiplexReader) Acknowledgements() chan<- interface{} { return this.acknowledgements }
