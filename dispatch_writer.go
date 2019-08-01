package messaging

type DispatchWriter struct {
	writer    Writer
	committer CommitWriter
	discovery TypeDiscovery
}

func NewDispatchWriter(writer Writer, discovery TypeDiscovery) *DispatchWriter {
	committer, _ := writer.(CommitWriter)

	return &DispatchWriter{
		writer:    writer,
		committer: committer,
		discovery: discovery,
	}
}

func (this *DispatchWriter) Write(item Dispatch) error {
	messageType, destination, err := this.discovery.Discover(item.Message)
	if err != nil {
		return err
	}

	item.MessageType = messageType
	item.Destination = destination
	return this.writer.Write(item)
}

func (this *DispatchWriter) Commit() error {
	if this.committer == nil {
		return nil
	}

	return this.committer.Commit()
}

func (this *DispatchWriter) Close() {
	this.writer.Close()
}
