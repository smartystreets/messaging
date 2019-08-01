package messaging

type SerializationWriter struct {
	writer          Writer
	commitWriter    CommitWriter
	serializer      Serializer
	contentType     string
	contentEncoding string
}

func NewSerializationWriter(inner Writer, serializer Serializer) *SerializationWriter {
	commitWriter, _ := inner.(CommitWriter)
	return &SerializationWriter{
		writer:          inner,
		commitWriter:    commitWriter,
		serializer:      serializer,
		contentType:     serializer.ContentType(),
		contentEncoding: serializer.ContentEncoding(),
	}
}

func (this *SerializationWriter) Write(dispatch Dispatch) error {
	if len(dispatch.Payload) > 0 {
		return this.writer.Write(dispatch) // already have a payload a message type, forward to inner
	} else if dispatch.Message == nil {
		return ErrEmptyDispatch // no payload and no message, this is a total fail
	}

	payload, err := this.serializer.Serialize(dispatch.Message)
	if err != nil {
		return err // serialization failed
	}

	dispatch.ContentType = this.contentType
	dispatch.ContentEncoding = this.contentEncoding
	dispatch.Payload = payload
	return this.writer.Write(dispatch)
}

func (this *SerializationWriter) Commit() error {
	if this.commitWriter == nil {
		return nil
	}

	return this.commitWriter.Commit()
}

func (this *SerializationWriter) Close() {
	this.writer.Close()
}
