package messaging

type DiscardWriter struct{}

func NewDiscardWriter() DiscardWriter { return DiscardWriter{} }

func (this DiscardWriter) Write(Dispatch) error { return nil }
func (this DiscardWriter) Commit() error        { return nil }
func (this DiscardWriter) Close()               {}
