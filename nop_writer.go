package messaging

type NopWriter struct{}

func NewNopWriter() NopWriter { return NopWriter{} }

func (this NopWriter) Write(Dispatch) error { return nil }
func (this NopWriter) Commit() error        { return nil }
func (this NopWriter) Close()               {}
