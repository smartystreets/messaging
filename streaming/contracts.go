package streaming

type logger interface {
	Printf(format string, args ...interface{})
}
