package streaming

type ShutdownStrategy int

const (
	ShutdownStrategyImmediate ShutdownStrategy = iota
	ShutdownStrategyCurrentBatch
	ShutdownStrategyDrain
)

func (this ShutdownStrategy) String() string {
	switch this {
	case ShutdownStrategyImmediate:
		return "immediate"
	case ShutdownStrategyCurrentBatch:
		return "current-batch"
	case ShutdownStrategyDrain:
		return "drain"
	default:
		return "unknown"
	}
}
