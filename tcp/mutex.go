package tcp

type nopMutex struct{}

func (this nopMutex) Lock()   {}
func (this nopMutex) Unlock() {}
