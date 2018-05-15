package tcp

type Mutex interface {
	Lock()
	Unlock()
}

type nopMutex struct{}

func (this nopMutex) Lock()   {}
func (this nopMutex) Unlock() {}
