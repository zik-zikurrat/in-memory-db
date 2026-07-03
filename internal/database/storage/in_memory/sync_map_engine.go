package inmemory

import "sync"

type SyncMapEngine struct {
	data *sync.Map
}

func NewSyncMapEngine() *SyncMapEngine {
	return &SyncMapEngine{
		data: &sync.Map{},
	}
}

func (e *SyncMapEngine) Set(key, value string) {
	e.data.Store(key, value)
}

func (e *SyncMapEngine) Get(key string) (string, bool) {
	value, ok := e.data.Load(key)
	if !ok {
		return "", false
	}
	return value.(string), true
}

func (e *SyncMapEngine) Del(key string) bool {
	e.data.Delete(key)
	if _, ok := e.Get(key); !ok {
		return true
	}
	return false
}
