package inmemory

import "sync"

type SingleMapEngine struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewSingleMapEngine() *SingleMapEngine {
	return &SingleMapEngine{
		data: make(map[string]string),
	}
}

func (e *SingleMapEngine) Set(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value
}

func (e *SingleMapEngine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, ok := e.data[key]
	return value, ok
}

func (e *SingleMapEngine) Del(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.data[key]; !ok {
		return false
	}
	delete(e.data, key)
	return true
}
