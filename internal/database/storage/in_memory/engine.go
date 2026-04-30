package inmemory

import "sync"

type Engine struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewEngine() *Engine {
	return &Engine{
		data: make(map[string]string),
	}
}

func (e *Engine) Set(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, ok := e.data[key]
	return value, ok
}

func (e *Engine) Del(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.data[key]; !ok {
		return false
	}
	delete(e.data, key)
	return true
}
