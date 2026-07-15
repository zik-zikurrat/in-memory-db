package cache

import (
	"context"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Elem struct {
	key   string
	value string
	size  uint64
	next  *Elem
	prev  *Elem
}

type LRU struct {
	m      sync.RWMutex
	head   *Elem
	tail   *Elem
	cache  *Cache
	kv     map[string]*Elem
	engine *inmemory.HashBasedPartitionMapEngine
}

func NewElem(key, value string) *Elem {
	return &Elem{
		key:   key,
		value: value,
	}
}

func NewLRU(cache *Cache) *LRU {
	return &LRU{
		cache: cache,
		kv:    make(map[string]*Elem),
	}
}

func (lru *LRU) Put(key, value string) {
	elem, ok := lru.kv[key]
	if ok {
		elem.value = value
		old_size := len(elem.value)
		new_size := len(value)
		elem.size += (uint64(new_size) - uint64(old_size))
		lru.cache.used += elem.size
		lru.promote(elem)
		return
	}
	elem = NewElem(key, value)
	lru.kv[key] = elem
	elem.size += uint64((len(key) + len(value)))
	lru.cache.used += elem.size
	lru.append(elem)
}

func (lru *LRU) Get(key string) (string, bool) {
	elem, ok := lru.kv[key]
	if !ok {
		return "", false
	}

	lru.promote(elem)

	return elem.value, true
}

func (lru *LRU) promote(elem *Elem) {
	if elem == lru.tail {
		return
	}

	lru.detach(elem)
	lru.append(elem)
}

func (lru *LRU) append(elem *Elem) {
	elem.next = nil
	elem.prev = lru.tail

	if lru.tail != nil {
		lru.tail.next = elem
	} else {
		lru.head = elem
	}

	lru.tail = elem
}

func (lru *LRU) detach(elem *Elem) {
	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		lru.head = elem.next
	}

	if elem.next != nil {
		elem.next.prev = elem.prev
	} else {
		lru.tail = elem.prev
	}
	elem.prev = nil
	elem.next = nil
}

func (lru *LRU) CheckCacheLimit(ctx context.Context, l *zap.Logger) {
	ticker := time.NewTicker(time.Second * 45)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for lru.cache.used >= lru.cache.limit {
				lru.deleteLeastActive()
			}
		case <-ctx.Done():
			l.Info("context done")
			return
		}
	}
}

func (lru *LRU) deleteLeastActive() {
	if lru.head != nil {
		lru.engine.Del(lru.head.key)
		lru.m.Lock()
		lru.cache.used -= lru.head.size
		lru.head = lru.head.next
		lru.head.prev = nil
		lru.m.Unlock()
	}
}
