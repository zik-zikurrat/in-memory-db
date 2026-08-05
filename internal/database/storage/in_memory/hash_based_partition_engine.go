package inmemory

import (
	"context"
	"in-memory-key-value-db/internal/config"
	"in-memory-key-value-db/internal/database/storage/wal"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	_defaultRetry = 5
)

func murmur2(data []byte) int32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	length := len(data)
	h := seed ^ uint32(length)

	for i := 0; i < length/4; i++ {
		i4 := i * 4
		k := uint32(data[i4]) |
			uint32(data[i4+1])<<8 |
			uint32(data[i4+2])<<16 |
			uint32(data[i4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= uint32(data[(length & ^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length & ^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length & ^3])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return int32(h)
}

func partition(key []byte, numPartitions int) int {
	return int((murmur2(key) & 0x7fffffff) % int32(numPartitions))
}

func (d *Data) bucket(key string) *Partition {
	return d.buckets[partition([]byte(key), len(d.buckets))]
}

type Cache struct {
	limit uint64
	used  uint64
}

func NewCache(cfg *config.CacheConfig) *Cache {
	return &Cache{
		limit: cfg.Limit,
	}
}

type Elem struct {
	key   string
	value string
	size  uint64
	next  *Elem
	prev  *Elem
}

type Partition struct {
	log       *zap.Logger
	walEvents chan wal.WALEvent
	mu        sync.RWMutex
	m         map[string]*Elem
	head      *Elem
	tail      *Elem
	used      uint64
	limit     uint64
}

type Data struct {
	buckets []*Partition
}

func NewElem(key, value string) *Elem {
	return &Elem{
		key:   key,
		value: value,
		size:  uint64(len(key) + len(value)),
	}
}

func NewData(ctx context.Context, cache *Cache, walEvents chan wal.WALEvent, log *zap.Logger) *Data {
	n := runtime.NumCPU()

	limitPerPartition := cache.limit / uint64(n)

	d := &Data{buckets: make([]*Partition, n)}

	for i := range n {
		p := &Partition{
			walEvents: walEvents,
			m:         make(map[string]*Elem),
			limit:     limitPerPartition,
			log:       log,
		}
		d.buckets[i] = p

		go func(part *Partition) {
			part.checkPartitionLimit(ctx)
		}(p)
	}
	return d
}

type HashBasedPartitionMapEngine struct {
	data *Data
}

func NewHashBasedPartitionMapEngine(ctx context.Context, cache *Cache, walEvents chan wal.WALEvent, log *zap.Logger) *HashBasedPartitionMapEngine {
	return &HashBasedPartitionMapEngine{data: NewData(ctx, cache, walEvents, log)}
}

func (e *HashBasedPartitionMapEngine) Set(key, value string) {
	p := e.data.bucket(key)
	p.mu.Lock()
	defer p.mu.Unlock()

	elem, ok := p.m[key]
	if ok {
		newSize := uint64(len(key) + len(value))
		oldSize := elem.size
		elem.value = value
		elem.size = newSize

		p.used = p.used - oldSize + newSize
		p.promote(elem)
		return
	}

	elem = NewElem(key, value)
	p.m[key] = elem
	p.used += elem.size
	p.append(elem)
}

func (e *HashBasedPartitionMapEngine) Get(key string) (string, bool) {
	p := e.data.bucket(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	elem, ok := p.m[key]
	if !ok {
		return "", false
	}

	p.promote(elem)
	return elem.value, true
}

func (e *HashBasedPartitionMapEngine) Del(key string) bool {
	p := e.data.bucket(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	elem, ok := p.m[key]
	if !ok {
		return false
	}

	delete(p.m, key)
	p.used -= elem.size
	p.detach(elem)
	return true
}

func (e *HashBasedPartitionMapEngine) GetBuckets() []*Partition {
	return e.data.buckets
}

func (p *Partition) Snapshot() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snap := make(map[string]string, len(p.m))
	for k, v := range p.m {
		snap[k] = v.value
	}
	return snap
}

// LRU METHODS
func (p *Partition) promote(elem *Elem) {
	if elem == p.tail {
		return
	}

	p.detach(elem)
	p.append(elem)
}

func (p *Partition) append(elem *Elem) {
	elem.next = nil
	elem.prev = p.tail

	if p.tail != nil {
		p.tail.next = elem
	} else {
		p.head = elem
	}

	p.tail = elem
}

func (p *Partition) detach(elem *Elem) {
	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		p.head = elem.next
	}

	if elem.next != nil {
		elem.next.prev = elem.prev
	} else {
		p.tail = elem.prev
	}
	elem.prev = nil
	elem.next = nil
}

func (p *Partition) checkPartitionLimit(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.evictIfNeeded()
		case <-ctx.Done():
			p.log.Info("context done")
			return
		}
	}
}

func (p *Partition) evictIfNeeded() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.used > p.limit && p.head != nil {
		if err := p.deleteLeastActive(); err != nil {
			cnt := 0
			for cnt <= _defaultRetry {
				if err := p.deleteLeastActive(); err != nil {
					cnt++
				} else {
					break
				}
			}
			continue
		}
	}
}

func (p *Partition) deleteLeastActive() error {
	victim := p.head
	if victim == nil {
		return nil
	}

	done := make(chan error, 1)
	p.walEvents <- wal.WALEvent{
		Command:   "DEL",
		Arguments: []string{victim.key, victim.value},
		Done:      done,
	}
	if err := <-done; err != nil {
		p.log.Error("wal write tombstone failed", zap.Error(err))
		return err
	}

	delete(p.m, victim.key)

	p.used -= victim.size

	p.head = victim.next
	if p.head != nil {
		p.head.prev = nil
	} else {
		p.tail = nil
	}

	return nil
}
