package inmemory

import (
	"runtime"
	"sync"
)

var partitionCount = runtime.NumCPU()

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

type Partition struct {
	mu sync.RWMutex
	m  map[string]string
}

type Data struct {
	buckets []*Partition
}

func NewPartition(key string, value interface{}) *Partition {
	return &Partition{
		m: make(map[string]string),
	}
}

func NewData() *Data {
	n := runtime.NumCPU()
	d := &Data{buckets: make([]*Partition, n)}
	for i := range n {
		d.buckets[i] = &Partition{m: make(map[string]string)}
	}
	return d
}

type HashBasedPartitionMapEngine struct {
	data *Data
}

func NewHashBasedPartitionMapEngine() *HashBasedPartitionMapEngine {
	return &HashBasedPartitionMapEngine{data: NewData()}
}

func (e *HashBasedPartitionMapEngine) Set(key, value string) bool {
	p := e.data.bucket(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	_, existed := p.m[key]
	p.m[key] = value
	return !existed
}

func (e *HashBasedPartitionMapEngine) Get(key string) (string, bool) {
	p := e.data.bucket(key)
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.m[key]
	return v, ok
}

func (e *HashBasedPartitionMapEngine) Del(data *Data, key string) bool {
	p := data.bucket(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[key]; !ok {
		return false
	}
	delete(p.m, key)
	return true
}
