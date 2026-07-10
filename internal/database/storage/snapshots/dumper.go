package snapshots

import (
	"fmt"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"sync"
)

type Snapshotable interface {
	Dump() error
	Load() error
}

type HashBasedPartitionDumper struct {
	mu       sync.Mutex
	engine   *inmemory.HashBasedPartitionMapEngine
	dumpFile string
}

func NewHashBasedPartitionDumper(engine *inmemory.HashBasedPartitionMapEngine) *HashBasedPartitionDumper {
	return &HashBasedPartitionDumper{
		engine: engine,
	}
}

func (d *HashBasedPartitionDumper) Dump() error {
	fmt.Printf("START DUMP EEEEE")
	buckets := d.engine.GetBuckets()
	workers := len(buckets)
	dump := make([]map[string]string, 0, len(buckets))
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for _, partition := range buckets {
		go func(partition *inmemory.Partition) {
			defer wg.Done()
			d.mu.Lock()
			defer d.mu.Unlock()
			dump = append(dump, partition.Snapshot())
		}(partition)
	}
	wg.Wait()
	return nil
}

func (d *HashBasedPartitionDumper) Load() error {
	panic("NOT IMPLEMENTED")
}
