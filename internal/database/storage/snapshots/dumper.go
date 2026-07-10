package snapshots

import (
	"encoding/json"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"os"
	"path/filepath"
	"sync"
)

type Snapshotable interface {
	Dump() error
	Load() error
}

type HashBasedPartitionDumper struct {
	mu       sync.Mutex
	engine   *inmemory.HashBasedPartitionMapEngine
	dumpDir  string
	dumpFile string
}

func NewHashBasedPartitionDumper(dumpDir, dumpFile string, engine *inmemory.HashBasedPartitionMapEngine) *HashBasedPartitionDumper {
	return &HashBasedPartitionDumper{
		engine:   engine,
		dumpDir:  dumpDir,
		dumpFile: dumpFile,
	}
}

func (d *HashBasedPartitionDumper) Dump() error {
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

	if err := d.writeDump(dump); err != nil {
		return err
	}

	return nil
}

func (d *HashBasedPartitionDumper) writeDump(dump []map[string]string) error {
	// create or open exists file
	f, err := os.OpenFile(filepath.Join(d.dumpDir, d.dumpFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	// convert dump to byte slice
	byteDump, err := json.Marshal(dump)
	if err != nil {
		return err
	}

	// write dump
	_, err = f.Write(byteDump)
	if err != nil {
		return err
	}

	return nil
}

func (d *HashBasedPartitionDumper) Load() error {
	panic("NOT IMPLEMENTED")
}
