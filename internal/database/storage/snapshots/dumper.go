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

	for i, partition := range buckets {
		go func(i int, p *inmemory.Partition) {
			defer wg.Done()
			dump[i] = p.Snapshot()
		}(i, partition)
	}
	wg.Wait()

	if err := d.writeDump(dump); err != nil {
		return err
	}

	return nil
}

func (d *HashBasedPartitionDumper) writeDump(dump []map[string]string) error {
	tmpPath := filepath.Join(d.dumpDir, d.dumpFile+".tmp")
	finalPath := filepath.Join(d.dumpDir, d.dumpFile)

	data, err := json.Marshal(dump)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, finalPath)
}

func (d *HashBasedPartitionDumper) Load() error {
	panic("NOT IMPLEMENTED")
}
