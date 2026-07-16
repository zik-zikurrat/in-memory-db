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
	dump := make([]map[string]string, len(buckets))
	wg := sync.WaitGroup{}
	wg.Add(len(buckets))

	for index, partition := range buckets {
		go func(
			index int,
			partition *inmemory.Partition,
		) {
			defer wg.Done()
			dump[index] = partition.Snapshot()
		}(index, partition)
	}
	wg.Wait()

	return d.writeDump(dump)
}

func (d *HashBasedPartitionDumper) writeDump(
	dump []map[string]string,
) error {
	if err := os.MkdirAll(d.dumpDir, 0o755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(
		d.dumpDir,
		"."+d.dumpFile+".*.tmp",
	)
	if err != nil {
		return err
	}

	tmpPath := tmpFile.Name()
	finalPath := filepath.Join(d.dumpDir, d.dumpFile)

	committed := false

	defer func() {
		_ = tmpFile.Close()

		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	encoder := json.NewEncoder(tmpFile)

	if err := encoder.Encode(dump); err != nil {
		return err
	}

	if err := tmpFile.Sync(); err != nil {
		return err
	}

	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	committed = true

	return nil
}

func (d *HashBasedPartitionDumper) Load() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dumpPath := filepath.Join(d.dumpDir, d.dumpFile)

	data, err := os.ReadFile(dumpPath)
	if err != nil {
		return err
	}

	var restore []map[string]string

	if err := json.Unmarshal(data, &restore); err != nil {
		return err
	}

	for _, partitionDump := range restore {
		for key, value := range partitionDump {
			d.engine.Set(key, value)
		}
	}

	return nil
}
