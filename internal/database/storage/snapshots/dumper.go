package snapshots

import (
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
)

type Snapshotable interface {
	Snapshot() []map[string]string
	Dump([]map[string]string) error
	Load(data map[string]string)
}

type HashBasedPartitionDumper struct {
	engine  *inmemory.HashBasedPartitionMapEngine
	buckets []map[string]string
}

func NewHashBasedPartitionDumper(engine *inmemory.HashBasedPartitionMapEngine) *HashBasedPartitionDumper {
	return &HashBasedPartitionDumper{
		engine:  engine,
		buckets: make([]map[string]string, 0, engine.),
	}
}

func (d *HashBasedPartitionDumper) Snapshot() []map[string]string {

}
