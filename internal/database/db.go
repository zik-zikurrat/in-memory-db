package database

import (
	"context"
	"in-memory-key-value-db/internal/database/compute"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"in-memory-key-value-db/internal/database/storage/snapshots"
	"in-memory-key-value-db/internal/database/storage/wal"
	"sync"

	"go.uber.org/zap"
)

type DB struct {
	log      *zap.Logger
	engine   *inmemory.HashBasedPartitionMapEngine
	wal      *wal.WAL
	snapshot *snapshots.Snapshot
	compute  *compute.Compute

	cancel context.CancelFunc
	wg     sync.WaitGroup
}
