package snapshots

import (
	"sync/atomic"
	"time"
)

type Snapshot struct {
	changesCnt atomic.Int64
	lastSave   time.Time
	engine     Snapshotable
}

func NewSnapshot(engine Snapshotable) *Snapshot {
	return &Snapshot{
		changesCnt: atomic.Int64{},
		lastSave:   time.Now(),
		engine:     engine,
	}
}
