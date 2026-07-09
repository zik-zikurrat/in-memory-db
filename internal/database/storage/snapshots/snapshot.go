package snapshots

import (
	"context"
	"in-memory-key-value-db/internal/config"
	"time"
)

type Snapshot struct {
	changesCnt int
	lastSave   time.Time
	engine     Snapshotable
}

func NewSnapshot(engine Snapshotable) *Snapshot {
	return &Snapshot{
		changesCnt: 0,
		lastSave:   time.Now(),
		engine:     engine,
	}
}

func (s *Snapshot) Fork(ctx context.Context, cfg *config.SnapshotConfig, change <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastSave := time.Now()

	for {
		select {

		case <-ctx.Done():
			return

		case <-change:
			s.changesCnt++

		case <-ticker.C:
			for _, rule := range cfg.Save {
				if s.changesCnt >= rule.Changes &&
					time.Since(lastSave) >= rule.Seconds {
					s.engine.Snapshot()

					s.changesCnt = 0
					lastSave = time.Now()
					break
				}
			}
		}
	}
}
