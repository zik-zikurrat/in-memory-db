package snapshots

import (
	"context"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"time"

	"go.uber.org/zap"
)

type Snapshot struct {
	changesCnt int
	lastSave   time.Time
	engine     Snapshotable
	logger     *zap.Logger
}

func NewSnapshot(engine Snapshotable) *Snapshot {
	return &Snapshot{
		changesCnt: 0,
		lastSave:   time.Now(),
		engine:     engine,
	}
}

func (s *Snapshot) Fork(ctx context.Context, cfg *config.Config, change <-chan struct{}) {
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
			for _, rule := range cfg.Engine.Snapshot.Save {
				if s.changesCnt >= rule.Changes &&
					time.Since(lastSave) >= rule.Seconds {
					if err := s.engine.Dump(); err != nil {
						s.logger.Error(fmt.Sprintf("error to make dump with %d changes", s.changesCnt), zap.Error(err))
					}
					s.changesCnt = 0
					lastSave = time.Now()
					break
				}
			}
		}
	}
}
