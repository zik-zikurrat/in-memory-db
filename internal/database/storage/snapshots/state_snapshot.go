package snapshots

import (
	"context"
	"in-memory-key-value-db/internal/config"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"time"
)

type Snapshot struct {
	save      map[time.Duration]int
	cnagesCnt int
	state     inmemory.Data
}

func NewSnapshot(cfg *config.SnapshotConfig) *Snapshot {
	save := make(map[time.Duration]int, len(cfg.Save))
	for _, rule := range cfg.Save {
		save[time.Duration(rule.Seconds)*time.Second] = rule.Changes
	}
	return &Snapshot{
		save:      save,
		cnagesCnt: 0,
		state:     inmemory.Data{},
	}
}

func (s *Snapshot) Fork(ctx context.Context, change <-chan struct{}) {
	// добавить таймеры
	for {
		select {
		case <-change:
			s.cnagesCnt++
			// обработать случаи
		case 
		}
	}
}
