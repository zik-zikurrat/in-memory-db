package expiry

import (
	"context"
	"fmt"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Expiry struct {
	cleaners int
}

type ExpiryEvent struct {
	Key  string
	Time time.Duration
}

type Worker struct {
	log   *zap.Logger
	event chan ExpiryEvent
}

func NewExpiry() *Expiry {
	return &Expiry{
		cleaners: 100,
	}
}

func NewWorker(log *zap.Logger, event chan ExpiryEvent) *Worker {
	return &Worker{
		log:   log,
		event: event,
	}
}

func (w *Worker) Run(ctx context.Context,
	expiry *Expiry,
	engine *inmemory.Engine,
) {
	toClean := make(chan ExpiryEvent)
	var wg sync.WaitGroup

	for i := 1; i <= expiry.cleaners; i++ {
		wg.Add(1)
		go func(id int, wg *sync.WaitGroup, toClean <-chan ExpiryEvent) {
			defer func() {
				if r := recover(); r != nil {
					w.log.Error("recovered. Panic in cleaner goroutine", zap.Any("panic", r))
				}
			}()
			w.clear(i, wg, toClean, engine)
		}(i, &wg, toClean)
	}

	for {
		select {
		case event := <-w.event:
			toClean <- event
		case <-ctx.Done():
			close(toClean)
			w.log.Info("context done")
			return
		}
	}
}

func (w *Worker) clear(
	id int,
	wg *sync.WaitGroup,
	toClean <-chan ExpiryEvent,
	engine *inmemory.Engine,
) {
	defer wg.Done()

	for event := range toClean {
		timer := time.NewTimer(event.Time)
		w.log.Info(fmt.Sprintf("Cleaner %d started clean with key: %s and time %s", id, event.Key, event.Time.String()))
		<-timer.C

		engine.Del(event.Key)
		w.log.Info(fmt.Sprintf("Cleaner %d finished clean with key: %s", id, event.Key))
	}
}
