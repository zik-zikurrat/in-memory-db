package expirity

import (
	"context"
	"fmt"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Expirity struct {
	toClean  map[string]time.Duration // нужно для того чтобы в случае завершения контекста знать что надо очистить
	cleaners int                      // кол-во горутин которые чистят storage
}

type ExpirityEvent struct {
	Key  string
	Time time.Duration
}

type Worker struct {
	log   *zap.Logger
	event chan ExpirityEvent
}

func NewExpirity() *Expirity {
	return &Expirity{
		toClean:  make(map[string]time.Duration),
		cleaners: 100,
	}
}

func NewWorker(log *zap.Logger, event chan ExpirityEvent) *Worker {
	return &Worker{
		log:   log,
		event: event,
	}
}

func (w *Worker) Run(ctx context.Context,
	expirity *Expirity,
	engine *inmemory.Engine,
) {
	toClean := make(chan ExpirityEvent)
	var wg sync.WaitGroup

	for i := 1; i <= expirity.cleaners; i++ {
		wg.Add(1)
		go func(id int, wg *sync.WaitGroup, toClean <-chan ExpirityEvent) {
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
			expirity.toClean[event.Key] = event.Time
			toClean <- event
		case <-ctx.Done():
			w.log.Info("context done")
			return
		}
	}
}

func (w *Worker) clear(
	id int,
	wg *sync.WaitGroup,
	toClean <-chan ExpirityEvent,
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
