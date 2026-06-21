package expiry

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Deleter interface {
	Del(key string) bool
}

type ExpiryEvent struct {
	Key  string
	Time time.Duration
}

type Worker struct {
	log   *zap.Logger
	event chan ExpiryEvent
}

func NewWorker(log *zap.Logger, event chan ExpiryEvent) *Worker {
	return &Worker{
		log:   log,
		event: event,
	}
}

func (w *Worker) Run(ctx context.Context,
	engine Deleter,
) {

	for {
		select {
		case event := <-w.event:
			w.schedule(engine, event.Key, event.Time)
		case <-ctx.Done():
			w.log.Info("context done")
			return
		}
	}
}

func (w *Worker) schedule(engine Deleter, key string, d time.Duration) {
	if d > 0 {
		time.AfterFunc(d, func() {
			engine.Del(key)
		})
	}
}
