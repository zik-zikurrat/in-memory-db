package wal

import (
	"context"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
)

type WAL struct {
	Offset          int
	Batch           []string
	MaxBatchSize    int64
	DataDir         string
	MaxSegmentSize  int64
	CurrSegmentPath string
	CurrSegment     *os.File
	CurrSegmentSize int64
}

type Worker struct {
	log    *zap.Logger
	events chan WALEvent
}

type WALEvent struct {
	Command   string
	Arguments []string
}

func NewWAL(cfg *config.Config) (*WAL, error) {
	dir := cfg.Engine.WAl.DataDir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	w := &WAL{
		Batch:          make([]string, 0, cfg.Engine.WAl.FlushingBatchSize),
		MaxBatchSize:   cfg.Engine.WAl.FlushingBatchSize,
		MaxSegmentSize: cfg.Engine.WAl.MaxSegmentSize,
		DataDir:        dir,
	}
	if err := w.rotateSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) rotateSegment() error {
	if w.CurrSegment != nil {
		if err := w.CurrSegment.Sync(); err != nil {
			return err
		}
		if err := w.CurrSegment.Close(); err != nil {
			return err
		}
	}

	name := fmt.Sprintf("segment_%d.log", time.Now().UnixMilli())
	f, err := os.OpenFile(filepath.Join(w.DataDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	w.CurrSegment = f
	w.CurrSegmentPath = f.Name()
	w.CurrSegmentSize = 0
	return nil
}

func (w *WAL) flushBatch() error {
	if len(w.Batch) == 0 {
		return nil
	}

	if w.isSegmentFull() {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	data := []byte(strings.Join(w.Batch, "\n") + "\n")
	n, err := w.CurrSegment.Write(data)
	if err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	w.CurrSegmentSize += int64(n)

	if err := w.CurrSegment.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	w.Batch = w.Batch[:0]
	return nil
}

func NewWorker(log *zap.Logger, events chan WALEvent) *Worker {
	return &Worker{
		log:    log,
		events: events,
	}
}

func (w *Worker) Run(ctx context.Context, wal *WAL) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case event := <-w.events:
			w.log.Info("got event",
				zap.Int("offset", wal.Offset),
				zap.String("command", event.Command),
				zap.String("argument", strings.Join(event.Arguments, " ")),
			)
			wal.Batch = append(wal.Batch, fmt.Sprintf("%s %s", event.Command, strings.Join(event.Arguments, " ")))
			wal.Offset++
			if wal.isBatchFull() {
				if err := wal.flushBatch(); err != nil {
					w.log.Error("flush failed", zap.Error(err))
				}
			}
		case <-ticker.C:
			w.log.Info("time to flush batch")
			if err := wal.flushBatch(); err != nil {
				w.log.Error("flush failed", zap.Error(err))
			}
		case <-ctx.Done():
			w.log.Info("context done")
			if err := wal.flushBatch(); err != nil {
				w.log.Error("flush failed", zap.Error(err))
			}
			if err := wal.close(); err != nil {
				w.log.Error("error closed wal", zap.Error(err))
			}
			return
		}
	}
}

func (w *WAL) isBatchFull() bool {
	return len(w.Batch) >= int(w.MaxBatchSize)
}

func (w *WAL) isSegmentFull() bool {
	return w.CurrSegmentSize >= w.MaxSegmentSize
}

func (w *WAL) close() error {
	if err := w.CurrSegment.Close(); err != nil {
		return err
	}
	return nil
}
