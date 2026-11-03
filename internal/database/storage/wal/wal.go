package wal

import (
	"context"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

type WAL struct {
	log                  *zap.Logger
	Offset               int
	Batch                []string
	Pending              []chan error
	MaxBatchSize         int64
	DataDir              string
	MaxSegmentSize       int64
	segments             []segment
	FlushingBatchTimeout time.Duration
	events               chan record
}

type segment struct {
	id   uint64
	size int64
	path string
}

type Worker struct {
	log    *zap.Logger
	events chan record
}

type record struct {
	Command   string
	Arguments []string
	Done      chan error
}

func NewWAL(cfg *config.Config, log *zap.Logger) (*WAL, error) {
	dir := cfg.Engine.WAl.DataDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	w := &WAL{
		log:                  log,
		Batch:                make([]string, 0, cfg.Engine.WAl.FlushingBatchSize),
		MaxBatchSize:         cfg.Engine.WAl.FlushingBatchSize,
		MaxSegmentSize:       cfg.Engine.WAl.MaxSegmentSize,
		DataDir:              dir,
		FlushingBatchTimeout: cfg.Engine.WAl.FlushingBatchTimeout,
		events:               make(chan record, 100),
	}
	return w, nil
}

func (w *WAL) Start(ctx context.Context, flushEvent chan struct{}) {
	worker := newWorker(w.log, w.events)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				w.log.Error("recovered", zap.Any("panic", r))
			}
		}()
		worker.run(ctx, w, flushEvent)
	}()
}

func (w *WAL) listSegments() error {
	segmentOrder := make(map[int]segment)
	lenSegments := 0
	segments, err := os.ReadDir(w.DataDir)
	if err != nil {
		return err
	}
	for _, segmentV := range segments {
		if segmentV.IsDir() {
			continue
		}
		lenSegments++
		order, err := strconv.Atoi(strings.Split(segmentV.Name(), "_")[1])
		if err != nil {
			return err
		}
		info, err := os.Stat(segmentV.Name())
		if err != nil {
			return err
		}
		segmentOrder[order] = segment{
			id:   uint64(order),
			size: info.Size(),
			path: filepath.Join(w.DataDir, segmentV.Name()),
		}
	}
	for i := 1; i <= lenSegments; i++ {
		segment, ok := segmentOrder[i]
		if !ok {
			return fmt.Errorf("segment %d missing", i)
		}
		w.segments = append(w.segments, segment)
	}
	return nil
}

func (w *WAL) Replay(apply func(cmd string, args []string) error) error {
	logs, err := os.ReadDir(w.DataDir)
	if err != nil {
		return err
	}
	for _, logFile := range logs {
		path := filepath.Join(w.DataDir, logFile.Name())

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dataSplit := strings.Split(string(data), "\n")
		for idx, val := range dataSplit {
			if val != "" {
				query := strings.Split(val, " ")
				command := query[0]
				args := query[1:]
				if err := apply(command, args); err != nil {
					return fmt.Errorf("%s line %d: %w", logFile.Name(), idx+1, err)
				}
			}
		}
	}
	return nil

}

func (w *WAL) rotateSegment() error {
	currSegment := w.segments[len(w.segments)]
	if currSegment != (segment{}) {
		file, err := os.Open(currSegment.path)
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}

	name := fmt.Sprintf("segment_%d.log", time.Now().UnixMilli())
	f, err := os.OpenFile(filepath.Join(w.DataDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	segment := segment{
		id:   1,
		size: 0,
		path: f.Name(),
	}
	w.segments = append(w.segments, segment)
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

	currSegment := w.segments[len(w.segments)]
	data := []byte(strings.Join(w.Batch, "\n") + "\n")
	currSegmentFile, err := os.OpenFile(currSegment.path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	n, err := currSegmentFile.Write(data)
	if err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	currSegment.size += int64(n)

	if err := currSegmentFile.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	w.Batch = w.Batch[:0]
	return nil
}

func (w *WAL) flushAndNotify(log *zap.Logger) {
	err := w.flushBatch()
	for _, done := range w.Pending {
		done <- err
	}
	w.Pending = w.Pending[:0]
	if err != nil {
		log.Error("flush failed", zap.Error(err))
	}
}

func newWorker(log *zap.Logger, events chan record) *Worker {
	return &Worker{
		log:    log,
		events: events,
	}
}

func (w *Worker) run(ctx context.Context, wal *WAL, flushEvent chan struct{}) {
	ticker := time.NewTicker(wal.FlushingBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case event := <-w.events:
			w.log.Info("got event",
				zap.Int("offset", wal.Offset),
				zap.String("command", event.Command),
				zap.String("argument", strings.Join(event.Arguments, " ")),
			)
			record := fmt.Sprintf("%s %s", event.Command, strings.Join(event.Arguments, " "))
			wal.Batch = append(wal.Batch, record)
			wal.Pending = append(wal.Pending, event.Done)
			wal.Offset++
			if wal.isBatchFull() {
				wal.flushAndNotify(w.log)
			}
		case <-ticker.C:
			if len(wal.Batch) > 0 {
				w.log.Debug("flushing batch by ticker", zap.Int("size", len(wal.Batch)))
				wal.flushAndNotify(w.log)
			}
		case <-flushEvent:
			w.log.Debug("forced flush butch due to dump")
			wal.flushAndNotify(w.log)
		case <-ctx.Done():
			w.log.Info("context done")
			for {
				select {
				case event, ok := <-w.events:
					if !ok {
						goto flush
					}
					wal.Batch = append(wal.Batch, fmt.Sprintf("%s %s", event.Command, strings.Join(event.Arguments, " ")))
				default:
					goto flush
				}
			}
		flush:
			w.log.Debug("flushing batch by ctx done", zap.Int("size", len(wal.Batch)))
			wal.flushAndNotify(w.log)
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
	currSegment := w.segments[len(w.segments)]
	return currSegment.size >= w.MaxSegmentSize
}

func (w *WAL) close() error {
	currSegment := w.segments[len(w.segments)]
	currSegmentFile, err := os.Open(currSegment.path)
	if err != nil {
		return err
	}
	if err := currSegmentFile.Close(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Append(cmd string, args ...string) <-chan error {
	done := make(chan error, 1)
	w.events <- record{
		Command:   cmd,
		Arguments: args,
		Done:      done,
	}
	return done
}
