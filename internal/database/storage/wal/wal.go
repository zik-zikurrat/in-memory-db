package wal

import (
	"cmp"
	"context"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"os"
	"path/filepath"
	"slices"
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
	segs := make([]segment, 0)
	segments, err := os.ReadDir(w.DataDir)
	if err != nil {
		return err
	}
	for _, segmentV := range segments {
		if segmentV.IsDir() {
			continue
		}
		name := segmentV.Name()
		s, ok := strings.CutPrefix(name, "segment_")
		if !ok {
			continue
		}
		s, ok = strings.CutSuffix(s, ".log")
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		info, err := segmentV.Info()
		if err != nil {
			return err
		}
		segs = append(segs, segment{
			id:   id,
			size: info.Size(),
			path: filepath.Join(w.DataDir, segmentV.Name()),
		})
	}
	slices.SortFunc(segs, func(a, b segment) int {
		return cmp.Compare(a.id, b.id)
	})
	for i := 1; i < len(segs); i++ {
		if segs[i].id == segs[i-1].id+1 {
			continue
		} else {
			return fmt.Errorf("segment %d missing", segs[i-1].id+1)
		}
	}
	w.segments = segs
	return nil
}

func (w *WAL) Replay(apply func(cmd string, args []string) error) error {
	if err := w.listSegments(); err != nil {
		return err
	}
	for _, segmentV := range w.segments {
		data, err := os.ReadFile(segmentV.path)
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
					return fmt.Errorf("segment id: %d line %d: %w", segmentV.id, idx+1, err)
				}
			}
		}
	}
	return nil

}

func (w *WAL) rotateSegment() error {
	var id uint64 = 1
	name := fmt.Sprintf("segment_%d.log", id)
	if len(w.segments) == 0 {
		w.log.Warn("empty segments list")
		segment := segment{
			id:   id,
			size: 0,
			path: filepath.Join(w.DataDir, name),
		}
		file, err := w.openFile(name)
		if err != nil {
			return err
		}
		defer w.closeFile(file)
		w.segments = append(w.segments, segment)
		return nil
	}
	currSegment := w.segments[len(w.segments)-1]
	id = currSegment.id + 1
	name = fmt.Sprintf("segment_%d.log", id)
	segment := segment{
		id:   currSegment.id + 1,
		size: 0,
		path: filepath.Join(w.DataDir, name),
	}
	file, err := w.openFile(name)
	if err != nil {
		return err
	}
	defer w.closeFile(file)
	w.segments = append(w.segments, segment)
	return nil
}

func (w *WAL) flushBatch() error {
	defer func() {
		w.Batch = w.Batch[:0]
	}()
	if len(w.Batch) == 0 {
		return nil
	}

	if len(w.segments) == 0 || w.isSegmentFull() {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}
	currSegment := &w.segments[len(w.segments)-1]
	data := []byte(strings.Join(w.Batch, "\n") + "\n")
	currSegmentFile, err := os.OpenFile(currSegment.path, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer w.closeFile(currSegmentFile)
	delCorruptedFileStart := currSegment.size
	n, err := currSegmentFile.Write(data)
	if err != nil {
		if n != 0 {
			if err := deleteCorruptedFileData(currSegmentFile, delCorruptedFileStart); err != nil {
				return fmt.Errorf("failed to delete corrupted data: %w", err)
			}
		}
		return fmt.Errorf("write batch: %w", err)
	}
	currSegment.size += int64(n)

	if err := currSegmentFile.Sync(); err != nil {
		if err := deleteCorruptedFileData(currSegmentFile, delCorruptedFileStart); err != nil {
			return fmt.Errorf("failed to delete corrupted data: %w", err)
		}
		currSegment.size -= int64(n)
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

func (w *WAL) flushAndNotify() {
	err := w.flushBatch()
	for _, done := range w.Pending {
		done <- err
	}
	w.Pending = w.Pending[:0]
	if err != nil {
		w.log.Error("flush failed", zap.Error(err))
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
				wal.flushAndNotify()
			}
		case <-ticker.C:
			if len(wal.Batch) > 0 {
				w.log.Debug("flushing batch by ticker", zap.Int("size", len(wal.Batch)))
				wal.flushAndNotify()
			}
		case <-flushEvent:
			w.log.Debug("forced flush butch due to dump")
			wal.flushAndNotify()
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
			wal.flushAndNotify()
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
	if len(w.segments) == 0 {
		return false
	}
	currSegment := w.segments[len(w.segments)-1]
	return currSegment.size >= w.MaxSegmentSize
}

func (w *WAL) close() error {
	if len(w.segments) == 0 {
		return nil
	}
	currSegment := w.segments[len(w.segments)-1]
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

func (w *WAL) closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		w.log.Error("failed to close file", zap.Error(err))
	}
}

func (w *WAL) openFile(name string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(w.DataDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func deleteCorruptedFileData(file *os.File, delStart int64) error {
	err := file.Truncate(delStart)
	if err != nil {
		return err
	}
	return nil
}
