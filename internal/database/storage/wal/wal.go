package wal

import (
	"bufio"
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
	Offset       int
	Batch        []string
	MaxBatchSize int
	DataDir      string
	// data_directory: "/data/spider/wal"
}

type Worker struct {
	log    *zap.Logger
	events chan WALEvent
}

type WALEvent struct {
	Command   string
	Arguments []string
}

func NewWAL(cfg *config.Config) *WAL {
	batch := make([]string, cfg.Engine.WAl.FlushingBatchSize)
	return &WAL{
		Batch:        batch,
		MaxBatchSize: cfg.Engine.WAl.FlushingBatchSize,
		DataDir:      cfg.Engine.WAl.DataDir,
	}
}

func NewWorker(log *zap.Logger, events chan WALEvent) *Worker {
	return &Worker{
		log:    log,
		events: events,
	}
}

func (w *Worker) Run(wal *WAL, ctx context.Context) {
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
			if wal.isBatchFull() {
				wal.flushBatch()
			}
			wal.Batch = append(wal.Batch, fmt.Sprintf("%s %s", event.Command, strings.Join(event.Arguments, " ")))
			wal.Offset++
		case <-ticker.C:
			w.log.Info("time to flush batch")
			wal.flushBatch()
		case <-ctx.Done():
			w.log.Info("context done")
			wal.flushBatch()
			return
		}
	}
}

func (w *WAL) isBatchFull() bool {
	if len(w.Batch) >= w.MaxBatchSize {
		return true
	}
	return false
}

func (w *WAL) flushBatch() {
	segmentInfo, err := os.Stat(w.DataDir + "segment_")
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	d1 := []byte("hello\ngo\n")
	path1 := filepath.Join(os.TempDir(), "dat1")
	err := os.WriteFile(path1, d1, 0644)
	check(err)

	path2 := filepath.Join(os.TempDir(), "dat2")
	f, err := os.Create(path2)
	check(err)

	defer f.Close()

	d2 := []byte{115, 111, 109, 101, 10}
	n2, err := f.Write(d2)
	check(err)
	fmt.Printf("wrote %d bytes\n", n2)

	n3, err := f.WriteString("writes\n")
	check(err)
	fmt.Printf("wrote %d bytes\n", n3)

	f.Sync()

	w := bufio.NewWriter(f)
	n4, err := w.WriteString("buffered\n")
	check(err)
	fmt.Printf("wrote %d bytes\n", n4)

	w.Flush()

}
