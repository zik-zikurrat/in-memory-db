package wal

import (
	"errors"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func newTestWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	cfg := &config.Config{}
	cfg.Engine.WAl.DataDir = dir
	w, err := NewWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func writeSegment(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeBatch(t *testing.T, wal *WAL, cmd string, args ...string) {
	t.Helper()
	recordS := fmt.Sprintf("%s %s", cmd, strings.Join(args, " "))
	wal.Batch = append(wal.Batch, recordS)
}

func TestReplay_ReadsInOrderAndKeepsFiles(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "segment_1.log", "SET a 1 0\nSET b 2 0\n")
	writeSegment(t, dir, "segment_2.log", "DEL a\n")

	var got []string
	err := newTestWAL(t, dir).Replay(func(cmd string, args []string) error {
		got = append(got, cmd+" "+strings.Join(args, " "))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"SET a 1 0", "SET b 2 0", "DEL a"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	for _, name := range []string{"segment_1.log", "segment_2.log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s was deleted by Replay", name)
		}
	}
}

func TestReplay_ReturnsApplyError(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "segment_1.log", "SET a 1 0\nSET b 2 0\n")

	boom := errors.New("boom")
	err := newTestWAL(t, dir).Replay(func(string, []string) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestListSegments_SortCorrect(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "segment_1.log", "SET a 1 0\nSET b 2 0\n")
	writeSegment(t, dir, "segment_2.log", "DEL a\n")
	writeSegment(t, dir, ".DS_Store", "error\n")
	writeSegment(t, dir, "segment_3.log", "DEL b\n")

	wal := newTestWAL(t, dir)
	if err := wal.listSegments(); err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for _, s := range wal.segments {
		got = append(got, s.id)
	}
	want := []uint64{1, 2, 3}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
func TestListSegments_MissingSegment(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "segment_43.log", "SET a 1 0\nSET b 2 0\n")
	writeSegment(t, dir, "segment_53.log", "DEL a\n")
	writeSegment(t, dir, ".DS_Store", "error\n")

	wal := newTestWAL(t, dir)
	if err := wal.listSegments(); err == nil {
		t.Fatal("want error")
	}
}

func TestFlushBatch_AppendsAndUpdatesSize(t *testing.T) {
	dir := t.TempDir()
	old := "SET c 3 0\nSET d 4 0\n"
	writeSegment(t, dir, "segment_45.log", old)

	w := newTestWAL(t, dir)
	w.MaxSegmentSize = 1 << 20
	if err := w.listSegments(); err != nil {
		t.Fatal(err)
	}

	w.Batch = append(w.Batch, "SET e 5 0", "DEL b")
	if err := w.flushBatch(); err != nil {
		t.Fatal(err)
	}

	added := "SET e 5 0\nDEL b\n"
	wantSize := int64(len(old) + len(added))

	if got := w.segments[len(w.segments)-1].size; got != wantSize {
		t.Fatalf("segment.size = %d, want %d", got, wantSize)
	}

	path := filepath.Join(dir, "segment_45.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != old+added {
		t.Fatalf("file = %q, want %q", data, old+added)
	}

	if int64(len(data)) != wantSize {
		t.Fatalf("file size = %d, want %d", len(data), wantSize)
	}

	if len(w.Batch) != 0 {
		t.Fatalf("batch not cleared: %v", w.Batch)
	}
}

func TestRotateSegment_CorrectIdINCR(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "segment_43.log", "SET a 1 0\nSET b 2 0\n")
	writeSegment(t, dir, "segment_44.log", "DEL a\n")
	writeSegment(t, dir, "segment_45.log", "SET c 3 0\nSET d 4 0\n")

	wal := newTestWAL(t, dir)
	if err := wal.listSegments(); err != nil {
		t.Fatal(err)
	}
	if err := wal.rotateSegment(); err != nil {
		t.Fatal(err)
	}
	if err := wal.listSegments(); err != nil {
		t.Fatal(err)
	}
	lastSegment := wal.segments[len(wal.segments)-1]
	if lastSegment.id != 46 {
		t.Fatalf("want segment id 46 got: %d", lastSegment.id)
	}
}

func TestReplay_CorrectReply(t *testing.T) {

}
