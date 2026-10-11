package wal

import (
	"errors"
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
