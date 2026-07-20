package inmemory

import (
	"context"
	"fmt"
	"in-memory-key-value-db/internal/database/storage/wal"
	"math/rand"
	"strconv"
	"testing"

	"go.uber.org/zap"
)

var sink string

const (
	_defaultCacheLimit = 268435456 // 256MB
)

func helperNewPartitionEngine(ctx context.Context) *HashBasedPartitionMapEngine {
	walEvents := make(chan wal.WALEvent, 100)
	noopLogger := zap.NewNop()

	testCache := &Cache{
		limit: _defaultCacheLimit,
	}

	return NewHashBasedPartitionMapEngine(ctx, testCache, walEvents, noopLogger)
}

func benchMixedParallel(b *testing.B, get func(string) (string, bool), set func(string, string), writePct int) {
	const keyspace = 100_000
	for i := 0; i < keyspace; i++ {
		set("key"+strconv.Itoa(i), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := "key" + strconv.Itoa(rng.Intn(keyspace))
			if rng.Intn(100) < writePct {
				set(k, "value")
			} else {
				sink, _ = get(k)
			}
		}
	})
}

func BenchmarkSingle_Mixed_Parallel(b *testing.B) {
	e := NewSingleMapEngine()
	benchMixedParallel(b, e.Get, e.Set, 10)
}

func BenchmarkPartitioned_Mixed_Parallel(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := helperNewPartitionEngine(ctx)
	benchMixedParallel(b, e.Get, e.Set, 10)
}

func BenchmarkSyncMap_Mixed_Parallel(b *testing.B) {
	e := NewSyncMapEngine()
	benchMixedParallel(b, e.Get, e.Set, 10)
}

func BenchmarkPartitioned_Mixed(b *testing.B) {
	for _, w := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("writes=%d%%", w), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			e := helperNewPartitionEngine(ctx)
			benchMixedParallel(b, e.Get, e.Set, w)
		})
	}
}

func BenchmarkMixed_ByWritePct(b *testing.B) {
	var cancels []context.CancelFunc
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	engines := []struct {
		name string
		new  func() (get func(string) (string, bool), set func(string, string))
	}{
		{"single", func() (func(string) (string, bool), func(string, string)) {
			e := NewSingleMapEngine()
			return e.Get, e.Set
		}},
		{"partitioned", func() (func(string) (string, bool), func(string, string)) {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)

			e := helperNewPartitionEngine(ctx)
			return e.Get, e.Set
		}},
		{"sync", func() (func(string) (string, bool), func(string, string)) {
			e := NewSyncMapEngine()
			return e.Get, e.Set
		}},
	}

	for _, w := range []int{1, 10, 50} {
		for _, eng := range engines {
			b.Run(fmt.Sprintf("%s/writes=%d%%", eng.name, w), func(b *testing.B) {
				get, set := eng.new()
				benchMixedParallel(b, get, set, w)
			})
		}
	}
}
