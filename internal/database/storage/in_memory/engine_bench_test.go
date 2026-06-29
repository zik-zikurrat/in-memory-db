package inmemory

import (
	"math/rand"
	"strconv"
	"testing"
)

func BenchmarkSingleMapEngine_Set(b *testing.B) {
	engine := NewSingleMapEngine()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		engine.Set("key"+strconv.Itoa(i), "value")
	}
}

var sink string

func BenchmarkEngine_Mixed_Parallel(b *testing.B) {
	e := NewHashBasedPartitionMapEngine()

	const keyspace = 100_000
	for i := 0; i < keyspace; i++ {
		e.Set("key"+strconv.Itoa(i), "value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := "key" + strconv.Itoa(rng.Intn(keyspace))
			if rng.Intn(100) < 90 {
				sink, _ = e.Get(k)
			} else {
				e.Set(k, "value")
			}
		}
	})
}
