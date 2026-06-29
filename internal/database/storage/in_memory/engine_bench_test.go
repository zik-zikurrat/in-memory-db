package inmemory

import (
	"math/rand"
	"strconv"
	"testing"
)

var sink string

func benchMixedParallel(b *testing.B, get func(string) (string, bool), set func(string, string)) {
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
			if rng.Intn(100) < 90 {
				sink, _ = get(k)
			} else {
				set(k, "value")
			}
		}
	})
}

func BenchmarkSingle_Mixed_Parallel(b *testing.B) {
	e := NewSingleMapEngine()
	benchMixedParallel(b, e.Get, e.Set)
}

func BenchmarkPartitioned_Mixed_Parallel(b *testing.B) {
	e := NewHashBasedPartitionMapEngine()
	benchMixedParallel(b, e.Get, e.Set)
}
