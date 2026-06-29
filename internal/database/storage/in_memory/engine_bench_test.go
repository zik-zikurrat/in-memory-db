package inmemory

import (
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
