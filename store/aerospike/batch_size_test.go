package aerospike

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/config"
)

// Issue #90: a zero batch step makes `for i := 0; i < n; i += batchSize`
// spin forever. effectiveBatchSize must substitute the default so those
// loops terminate even when New is called with BatchSize 0.
func TestEffectiveBatchSizeClampsNonPositive(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "zero clamps to default", in: 0, want: config.DefaultAerospikeBatchSize},
		{name: "negative clamps to default", in: -1, want: config.DefaultAerospikeBatchSize},
		{name: "positive value is preserved", in: 100, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveBatchSize(tt.in, nil); got != tt.want {
				t.Fatalf("effectiveBatchSize(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}

	// A clamped step must advance. With the old `i += 0` this loop never ends.
	const n = 3
	step := effectiveBatchSize(0, nil)
	visits := 0
	for i := 0; i < n; i += step {
		visits++
		if visits > n {
			t.Fatal("batch loop did not advance")
		}
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1 (step %d covers %d ids in one batch)", visits, step, n)
	}
}

func TestEffectiveBatchSizeWarnsWhenClamped(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	if got := effectiveBatchSize(0, logger); got != config.DefaultAerospikeBatchSize {
		t.Fatalf("effectiveBatchSize(0) = %d, want %d", got, config.DefaultAerospikeBatchSize)
	}
	if logs.Len() != 1 {
		t.Fatalf("clamping must warn once, got %d", logs.Len())
	}
	if logs.All()[0].Level != zapcore.WarnLevel {
		t.Fatalf("log level = %s, want warn", logs.All()[0].Level)
	}

	if got := effectiveBatchSize(100, logger); got != 100 {
		t.Fatalf("effectiveBatchSize(100) = %d, want 100", got)
	}
	if logs.Len() != 1 {
		t.Fatalf("a positive batch size must not warn, got %d logs", logs.Len())
	}
}
