package aerospike

import "testing"

// Issue #90: a zero batch step makes `for i := 0; i < n; i += batchSize`
// spin forever. effectiveBatchSize must substitute the default so those
// loops terminate even when New is called with BatchSize 0.
func TestEffectiveBatchSizeClampsNonPositive(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "zero clamps to default", in: 0, want: defaultBatchSize},
		{name: "negative clamps to default", in: -1, want: defaultBatchSize},
		{name: "positive value is preserved", in: 100, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveBatchSize(tt.in); got != tt.want {
				t.Fatalf("effectiveBatchSize(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}

	// A clamped step must advance. With the old `i += 0` this loop never ends.
	const n = 3
	step := effectiveBatchSize(0)
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
