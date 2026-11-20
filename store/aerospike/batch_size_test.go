package aerospike

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/broadcaster/config"
)

// Issue #90: a zero batch step makes `for i := 0; i < n; i += batchSize`
// spin forever. New must reject a non-positive BatchSize before it connects,
// so callers that bypass config validation fail loudly.
func TestNewRejectsNonPositiveBatchSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		// No Aerospike listens on this host. The batch size check must
		// return first, so the error is ErrInvalidBatchSize, not a dial error.
		s, err := New(context.Background(), config.Aero{
			Hosts:     []string{"127.0.0.1:1"},
			Namespace: "bsv_broadcaster",
			BatchSize: size,
		})
		if !errors.Is(err, ErrInvalidBatchSize) {
			t.Fatalf("New with BatchSize %d: err = %v, want ErrInvalidBatchSize", size, err)
		}
		if s != nil {
			t.Fatalf("New with BatchSize %d returned a store", size)
		}
	}
}
