package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalLimiter(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	d := l.take("ip:1.2.3.4", 2, 2, 1, now)
	require.True(t, d.Allowed)
	require.Equal(t, int64(1), d.Remaining)
	require.True(t, l.take("ip:1.2.3.4", 2, 2, 1, now).Allowed)

	d = l.take("ip:1.2.3.4", 2, 2, 1, now)
	require.False(t, d.Allowed, "burst is one minute of quota")
	require.InDelta(t, 30.0, d.Reset.Seconds(), 0.001, "2/min refills one unit every 30s")

	require.True(t, l.take("ip:5.6.7.8", 2, 2, 1, now).Allowed, "subjects are independent")
	require.True(t, l.take("ip:1.2.3.4", 2, 2, 1, now.Add(30*time.Second)).Allowed, "refilled")
	require.False(t, l.take("ip:9.9.9.9", 2, 2, 3, now).Allowed, "a cost above the burst never fits")
}

// A burst below one minute caps what a fresh subject can spend at once
// without changing the refill rate.
func TestLocalLimiterBurstBelowOneMinute(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	// 600/min (10 units a second), burst 20: two 10-unit spends, then held
	// to the rate rather than the 60 a one-minute bucket would allow.
	require.True(t, l.take("ip:1.2.3.4", 600, 20, 10, now).Allowed)
	require.True(t, l.take("ip:1.2.3.4", 600, 20, 10, now).Allowed)
	d := l.take("ip:1.2.3.4", 600, 20, 10, now)
	require.False(t, d.Allowed, "burst caps the up-front spend")
	require.InDelta(t, 1.0, d.Reset.Seconds(), 0.001, "10 units refill in one second")

	require.True(t, l.take("ip:1.2.3.4", 600, 20, 10, now.Add(time.Second)).Allowed, "refilled at the per-minute rate")

	d = l.take("ip:5.6.7.8", 600, 20, 10, now)
	require.True(t, d.Allowed)
	require.InDelta(t, 1.0, d.Reset.Seconds(), 0.001, "time to full is measured against the burst, not the minute")
}

// Changing the burst rebuilds the bucket, as a change of rate does.
func TestLocalLimiterBurstChangeRebuilds(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	require.True(t, l.take("ip:1.2.3.4", 600, 10, 10, now).Allowed)
	require.False(t, l.take("ip:1.2.3.4", 600, 10, 10, now).Allowed)
	require.True(t, l.take("ip:1.2.3.4", 600, 20, 10, now).Allowed, "a new burst starts a full bucket")
}

func TestLocalLimiterResetWhenAllowed(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	d := l.take("ip:10.0.0.1", 60, 60, 30, now)
	require.True(t, d.Allowed)
	require.Equal(t, int64(30), d.Remaining)
	require.InDelta(t, 30.0, d.Reset.Seconds(), 0.001, "60/min refills to full one second per unit")
}

func TestInflight(t *testing.T) {
	f := newInflight()
	require.True(t, f.acquire("key:a", 2))
	require.True(t, f.acquire("key:a", 2))
	require.False(t, f.acquire("key:a", 2))
	require.True(t, f.acquire("key:b", 2))
	f.release("key:a")
	require.True(t, f.acquire("key:a", 2))
	f.release("key:a")
	f.release("key:a")
	f.release("key:b")
	require.Empty(t, f.n, "released subjects are dropped")
}

func TestInflightLimitOne(t *testing.T) {
	f := newInflight()
	require.True(t, f.acquire("key:solo", 1))
	require.False(t, f.acquire("key:solo", 1))
	f.release("key:solo")
	require.Empty(t, f.n, "released subjects are dropped")
}
