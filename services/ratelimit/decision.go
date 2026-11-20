// Package ratelimit enforces tiered quotas on the public API: a per-key
// concurrency cap and a per-minute cost-unit quota, shared across replicas
// via store.QuotaStore leases (keyed callers) or per-replica token buckets
// (anonymous callers, and keyed callers while the quota store is down).
package ratelimit

import "time"

// Decision is the outcome of one charge.
type Decision struct {
	Allowed   bool
	Remaining int64         // approximate units left
	Reset     time.Duration // until the window resets (keyed) or the bucket refills enough (local)
}

// Tier is a resolved rate-limit policy. Zero fields mean "no limit".
type Tier struct {
	Name           string
	UnitsPerMinute int64
	MaxInFlight    int
	MaxBatch       int
	// Burst is the token-bucket capacity where the tier is enforced by a
	// local bucket: ratelimit.anonymous.burst_units, or UnitsPerMinute.
	Burst int64
}
