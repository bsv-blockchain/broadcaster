package apikey

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
)

const ctxPrincipal = "apikey.principal"

// Principal is the identified caller of a request.
type Principal struct {
	KeyID string // "" for anonymous callers
	// QuotaID is the allowance the key draws from (store.APIKey.QuotaID):
	// the WoC limit_id shared by every key on a plan, else KeyID. "" for
	// anonymous callers.
	QuotaID  string
	Tier     string // the key's tier; config.TierAnonymous for anonymous callers
	ClientIP string
}

// Anonymous reports whether the caller presented no key.
func (p Principal) Anonymous() bool { return p.KeyID == "" }

// Subject identifies the principal for per-caller accounting: the anonymous
// quota bucket, the fail-open bucket, and the in-flight concurrency map.
// Keyed callers are accounted by QuotaID, so keys sharing a plan's limit
// share one bucket: creating more keys must not multiply the allowance.
// Anonymous callers are keyed by NormalizeClientIP(ClientIP), not the raw
// address -- an IPv6 client rotating addresses within its own /64 (privacy
// extensions, CGNAT) must share one bucket, not get a fresh one on every
// rotation. ClientIP itself stays raw; request logs (client_ip) still
// report the exact address the request arrived from.
func (p Principal) Subject() string {
	if p.Anonymous() {
		return "ip:" + NormalizeClientIP(p.ClientIP)
	}
	return "key:" + p.QuotaKey()
}

// QuotaKey is the shared quota-store key for a keyed principal: QuotaID,
// falling back to KeyID for a principal built without one.
func (p Principal) QuotaKey() string {
	if p.QuotaID != "" {
		return p.QuotaID
	}
	return p.KeyID
}

// WithPrincipal stores p on the request. Middleware calls it; tests use it
// to stand in for Middleware.
func WithPrincipal(c *gin.Context, p Principal) { c.Set(ctxPrincipal, p) }

// PrincipalFrom returns the request's principal. Without one (identify
// middleware not installed on this route) the caller is anonymous.
func PrincipalFrom(c *gin.Context) Principal {
	if v, ok := c.Get(ctxPrincipal); ok {
		if p, ok := v.(Principal); ok {
			return p
		}
	}
	return Principal{Tier: config.TierAnonymous, ClientIP: c.ClientIP()}
}

// LogFields returns key_id and tier for request logs when the caller
// presented a valid key, and nothing otherwise. Never the key itself.
func LogFields(c *gin.Context) []zap.Field {
	v, ok := c.Get(ctxPrincipal)
	if !ok {
		return nil
	}
	p, ok := v.(Principal)
	if !ok || p.Anonymous() {
		return nil
	}
	fs := []zap.Field{zap.String("key_id", p.KeyID), zap.String("tier", p.Tier)}
	if p.QuotaID != "" && p.QuotaID != p.KeyID {
		fs = append(fs, zap.String("limit_id", p.QuotaID))
	}
	return fs
}
