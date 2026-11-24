// Package apikey implements API-key credentials for the public API: the key
// format, a cached resolver from key to stored record, and the Gin
// middleware that identifies each caller as a Principal.
package apikey

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math/big"
	"strings"
)

// Key format: Prefix + 43 base62 chars (32 random bytes) + "_" + 6 base62
// chars (CRC32 of the body). The prefix makes leaked keys recognizable to
// secret scanners; the checksum rejects typos and garbage before any store
// lookup. External introspection providers (see Introspector) issue keys
// in the same format under their own configured prefixes.
const (
	Prefix      = "bsvb_"
	secretBytes = 32
	bodyLen     = 43
	checksumLen = 6
	idBytes     = 12
)

// ErrMalformed reports a string that is not a well-formed API key.
var ErrMalformed = errors.New("malformed api key")

// Generate returns a new random API key.
func Generate() (string, error) {
	var b [secretBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	body := encodeBase62(b[:], bodyLen)
	return Prefix + body + "_" + checksum(body), nil
}

// Validate checks the key's shape and checksum without any store access.
func Validate(key string) error {
	return ValidateWithPrefix(key, Prefix)
}

// ValidateWithPrefix is Validate for a key issued under prefix, such as an
// introspection provider's.
func ValidateWithPrefix(key, prefix string) error {
	if !strings.HasPrefix(key, prefix) {
		return ErrMalformed
	}
	rest := key[len(prefix):]
	if len(rest) != bodyLen+1+checksumLen || rest[bodyLen] != '_' {
		return ErrMalformed
	}
	body, sum := rest[:bodyLen], rest[bodyLen+1:]
	if !isBase62(body) || !isBase62(sum) {
		return ErrMalformed
	}
	if subtle.ConstantTimeCompare([]byte(checksum(body)), []byte(sum)) != 1 {
		return ErrMalformed
	}
	return nil
}

// Hash is the stored lookup form of a key: hex SHA-256. Keys carry 256 bits
// of entropy, so a fast unsalted hash is safe and allows an indexed lookup.
func Hash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Hint is the displayable fragment of a key: its first 9 and last 3 chars.
func Hint(key string) string {
	if len(key) < 12 {
		return ""
	}
	return key[:9] + "…" + key[len(key)-3:]
}

// NewID returns a public key identifier, safe to log.
func NewID() (string, error) {
	var b [idBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "key_" + hex.EncodeToString(b[:]), nil
}

func checksum(body string) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc32.ChecksumIEEE([]byte(body)))
	return encodeBase62(b[:], checksumLen)
}

// encodeBase62 renders b as a big-endian base62 number (0-9a-zA-Z),
// left-padded with '0' to width.
func encodeBase62(b []byte, width int) string {
	s := new(big.Int).SetBytes(b).Text(62)
	if len(s) < width {
		s = strings.Repeat("0", width-len(s)) + s
	}
	return s
}

func isBase62(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}
