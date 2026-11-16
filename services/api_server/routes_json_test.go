package api_server

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// web/content/routes.json is generated from routeDocs; run
// `go run ./cmd/docs-routes` after changing a route's docs.
func TestRoutesJSONUpToDate(t *testing.T) {
	var want bytes.Buffer
	require.NoError(t, WriteRoutesJSON(&want))

	got, err := os.ReadFile("../../web/content/routes.json")
	require.NoError(t, err)
	assert.Equal(t, want.String(), string(got),
		"web/content/routes.json is stale: run `go run ./cmd/docs-routes`")
}
