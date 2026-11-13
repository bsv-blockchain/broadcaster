package api_server

import (
	"encoding/json"
	"io"
)

// WriteRoutesJSON writes the API route docs as the docs site's
// web/content/routes.json: the single source of the endpoints both the
// built-in page and the site render. cmd/docs-routes writes the file;
// TestRoutesJSONUpToDate fails when the committed copy is stale.
func WriteRoutesJSON(w io.Writer) error {
	routes := make([]RouteDoc, len(routeDocs))
	for i, r := range routeDocs {
		// Arrays, never null: the site maps over both.
		if r.Headers == nil {
			r.Headers = []RouteHeader{}
		}
		if r.RequestBodies == nil {
			r.RequestBodies = []RouteBody{}
		}
		routes[i] = r
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(routes)
}
