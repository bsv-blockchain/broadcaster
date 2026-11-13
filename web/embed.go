// Package web embeds the static export of the API docs site that the API
// server serves at "/". The site is a Next.js project in this directory;
// `npm run build` writes its export to out/, and this package compiles it into
// the binary. Without a web build out/ holds only a placeholder, and the API
// server falls back to its built-in docs page.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:out
var out embed.FS

// FS returns the static export rooted at out/. It holds no index.html when the
// site has not been built.
func FS() fs.FS {
	sub, err := fs.Sub(out, "out")
	if err != nil {
		// fs.Sub only fails on an invalid path; "out" is a constant.
		panic(err)
	}
	return sub
}
