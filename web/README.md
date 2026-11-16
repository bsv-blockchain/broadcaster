# BSV Broadcaster docs site

The page every BSV Broadcaster API server serves at `/`: an overview at `/`,
the API reference at `/docs`, plus `llms.txt`, `llms-full.txt`, the OpenAPI
spec, `robots.txt` and `sitemap.xml`. It is a Next.js app built as a **static
export** and compiled into the Go binary, so it ships with every deployment and
needs no separate hosting.

Everything about the site lives in this directory. A design change never needs
an edit outside `web/`.

## Run it

```bash
npm ci
npm run dev        # http://localhost:3000, hot reload
```

`npm run dev` has no API server behind it, so the status dot next to the base
URL reads "Unreachable". That is expected; see "Check a change end to end".

```bash
npm run lint
npm run build      # writes the static export to out/
```

## How it reaches the binary

```
web/ (Next.js) --npm run build--> web/out/ --go:embed--> package web (web/embed.go)
                                                           |
                               services/api_server/site.go serves it at /, /docs, ...
```

- `npm run build` writes the export to `out/`. Only `out/.gitkeep` is
  committed; the build recreates it (`scripts/keep-out-placeholder.mjs`).
- `web/embed.go` (`package web`) embeds `out/` with `//go:embed all:out`.
- `services/api_server/site.go` registers one route per top-level entry of the
  export (`/`, `/docs`, `/_next/*`, `/llms.txt`, ...) and sets the response
  headers. A new file or directory under `public/` is picked up without a Go
  change, unless its name collides with an API route (then the server panics at
  startup and the Go tests fail).
- A binary built without `npm run build` has no export and serves the older
  built-in page (`services/api_server/docs.html`) instead.
- `npm ci` writes `node_modules/go.mod` (`scripts/fence-node-modules.mjs`) so
  `go build ./...` does not compile the stray `.go` files some npm packages ship.

## Generated files: do not edit

| File | Source | Regenerate |
|---|---|---|
| `content/routes.json` | `services/api_server/routes.go` (`routeDocs`) | `make docs-routes` (or `go run ./cmd/docs-routes`) from the repo root |
| `public/openapi.yaml`, `public/openapi.json` | `../openapi/bsv-broadcaster.openapi.yaml` | automatic on `npm run dev` / `npm run build` (`scripts/sync-openapi.mjs`); gitignored |

To change an endpoint's description, examples or headers, edit `routes.go` and
regenerate `routes.json`; a Go test (`TestRoutesJSONUpToDate`) fails while the
committed copy is stale. The route grouping, anchors and wording around the
routes are site code (`lib/routes.ts`) and are yours to change.

`lib/chaintracks.ts` is hand-maintained: the Chaintracks endpoint list has no
Go source yet.

## Export constraints

The site must stay exportable as plain files. In practice:

- **No route handlers that read the request** (`headers()`, `cookies()`,
  `request.url`, search params on the server). Text routes (`llms.txt`,
  `llms-full.txt`, `robots.ts`, `sitemap.ts`) declare
  `export const dynamic = "force-static"`; any new one must too.
- **No server actions, no middleware/proxy, no API routes** (`app/api/*`).
  Anything live happens in the browser against the API server, which is the
  same origin (the status dot fetches `/health`).
- **No `next/image` optimisation.** Use plain `<img>` or
  `images: { unoptimized: true }`.
- **No `headers()`, `redirects()` or `rewrites()` in `next.config.ts`**: a
  static export ignores them. Response headers live in Go
  (`services/api_server/site.go`).
- **Runtime host, not a hard-coded one.** Every operator serves this page from
  their own API server. Text a reader copies (base URL, curl examples) uses
  `useApiBaseUrl()` / `<ApiBaseUrl />` from `components/api-base-url.tsx`,
  which is `window.location.origin` after hydration.
  Prerendered text (canonical URLs, `llms*.txt`, the sitemap, JSON-LD) carries
  the placeholder origin `https://broadcaster.invalid`, which the API server
  replaces with the serving origin on every response. Set
  `NEXT_PUBLIC_API_BASE_URL` at build time only to pin one fixed origin.
- **Fonts and images must be redistributable** (see "Licensing rule" below).
  The faces are Geist, Geist Mono and Outfit, all SIL OFL 1.1, loaded with
  `next/font/google`, which downloads them at build time and self-hosts them.

## Licensing rule for everything committed here

This repository is public and open source, and the export is compiled into a
binary that operators download and serve. **Every font, component, image, icon
or other asset committed under `web/` must carry a licence that allows
redistribution in a public open-source repository and in a distributed
binary** (for example SIL OFL 1.1, MIT, Apache 2.0, CC0, or the BSV
Association's own work).

- No React Bits Pro, and no other paid component library whose licence covers
  only your own use.
- No fonts under Fontshare's ITF Free Font License (FFL), or any licence that
  forbids redistributing the font files.
- Ship the licence text with fonts (as `public/licenses/<Name>-OFL.txt`), and
  note the source and licence in a header comment of any third-party code.

## The CSP you must stay within

Set by Go on every site response:

```
default-src 'self';
script-src 'self' 'unsafe-inline';
style-src 'self' 'unsafe-inline';
img-src 'self' data:;
font-src 'self' data:;
connect-src 'self';
object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'
```

Over HTTPS it adds `upgrade-insecure-requests` and the server sends
`Strict-Transport-Security`. Nothing may load from another origin: no CDN
scripts or styles, no web fonts from a font service at runtime, no third-party
analytics, no `eval`. `blob:` workers and images are not allowed. If a change
genuinely needs more, change `siteCSP` in `services/api_server/site.go` in the
same pull request and say why.

## Check a change end to end

From the repository root:

```bash
make docs-preview          # npm ci + npm run build, then serves the export
open http://localhost:8080/
```

This runs `cmd/docs-preview`, which serves the export through the same routes
and headers as the API server, with a stub `/health`. `PORT=9000` changes the
port; `SITE_DIR=web/out go run ./cmd/docs-preview` serves a fresh export from
disk without recompiling. Check the browser console for CSP violations, and
both themes at desktop and phone width.

Go tests for the serving side:

```bash
go test ./services/api_server/ -run 'TestSite|TestRoutesJSON'
```
