<!-- BEGIN:nextjs-agent-rules -->

## This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->

# AGENTS.md — docs site (`web/`)

The BSV Broadcaster docs site: a Next.js app exported as static files and
embedded in the Go binary, which serves it at `/`, `/docs` and the
machine-readable paths. `README.md` in this directory has the full picture;
these are the rules.

## Scope

- Design and content changes stay inside `web/`. Do not edit Go code for a
  design change.
- The Go glue is outside `web/` and small: `web/embed.go` (the embed),
  `services/api_server/site.go` (routes, headers, CSP, fallback),
  `services/api_server/routes_json.go` and `cmd/docs-routes` (the endpoint
  generator), `cmd/docs-preview` (local preview). Touch it only when the task is
  about serving, headers or the endpoint data.

## Never hand-edit

- `content/routes.json`: generated from `services/api_server/routes.go`. To
  change endpoint text, edit `routes.go`, then run `go run ./cmd/docs-routes`
  from the repo root. `TestRoutesJSONUpToDate` fails on a stale copy.
- `public/openapi.yaml`, `public/openapi.json`: copied from
  `../openapi/bsv-broadcaster.openapi.yaml` on every `npm run dev`/`build`;
  gitignored.
- `out/`: build output. Only `out/.gitkeep` is committed, and it must stay.

## Static export rules

- `next.config.ts` keeps `output: "export"`. Do not add `headers()`,
  `redirects()`, `rewrites()`, `cacheComponents` or anything else a static
  export ignores or rejects.
- No `app/api/*` routes, no server actions, no middleware/proxy, nothing that
  reads the request at build or run time. Text routes declare
  `export const dynamic = "force-static"`.
- No `next/image` optimisation.
- Response headers and the CSP live in Go (`services/api_server/site.go`).

## Hosts

- Every operator serves this page from its own API server. Anything a reader
  copies (base URL, curl examples) uses `useApiBaseUrl()` or `<ApiBaseUrl />`
  from `components/api-base-url.tsx`, never a hard-coded host.
- `site.url` / `site.apiBaseUrl` default to the placeholder
  `https://broadcaster.invalid`, which the API server rewrites to the serving
  origin (`services/api_server/site.go`). They are only for prerendered text
  (canonical URLs, `llms*.txt`, sitemap, JSON-LD). Never hard-code a real
  operator's host; `NEXT_PUBLIC_API_BASE_URL` pins one only when set at build time.
- Live checks fetch the same origin (`/health`), never another host.

## CSP

`default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self'
'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src
'self'; object-src 'none'; base-uri 'self'; form-action 'self';
frame-ancestors 'none'`. No external origins, no `eval`, no `blob:`. Check the
browser console for violations after any change.

## Licensing (public repository)

Every font, component, image, icon or other asset committed under `web/` ships
in a public open-source repository and in a distributed binary. It must carry a
licence that allows that redistribution: SIL OFL, MIT, Apache 2.0, CC0, or the
BSV Association's own work.

- Never add React Bits Pro components, or any paid component whose licence
  covers only the buyer's own use.
- Never add fonts under Fontshare's ITF Free Font License (FFL), or any font
  licence that forbids redistributing the files.
- Fonts go through `next/font/google` or are OFL files, with their licence
  text in `public/licenses/`. Third-party code keeps a header naming its source
  and licence.
- If an asset's licence is unclear, do not commit it; ask.

## Public repository hygiene

This repository is public. Comments, READMEs and commit messages must not name
or link private repositories or quote file paths from other codebases.

## Before you finish

```bash
cd web && npm run lint && npm run build
cd .. && go build ./... && go test ./services/api_server/ -run 'TestSite|TestRoutesJSON'
make docs-preview   # then look at / and /docs, light and dark, desktop and phone
```
