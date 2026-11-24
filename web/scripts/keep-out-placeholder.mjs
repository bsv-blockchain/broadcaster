// `next build` empties out/ before exporting, which deletes the committed
// out/.gitkeep. The Go embed (web/embed.go, `//go:embed all:out`) needs that
// placeholder so a plain `go build` compiles without a web build, so put it back.
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

writeFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "out", ".gitkeep"), "");
