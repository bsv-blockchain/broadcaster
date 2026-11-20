// Some npm packages ship .go files (e.g. flatted/golang). Without a fence,
// `go build ./...`, `go vet ./...` and `go test ./...` at the repository root
// walk into web/node_modules and compile them. A go.mod makes node_modules its
// own module, which the root module's ./... pattern skips.
import { existsSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const modules = join(dirname(fileURLToPath(import.meta.url)), "..", "node_modules");
if (existsSync(modules)) {
  writeFileSync(join(modules, "go.mod"), "module ignored/web/node_modules\n");
}
