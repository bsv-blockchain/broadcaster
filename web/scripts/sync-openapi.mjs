// Copies the repository's OpenAPI spec into public/ so the site serves it as
// /openapi.yaml and /openapi.json. The source of truth is
// ../openapi/bsv-broadcaster.openapi.yaml; the copies are generated on every
// `npm run dev` and `npm run build` and are gitignored. Never edit them.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

const web = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(web, "..", "openapi", "bsv-broadcaster.openapi.yaml");
const yaml = readFileSync(source, "utf8");

mkdirSync(join(web, "public"), { recursive: true });
writeFileSync(join(web, "public", "openapi.yaml"), yaml);
writeFileSync(join(web, "public", "openapi.json"), `${JSON.stringify(parse(yaml), null, 2)}\n`);
console.log("openapi: copied", source, "to public/openapi.{yaml,json}");
