import { chaintracksV1, chaintracksV2 } from "@/lib/chaintracks";
import { faq } from "@/lib/faq";
import { branches, happyPath } from "@/lib/lifecycle";
import { groupedRoutes, routeSlug, type RouteDoc } from "@/lib/routes";
import { docsPath, site } from "@/lib/site";

/**
 * llms.txt and llms-full.txt, built from the same data as the pages.
 *
 * llms.txt is the index described at llmstxt.org: a title, a one-line summary,
 * then sections of links. llms-full.txt is the whole API reference as one
 * Markdown document, so an assistant can answer from it without crawling.
 */

const docs = `${site.url}${docsPath}`;
const fence = (lang: string, code: string): string => `\`\`\`${lang}\n${code.trim()}\n\`\`\``;

const intro = (): string[] => [
  `# ${site.name}`,
  "",
  `> ${site.description}`,
  "",
  `${site.name} is a Teranode-native replacement for ARC, formerly called Arcade, published by the ${site.association}. The public API server is ${site.apiBaseUrl}. Submit with \`POST /tx\` or \`POST /txs\`, track with \`GET /tx/:txid\`, stream updates over Server-Sent Events on \`GET /events\`, or receive webhooks via \`X-CallbackUrl\`. A \`MINED\` update carries the merkle proof.`,
  "",
];

export function llmsIndex(): string {
  const lines = intro();
  lines.push("## Docs", "");
  lines.push(`- [API reference](${docs}): every route with its request and response contract, headers and examples`);
  for (const { group, routes } of groupedRoutes()) {
    lines.push(
      `- [${group.label}](${docs}#${group.id}): ${routes.map((r) => `\`${r.method} ${r.path}\``).join(", ")}`,
    );
  }
  lines.push(`- [Chaintracks API](${docs}#chaintracks-api): block-header tracking, v2 and legacy v1`);
  lines.push(`- [Full reference as Markdown](${site.url}/llms-full.txt): this site's API reference in one file`);
  lines.push("", "## API", "");
  lines.push(`- [OpenAPI 3 spec (JSON)](${site.url}/openapi.json)`);
  lines.push(`- [OpenAPI 3 spec (YAML)](${site.url}/openapi.yaml)`);
  lines.push(`- [API server](${site.apiBaseUrl}/health): live health check`);
  lines.push("", "## FAQ", "");
  for (const item of faq) lines.push(`- [${item.question}](${site.url}/#${item.id}): ${item.answer}`);
  lines.push("", "## Optional", "");
  lines.push(`- [Source code](${site.repo})`);
  lines.push(`- [Soak test results](${site.soakTest}): recorded load run, with its scope and limits`);
  lines.push(`- [BSV Association](${site.parent})`);
  return `${lines.join("\n")}\n`;
}

function route(r: RouteDoc): string[] {
  const out = [`### ${r.method} ${r.path}`, "", `Anchor: ${docs}#${routeSlug(r)}`, "", r.summary + "."];
  if (r.description) out.push("", r.description);
  if (r.headers.length) {
    out.push("", "| Header | Requirement | Description |", "|---|---|---|");
    for (const h of r.headers) out.push(`| \`${h.name}\` | ${h.requirement} | ${h.description.replace(/\|/g, "\\|").replace(/\n/g, " ")} |`);
  }
  for (const body of r.requestBodies) {
    out.push("", `Request body: ${body.contentType}${body.description ? `. ${body.description}` : ""}`);
    if (body.example) out.push("", fence("", body.example));
  }
  if (r.responseStatus) out.push("", `Response: ${r.responseStatus}`);
  if (r.responseBody) out.push("", fence("json", r.responseBody));
  if (r.notes) out.push("", r.notes);
  return out;
}

export function llmsFull(): string {
  const lines = intro();
  lines.push(`Source: ${docs}. Last updated ${site.updated}.`, "", "## API server routes", "");
  lines.push(`Base URL: ${site.apiBaseUrl}. Content encoding UTF-8.`, "");
  for (const { group, routes } of groupedRoutes()) {
    lines.push(`## ${group.label}`, "");
    for (const r of routes) lines.push(...route(r), "");
  }

  lines.push("## Status lifecycle", "");
  lines.push("Statuses a transaction moves through, in order, and who is told:", "");
  lines.push("| Status | Meaning | SSE | Webhook |", "|---|---|---|---|");
  const webhook = { default: "always", full: "with X-FullStatusUpdates", never: "never" } as const;
  for (const s of [...happyPath, ...branches]) {
    const name = s.id === "reorg" ? `\`${s.status}\` (after a reorg)` : `\`${s.status}\``;
    lines.push(`| ${name} | ${s.summary} | yes | ${webhook[s.webhook]} |`);
  }
  lines.push("");
  for (const b of branches) lines.push(`- \`${b.status}\` (${b.title}): ${b.explainer}`);
  lines.push("");

  lines.push("## Chaintracks API", "");
  lines.push("A separate listener for block-header tracking, default local base URL `http://localhost:8083`. New integrations should use `/chaintracks/v2`. All paths are GET.", "");
  lines.push("| Path | Purpose |", "|---|---|");
  for (const [path, purpose] of chaintracksV2) lines.push(`| \`${path}\` | ${purpose} |`);
  lines.push("", "Legacy v1 (RPC-style envelope `{\"status\":\"success\",\"value\":...}`):", "");
  lines.push("| Path | Purpose |", "|---|---|");
  for (const [path, purpose] of chaintracksV1) lines.push(`| \`${path}\` | ${purpose} |`);

  lines.push("", "## FAQ", "");
  for (const item of faq) lines.push(`### ${item.question}`, "", item.answer, "");
  return `${lines.join("\n")}\n`;
}
