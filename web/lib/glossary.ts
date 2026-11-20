import { routeSlug } from "@/lib/routes";
import { docsPath, site } from "@/lib/site";

/**
 * Terms that appear as code in running copy (the FAQ), each with where it is
 * documented and one sentence on what it is.
 *
 * The sentences are written here rather than cut from the route descriptions:
 * a first sentence lifted out of a paragraph rarely stands on its own. They are
 * checked against content/routes.json, lib/lifecycle.ts and docs/.
 */

export type GlossaryEntry = {
  href: string;
  tip: string;
  /** Where the link goes, said in the tooltip. */
  where: string;
  external?: boolean;
};

const route = (method: string, path: string): string =>
  `${docsPath}#${routeSlug({ method, path })}`;

const submit = route("POST", "/tx");
const lifecycle = `${docsPath}#track`;
const migration = `${site.repo}/blob/main/docs/migration-arcade-to-bsv-broadcaster.md`;

export const glossary: Record<string, GlossaryEntry> = {
  "POST /tx": {
    href: submit,
    where: "POST /tx",
    tip: "Submits one transaction and answers 202 once it passes policy validation.",
  },
  "POST /txs": {
    href: route("POST", "/txs"),
    where: "POST /txs",
    tip: "Submits a batch of transactions in a single request.",
  },
  "GET /tx/:txid": {
    href: route("GET", "/tx/:txid"),
    where: "GET /tx/:txid",
    tip: "Returns a transaction's current status, with its merkle proof once it is mined.",
  },
  "GET /events": {
    href: route("GET", "/events"),
    where: "GET /events",
    tip: "Streams status updates as Server-Sent Events, from a separate service on port 8082 by default.",
  },
  "X-CallbackUrl": {
    href: submit,
    where: "POST /tx headers",
    tip: "A public HTTPS URL that Broadcaster posts this transaction's status updates to.",
  },
  "X-CallbackToken": {
    href: submit,
    where: "POST /tx headers",
    tip: "A bearer token sent with every webhook, which also scopes your Server-Sent Events stream.",
  },
  "X-FullStatusUpdates": {
    href: submit,
    where: "POST /tx headers",
    tip: "Set to true to have every status transition posted to your webhook, not only MINED and REJECTED.",
  },
  "application/octet-stream": {
    href: submit,
    where: "POST /tx request body",
    tip: "The transaction as raw bytes, the most efficient body to send.",
  },
  "text/plain": {
    href: submit,
    where: "POST /tx request body",
    tip: "The transaction as a hex string; surrounding whitespace is trimmed.",
  },
  "202": {
    href: submit,
    where: "POST /tx response",
    tip: "Accepted: the transaction passed policy validation and is queued for broadcast.",
  },
  RECEIVED: {
    href: lifecycle,
    where: "the status lifecycle",
    tip: "The first status, stored at intake; sent over Server-Sent Events, never to a webhook.",
  },
  MINED: {
    href: lifecycle,
    where: "the status lifecycle",
    tip: "The transaction is in a block, and the update carries its merkle proof.",
  },
  REJECTED: {
    href: lifecycle,
    where: "the status lifecycle",
    tip: "A verdict against the transaction, either at intake or from the network.",
  },
  blockHash: {
    href: lifecycle,
    where: "the MINED frame",
    tip: "The hash of the block that contains the transaction.",
  },
  blockHeight: {
    href: lifecycle,
    where: "the MINED frame",
    tip: "The height of that block on the chain.",
  },
  merklePath: {
    href: lifecycle,
    where: "the MINED frame",
    tip: "The merkle proof in BUMP format, for checking the transaction against the block header.",
  },
  "ARCADE_*": {
    href: migration,
    where: "the migration guide",
    external: true,
    tip: "Environment variables from the Arcade era; still read, but deprecated.",
  },
  "BSV_BROADCASTER_*": {
    href: migration,
    where: "the migration guide",
    external: true,
    tip: "The current environment variable prefix; it wins when both names are set.",
  },
};

/** The entry for a code span, ignoring a value after a header name (`X-FullStatusUpdates: true`). */
export function lookup(term: string): GlossaryEntry | undefined {
  return glossary[term] ?? glossary[term.replace(/:\s.*$/, "")];
}
