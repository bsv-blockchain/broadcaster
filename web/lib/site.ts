/**
 * The origin baked into prerendered text. Every operator serves this export
 * from its own API server, so the default is a placeholder that the server
 * replaces with the serving origin on every response (originPlaceholder in
 * services/api_server/site.go; keep the two in step). Set
 * NEXT_PUBLIC_API_BASE_URL at build time only to pin one fixed origin.
 */
const defaultOrigin = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "https://broadcaster.invalid").replace(/\/$/, "");

/** Facts about the site that appear in more than one place. */
export const site = {
  name: "BSV Broadcaster",
  /**
   * The serving deployment's origin, for canonical URLs, the sitemap, robots.txt,
   * llms.txt and JSON-LD. The page is served by the API server itself, so this
   * is also the API base URL. See defaultOrigin above.
   */
  url: defaultOrigin,
  description:
    "A high-throughput, callback-driven gateway for submitting BSV transactions, tracking their lifecycle, and streaming status updates to subscribers in real time.",
  association: "BSV Association",
  /** Freshness signals for JSON-LD and the sitemap. Bump `updated` when the content changes. */
  published: "2026-11-05",
  updated: "2026-11-06",
  /** Static: the page is prerendered, so a runtime date is not available. */
  copyrightYear: 2026,
  /**
   * The API base URL for prerendered text (llms.txt, JSON-LD, the first paint).
   * Every broadcaster operator serves this page from their own API server, so
   * anything a reader copies uses the serving host at runtime instead: see
   * `useApiBaseUrl` in components/api-base-url.tsx.
   */
  apiBaseUrl: defaultOrigin,
  repo: "https://github.com/bsv-blockchain/broadcaster",
  /** The OpenAPI spec this server serves (generated from the repo's openapi/ at build time). */
  openapi: "/openapi.yaml",
  parent: "https://bsvblockchain.org",
  /** The recorded soak run on bsvblockchain.org, with its charts and its scope and limits. */
  soakTest: "https://bsvblockchain.org/network/teranode#soak-test",
} as const;

/**
 * Machine-readable companions, announced in every page's head. Next replaces
 * `alternates` wholesale per page, so pages spread this next to their canonical.
 */
export const alternateTypes = {
  "text/markdown": "/llms.txt",
  "application/vnd.oai.openapi+json": "/openapi.json",
};

/** The API reference page. */
export const docsPath = "/docs";

/** Site destinations, in reading order. Used by the header, mobile menu and footer. */
export const nav = [
  { label: "Overview", href: "/" },
  { label: "API reference", href: docsPath },
  { label: "Chaintracks", href: `${docsPath}#chaintracks-api` },
] as const;

/** Whether a nav item is the page being shown. Chaintracks is a section of the docs, not a page. */
export function isCurrent(href: string, pathname: string): boolean {
  return !href.includes("#") && (href === "/" ? pathname === "/" : pathname.startsWith(href));
}
