import routes from "@/content/routes.json";

export type RouteDoc = {
  method: string;
  path: string;
  summary: string;
  description?: string;
  headers: { name: string; requirement: string; description: string }[];
  requestBodies: { contentType: string; description?: string; example?: string }[];
  responseStatus?: string;
  responseBody?: string;
  notes?: string;
};

/** Generated from services/api_server/routes.go by `go run ./cmd/docs-routes`; never edit routes.json by hand. */
export const routeDocs = routes as RouteDoc[];

/** "POST /tx" → "post-tx", "GET /tx/:txid" → "get-tx-txid". Stable anchor per endpoint. */
export function routeSlug(route: Pick<RouteDoc, "method" | "path">): string {
  return `${route.method}-${route.path}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/** Called by Merkle Service, not by integrators. */
export function isInternal(route: RouteDoc): boolean {
  return route.summary.toLowerCase().startsWith("internal");
}

export type RouteGroup = {
  id: string;
  label: string;
  /** The question the group answers, used as its heading on the docs page. */
  question: string;
  /** Matches a route into this group; the first group that matches wins. */
  match: (route: RouteDoc) => boolean;
};

/** The order a new integrator reads them in: check, submit, track, then the operator surface. */
export const routeGroups: RouteGroup[] = [
  {
    id: "health",
    label: "Health and policy",
    question: "Is the server up, and what will it accept?",
    match: (r) => ["/health", "/ready", "/policy"].includes(r.path),
  },
  {
    id: "submit",
    label: "Submit",
    question: "How do I submit transactions?",
    match: (r) => r.method === "POST" && ["/tx", "/txs"].includes(r.path),
  },
  {
    id: "track",
    label: "Track",
    question: "How do I track a transaction's status?",
    match: (r) => r.path.startsWith("/tx/") || r.path === "/events",
  },
  {
    id: "merkle-and-blocks",
    label: "Merkle and blocks",
    question: "How are merkle proofs and blocks processed?",
    match: () => true,
  },
];

export function groupedRoutes(): { group: RouteGroup; routes: RouteDoc[] }[] {
  const buckets = routeGroups.map((group) => ({ group, routes: [] as RouteDoc[] }));
  for (const route of routeDocs) {
    buckets.find((b) => b.group.match(route))?.routes.push(route);
  }
  return buckets.filter((b) => b.routes.length > 0);
}
