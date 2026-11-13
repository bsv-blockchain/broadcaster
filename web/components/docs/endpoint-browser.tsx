"use client";

import { MagnifyingGlass, X } from "@phosphor-icons/react";
import { type ReactNode, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { Endpoint } from "@/components/endpoint";
import { StatusLifecycle } from "@/components/status-lifecycle";
import { groupedRoutes, routeSlug, type RouteDoc } from "@/lib/routes";
import { docsPath } from "@/lib/site";

function subscribeLocation(notify: () => void): () => void {
  window.addEventListener("popstate", notify);
  return () => window.removeEventListener("popstate", notify);
}

/** `?q=` from the address, so `/docs?q=webhook` (a link, or an agent submitting the form) arrives filtered. */
function useUrlQuery(): string {
  return useSyncExternalStore(
    subscribeLocation,
    () => new URLSearchParams(window.location.search).get("q") ?? "",
    () => "",
  );
}

/** Everything a reader might type to find a route: method, path, summary and prose. */
function haystack(route: RouteDoc): string {
  return [route.method, route.path, route.summary, route.description ?? "", route.notes ?? ""]
    .join(" ")
    .toLowerCase();
}

/**
 * The API server routes, grouped the way an integrator reads them, with a
 * filter on top.
 *
 * The filter matches method, path, summary and description, so "webhook",
 * "merkle" or "/tx" all narrow the list. `/` focuses it from anywhere on the
 * page, as on most reference sites, unless the reader is already typing.
 *
 * The status lifecycle sits with Track, but only while nothing is filtered:
 * a filtered list is for finding a route, and a diagram between results is in
 * the way.
 */
export function EndpointBrowser(): ReactNode {
  const groups = useMemo(() => groupedRoutes(), []);
  const total = useMemo(() => groups.reduce((n, g) => n + g.routes.length, 0), [groups]);
  const urlQuery = useUrlQuery();
  /* Null until the reader types; until then the address decides. */
  const [typed, setTyped] = useState<string | null>(null);
  const query = typed ?? urlQuery;
  const setQuery = (value: string): void => setTyped(value);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onKey = (event: KeyboardEvent): void => {
      if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target?.closest("input, textarea, select, [contenteditable='true']")) return;
      event.preventDefault();
      input.current?.focus();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const q = query.trim().toLowerCase();
  const shown = q
    ? groups
        .map(({ group, routes }) => ({
          group,
          routes: routes.filter((route) => haystack(route).includes(q)),
        }))
        .filter(({ routes }) => routes.length > 0)
    : groups;
  const matches = shown.reduce((n, g) => n + g.routes.length, 0);

  return (
    <div>
      <div className="sticky top-[7.5rem] z-20 -mx-1 bg-background/92 px-1 py-3 backdrop-blur-md lg:top-16">
        <div className="flex items-center gap-3">
          {/* A real GET form, annotated for WebMCP, so an agent can call it as a tool;
              in the browser it filters in place instead of navigating. */}
          <form
            action={docsPath}
            method="get"
            role="search"
            aria-label="Filter API routes"
            tool-name="filter-api-routes"
            tool-description="Filter the BSV Broadcaster API reference to the routes whose method, path, summary or description contain the query, e.g. 'webhook', 'merkle' or '/tx'."
            onSubmit={(event) => event.preventDefault()}
            className="flex min-w-0 flex-1"
          >
            <label className="relative flex min-w-0 flex-1 items-center">
              <span className="sr-only">Filter endpoints</span>
              <MagnifyingGlass
                aria-hidden
                className="pointer-events-none absolute start-3.5 size-4 text-ink-muted"
              />
              <input
                ref={input}
                type="search"
                name="q"
                autoComplete="off"
                enterKeyHint="search"
                tool-param-description="Text to match against route method, path, summary and description"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    setQuery("");
                    event.currentTarget.blur();
                  }
                }}
                placeholder="Filter routes"
                className="h-11 w-full rounded-lg border border-border bg-surface-raised ps-10 pe-12 text-sm text-foreground transition-colors outline-none placeholder:text-ink-muted hover:border-info-bd focus:border-primary-edge focus:ring-2 focus:ring-ring/30 [&::-webkit-search-cancel-button]:hidden"
              />
              {query ? (
                <button
                  type="button"
                  onClick={() => {
                    setQuery("");
                    input.current?.focus();
                  }}
                  aria-label="Clear filter"
                  className="absolute end-2 inline-flex size-8 items-center justify-center rounded-md text-ink-muted transition-colors hover:bg-surface hover:text-foreground"
                >
                  <X aria-hidden className="size-4" />
                </button>
              ) : (
                <kbd
                  aria-hidden
                  className="pointer-events-none absolute end-3 hidden rounded border border-border px-1.5 font-mono text-xs leading-5 text-ink-muted sm:block"
                >
                  /
                </kbd>
              )}
            </label>
          </form>
          <p className="tnum shrink-0 text-sm text-ink-muted" aria-live="polite">
            {q ? (
              <>
                <span className="font-medium text-foreground">{matches}</span> of {total}
              </>
            ) : (
              <>
                <span className="font-medium text-foreground">{total}</span> routes
              </>
            )}
          </p>
        </div>
      </div>

      {shown.length === 0 ? (
        <div className="mt-6 rounded-lg border border-dashed border-border px-6 py-12 text-center">
          <p className="text-sm font-medium">No routes match &ldquo;{query.trim()}&rdquo;</p>
          <p className="mt-1 text-sm text-ink-muted">
            Try a path such as <code className="font-mono">/tx</code>, or a word such as
            &ldquo;webhook&rdquo;.
          </p>
          <button
            type="button"
            onClick={() => setQuery("")}
            className="mt-4 text-sm font-medium text-primary underline-offset-4 hover:underline"
          >
            Show every route
          </button>
        </div>
      ) : (
        <div className="mt-4 space-y-14">
          {shown.map(({ group, routes }) => (
            <section
              key={group.id}
              id={group.id}
              aria-labelledby={`${group.id}-heading`}
              className="scroll-mt-48 lg:scroll-mt-36"
            >
              {/* The short name as a label (it is what the sidebar calls the group),
                  and the question the group answers as the heading. */}
              <div className="mb-4 border-b border-border pb-3">
                <p className="label-sm flex items-baseline gap-2 text-primary uppercase">
                  {group.label}
                  <span className="tnum text-ink-muted">
                    {routes.length} {routes.length === 1 ? "route" : "routes"}
                  </span>
                </p>
                <h2 id={`${group.id}-heading`} className="headline-md mt-2">
                  <a href={`#${group.id}`} className="hover:text-primary">
                    {group.question}
                  </a>
                </h2>
              </div>
              {group.id === "track" && !q ? (
                <div className="mb-6">
                  <StatusLifecycle />
                </div>
              ) : null}
              <div className="flex flex-col gap-3">
                {routes.map((route) => (
                  <Endpoint key={routeSlug(route)} route={route} />
                ))}
              </div>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
