"use client";

import { CaretRight, Info, LinkSimple } from "@phosphor-icons/react";
import { useState, useSyncExternalStore, type ReactNode } from "react";
import { CodeBlock } from "@/components/code-block";
import { DataTable } from "@/components/data-table";
import { Note } from "@/components/note";
import { Tabs } from "@/components/tabs";
import { Badge, MethodBadge, RequirementBadge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { isInternal, routeSlug, type RouteDoc } from "@/lib/routes";
import { cn } from "@/lib/utils";

function subscribeHash(callback: () => void): () => void {
  window.addEventListener("hashchange", callback);
  return () => window.removeEventListener("hashchange", callback);
}

/** The URL fragment, live. Empty on the server, so nothing renders open until hydration. */
function useHash(): string {
  return useSyncExternalStore(
    subscribeHash,
    () => window.location.hash,
    () => "",
  );
}

/** A sample made only of `<placeholders>` is not worth copying. */
function isPlaceholder(code: string): boolean {
  return code
    .split("\n")
    .filter((line) => line.trim())
    .every((line) => /^\s*<[^>]+>\s*$/.test(line));
}

function Part({ title, children }: { title: string; children: ReactNode }): ReactNode {
  return (
    <section className="mt-8 first:mt-0">
      <h4 className="label-sm mb-3 text-ink-muted uppercase">{title}</h4>
      {children}
    </section>
  );
}

function RequestBody({ body }: { body: RouteDoc["requestBodies"][number] }): ReactNode {
  return (
    <>
      {body.description ? (
        <p className="body-md mb-3 max-w-3xl text-ink-muted">{body.description}</p>
      ) : null}
      {body.example ? (
        <CodeBlock
          code={body.example}
          title={body.contentType.startsWith("(") ? undefined : body.contentType}
          copy={!isPlaceholder(body.example)}
        />
      ) : null}
    </>
  );
}

function cleanSummary(route: RouteDoc): string {
  if (!isInternal(route)) return route.summary;
  const rest = route.summary.replace(/^internal\s*[—–-]\s*/i, "");
  return rest.charAt(0).toUpperCase() + rest.slice(1);
}

/**
 * The few facts a reader needs before deciding to open a contract, all read
 * from the contract itself so the tooltip can never disagree with the panel.
 */
function essentials(route: RouteDoc): { label: string; value: string }[] {
  const rows: { label: string; value: string }[] = [];
  if (route.responseStatus) rows.push({ label: "Returns", value: route.responseStatus });
  const required = route.headers.filter((h) => h.requirement.toLowerCase() === "required");
  rows.push({
    label: "Requires",
    value: required.length > 0 ? required.map((h) => h.name).join(", ") : "No required headers",
  });
  const bodies = route.requestBodies
    .map((b) => b.contentType)
    .filter((type) => !type.startsWith("("));
  if (bodies.length > 0) rows.push({ label: "Body", value: bodies.join(", ") });
  if (isInternal(route)) rows.push({ label: "Caller", value: "Merkle Service, not integrators" });
  return rows;
}

/**
 * The (i) beside each row. Opens on hover and keyboard focus like any shadcn
 * tooltip, and on tap too, because a touch screen has no hover and DESIGN.md
 * asks for tooltips to open on tap there.
 */
function EndpointInfo({ route }: { route: RouteDoc }): ReactNode {
  const [open, setOpen] = useState(false);
  return (
    <Tooltip open={open} onOpenChange={setOpen}>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={`About ${route.method} ${route.path}`}
          onClick={(event) => {
            event.preventDefault();
            setOpen(true);
          }}
          className="me-3 inline-flex size-9 shrink-0 items-center justify-center self-center rounded-md text-ink-muted transition-colors hover:bg-surface hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
        >
          <Info aria-hidden className="size-4.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent
        side="bottom"
        align="end"
        sideOffset={6}
        collisionPadding={12}
        className="max-w-[min(20rem,calc(100vw-1.5rem))] px-3.5 py-3 text-start text-pretty"
      >
        <span className="block font-medium">{cleanSummary(route)}</span>
        <span className="mt-2 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
          {essentials(route).map((row) => (
            <span key={row.label} className="contents">
              <span className="opacity-70">{row.label}</span>
              <span className="font-mono [overflow-wrap:anywhere]">{row.value}</span>
            </span>
          ))}
        </span>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * One endpoint as a disclosure, after bsvblockchain.org's `Disclosure`: a
 * bordered row that tints toward primary when open.
 *
 * Every endpoint has an anchor (`#post-tx`), and arriving on one opens it, so
 * a link into a single contract lands on that contract rather than a closed
 * row. The panel height animates, the one exception the reference allows for
 * an accordion, because a collapsing panel has to reflow the page under it.
 * Closed panels stay in the document and are made inert.
 */
export function Endpoint({ route }: { route: RouteDoc }): ReactNode {
  const slug = routeSlug(route);
  const hash = useHash();
  const [manual, setManual] = useState<boolean | null>(null);
  const open = manual ?? hash === `#${slug}`;
  const panelId = `${slug}-panel`;
  const internal = isInternal(route);

  return (
    <article
      id={slug}
      className={cn(
        "scroll-mt-48 rounded-lg border bg-surface-raised transition-colors lg:scroll-mt-36",
        open ? "border-primary-edge" : "border-border hover:border-info-bd",
      )}
    >
      {/* The row: a chevron first, so it reads as something that opens, then the
          contract's name; the (i) is a sibling, since a button cannot hold a button. */}
      <div className="flex items-stretch">
        {/* The WAI accordion pattern: the toggle sits inside the heading, so the
            outline reads h2 group, h3 route, h4 part. */}
        <h3 className="flex min-w-0 flex-1 font-sans font-normal tracking-normal [text-wrap:wrap]">
          <button
            type="button"
            onClick={() => setManual(!open)}
            aria-expanded={open}
            aria-controls={panelId}
            data-glow=""
            className="card-surface grid min-w-0 flex-1 grid-cols-[auto_auto_minmax(0,1fr)] items-center gap-x-3 gap-y-1.5 rounded-lg py-4 ps-4 pe-2 text-start focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none md:grid-cols-[auto_auto_auto_minmax(0,1fr)] md:gap-x-4"
          >
            <CaretRight
              aria-hidden
              weight="bold"
              className={cn(
                "col-start-1 row-start-1 size-4 shrink-0 text-ink-muted transition-transform duration-200",
                open && "rotate-90 text-primary",
              )}
            />
            <span className="col-start-2 row-start-1">
              <MethodBadge method={route.method} />
            </span>
            <code className="col-start-3 row-start-1 overflow-hidden font-mono text-sm font-medium text-ellipsis whitespace-nowrap text-foreground">
              {route.path}
            </code>
            <span className="col-span-2 col-start-2 row-start-2 flex min-w-0 items-center gap-2 text-sm text-ink-muted md:col-span-1 md:col-start-4 md:row-start-1">
              {internal ? <Badge variant="neutral">Internal</Badge> : null}
              <span className="md:overflow-hidden md:text-ellipsis md:whitespace-nowrap">
                {cleanSummary(route)}
              </span>
            </span>
          </button>
        </h3>
        <EndpointInfo route={route} />
      </div>

      <div
        id={panelId}
        inert={!open}
        className={cn(
          "grid transition-[grid-template-rows] duration-250 ease-[cubic-bezier(0.16,1,0.3,1)]",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="min-h-0 overflow-hidden">
          <div className="border-t border-border px-5 pt-6 pb-6">
            <div className="mb-8 flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
              {route.description ? (
                <p className="body-md max-w-3xl text-foreground">{route.description}</p>
              ) : null}
              <a
                href={`#${slug}`}
                className="inline-flex h-8 shrink-0 items-center gap-1.5 rounded-md px-2 text-xs font-medium text-ink-muted transition-colors hover:bg-secondary hover:text-foreground"
              >
                <LinkSimple aria-hidden className="size-3.5" />
                Link to this endpoint
              </a>
            </div>

            {route.headers.length > 0 ? (
              <Part title="Headers">
                <DataTable
                  head={["Header", "Requirement", "Description"]}
                  rows={route.headers.map((h) => [
                    h.name,
                    <RequirementBadge key="r" requirement={h.requirement} />,
                    h.description,
                  ])}
                />
              </Part>
            ) : null}

            {route.requestBodies.length > 0 ? (
              <Part title="Request body">
                {route.requestBodies.length > 1 ? (
                  <Tabs
                    items={route.requestBodies.map((body) => ({
                      label: body.contentType,
                      content: <RequestBody body={body} />,
                    }))}
                  />
                ) : (
                  <RequestBody body={route.requestBodies[0]} />
                )}
              </Part>
            ) : null}

            <Part title="Response">
              {route.responseBody ? (
                <CodeBlock
                  code={route.responseBody}
                  title={route.responseStatus ?? "Response"}
                  copy={!isPlaceholder(route.responseBody)}
                />
              ) : route.responseStatus ? (
                <Badge variant="neutral" className="tnum font-mono tracking-normal text-foreground">
                  {route.responseStatus}
                </Badge>
              ) : null}
            </Part>

            {route.notes ? (
              <Note title="Note:" className="mt-8">
                {route.notes}
              </Note>
            ) : null}
          </div>
        </div>
      </div>
    </article>
  );
}
