"use client";

import { ArrowUpRight } from "@phosphor-icons/react";
import { type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { groupedRoutes, routeSlug } from "@/lib/routes";
import { site } from "@/lib/site";
import { cn } from "@/lib/utils";

type Item = { id: string; label: string; method?: string };
type Block = { id: string; label: string; items: Item[] };

/** The Chaintracks part of the page, in the order it appears there. */
const chaintracks: Block = {
  id: "chaintracks-api",
  label: "Chaintracks",
  items: [
    { id: "chaintracks-overview", label: "Base URL and checks" },
    { id: "chaintracks-v2", label: "v2 endpoints" },
    { id: "chaintracks-v1", label: "Legacy v1" },
  ],
};

/**
 * The docs page's table of contents: every group and every route, with the
 * one being read marked.
 *
 * "Being read" is the last heading or endpoint whose top has passed just under
 * the header and filter bar. When it changes, the sidebar scrolls itself just
 * enough to keep the marked entry in view, without moving the page.
 */
export function DocsSidebar(): ReactNode {
  const blocks = useMemo<Block[]>(
    () => [
      ...groupedRoutes().map(({ group, routes }) => ({
        id: group.id,
        label: group.label,
        items: routes.map((route) => ({
          id: routeSlug(route),
          label: route.path,
          method: route.method,
        })),
      })),
      chaintracks,
    ],
    [],
  );
  const ids = useMemo(() => blocks.flatMap((b) => [b.id, ...b.items.map((i) => i.id)]), [blocks]);
  const [active, setActive] = useState<string | null>(null);
  const scroller = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let frame = 0;
    const update = (): void => {
      frame = 0;
      const line = 150;
      let current: string | null = null;
      for (const id of ids) {
        const el = document.getElementById(id);
        if (el && el.getBoundingClientRect().top <= line) current = id;
      }
      setActive(current);
    };
    const onScroll = (): void => {
      if (!frame) frame = requestAnimationFrame(update);
    };
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll, { passive: true });
    window.addEventListener("hashchange", onScroll);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
      window.removeEventListener("hashchange", onScroll);
    };
  }, [ids]);

  useEffect(() => {
    const box = scroller.current;
    const link = active ? box?.querySelector<HTMLElement>(`[data-id="${active}"]`) : null;
    if (!box || !link) return;
    const top = link.offsetTop - box.offsetTop;
    if (top < box.scrollTop + 24) box.scrollTop = Math.max(0, top - 24);
    else if (top + link.offsetHeight > box.scrollTop + box.clientHeight - 24)
      box.scrollTop = top + link.offsetHeight - box.clientHeight + 24;
  }, [active]);

  const activeBlock = blocks.find((b) => b.id === active || b.items.some((i) => i.id === active));

  return (
    <nav aria-label="API reference" className="sticky top-16 pt-8">
      <div
        ref={scroller}
        className="scrollbar-faint relative max-h-[calc(100dvh-6rem)] overflow-y-auto pe-3 pb-10"
      >
        <a
          href="#top"
          className="label-sm mb-4 block text-ink-muted uppercase transition-colors hover:text-foreground"
        >
          On this page
        </a>
        <ul className="space-y-6">
          {blocks.map((block) => {
            const inBlock = activeBlock?.id === block.id;
            return (
              <li key={block.id}>
                <a
                  href={`#${block.id}`}
                  data-id={block.id}
                  aria-current={active === block.id ? "location" : undefined}
                  className={cn(
                    "block text-sm font-semibold transition-colors",
                    inBlock ? "text-foreground" : "text-ink-muted hover:text-foreground",
                  )}
                >
                  {block.label}
                </a>
                <ul className="mt-2 space-y-px border-s border-border">
                  {block.items.map((item) => {
                    const current = active === item.id;
                    return (
                      <li key={item.id}>
                        <a
                          href={`#${item.id}`}
                          data-id={item.id}
                          aria-current={current ? "location" : undefined}
                          className={cn(
                            "-ms-px flex items-baseline gap-2 border-s py-1.5 ps-3 text-[0.8125rem] transition-colors",
                            current
                              ? "border-primary text-foreground"
                              : "border-transparent text-ink-muted hover:border-ink-muted/50 hover:text-foreground",
                          )}
                        >
                          {item.method ? (
                            <span
                              className={cn(
                                "w-9 shrink-0 font-mono text-[0.6875rem] font-semibold",
                                item.method === "POST" ? "text-pos" : "text-primary",
                              )}
                            >
                              {item.method}
                            </span>
                          ) : null}
                          <span
                            className={cn(
                              "min-w-0 break-all",
                              item.method && "font-mono text-[0.75rem]",
                            )}
                          >
                            {item.label}
                          </span>
                        </a>
                      </li>
                    );
                  })}
                </ul>
              </li>
            );
          })}
        </ul>

        <div className="mt-8 border-t border-border pt-6">
          <p className="label-sm mb-3 text-ink-muted uppercase">Resources</p>
          <ul className="space-y-2">
            {[
              { label: "OpenAPI spec", href: site.openapi },
              { label: "Source code", href: site.repo },
            ].map((link) => (
              <li key={link.href}>
                <a
                  href={link.href}
                  target="_blank"
                  rel="noreferrer noopener"
                  className="inline-flex items-center gap-1 text-sm text-ink-muted transition-colors hover:text-foreground"
                >
                  {link.label}
                  <ArrowUpRight aria-hidden className="size-3" />
                </a>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </nav>
  );
}
