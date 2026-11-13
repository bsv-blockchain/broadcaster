import { ArrowRight, ArrowUpRight } from "@phosphor-icons/react/dist/ssr";
import Link from "next/link";
import type { ReactNode } from "react";
import { ApiIllustration } from "@/components/api-illustration";
import { Reveal, RevealGroup, RevealItem } from "@/components/reveal";
import { RevealHeadline } from "@/components/reveal-headline";
import { Eyebrow, Section } from "@/components/section";
import { ButtonLink } from "@/components/ui/button";
import { groupedRoutes } from "@/lib/routes";
import { docsPath, site } from "@/lib/site";
import { cn } from "@/lib/utils";

/**
 * The front page's door to the API reference: the copy and the action on one
 * side, the animated request-and-status loop on the other.
 *
 * The groups are listed with their real route counts, each a deep link into
 * the reference, so the section previews the page's structure rather than
 * describing it.
 */
export function ApiCta(): ReactNode {
  const groups = groupedRoutes();
  const total = groups.reduce((n, g) => n + g.routes.length, 0);

  return (
    <Section id="api" tone="surface" aria-labelledby="api-cta-heading">
      <div className="grid grid-cols-[minmax(0,1fr)] items-center gap-12 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)] lg:gap-16">
        <div>
          <Eyebrow>API reference</Eyebrow>
          <h2 id="api-cta-heading" className="headline-lg">
            <RevealHeadline as="span">Submit, track and stream, from one reference</RevealHeadline>
          </h2>
          <p className="body-lg mt-4 max-w-xl text-ink-muted">
            {total} API server routes with their request and response contracts, headers and
            examples, the status lifecycle, and the Chaintracks block-header API. Every route has
            its own link.
          </p>

          <RevealGroup className="mt-8 grid max-w-xl grid-cols-2 gap-px overflow-hidden rounded-lg border border-border bg-border">
            {[
              ...groups.map(({ group, routes }) => ({
                href: `${docsPath}#${group.id}`,
                label: group.label,
                count: `${routes.length} ${routes.length === 1 ? "route" : "routes"}`,
              })),
              { href: `${docsPath}#chaintracks-api`, label: "Chaintracks", count: "v2 and legacy v1" },
            ].map((item, i, all) => (
              <RevealItem
                key={item.href}
                className={cn(
                  "bg-surface-raised",
                  all.length % 2 === 1 && i === all.length - 1 && "col-span-2",
                )}
              >
                <Link
                  href={item.href}
                  className="group flex h-full items-center justify-between gap-3 px-4 py-3.5 transition-colors hover:bg-info-bg focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none focus-visible:ring-inset"
                >
                  <span className="min-w-0">
                    <span className="block text-sm font-medium">{item.label}</span>
                    <span className="tnum block text-xs text-ink-muted">{item.count}</span>
                  </span>
                  <ArrowRight
                    aria-hidden
                    className="size-4 shrink-0 text-ink-muted transition-transform duration-150 group-hover:translate-x-0.5 group-hover:text-primary"
                  />
                </Link>
              </RevealItem>
            ))}
          </RevealGroup>

          <div className="mt-8 flex flex-wrap gap-3">
            <ButtonLink href={docsPath} size="lg" variant="primary">
              Open the API reference
              <ArrowRight aria-hidden />
            </ButtonLink>
            <ButtonLink
              href={site.openapi}
              target="_blank"
              rel="noreferrer noopener"
              size="lg"
              variant="secondary"
            >
              OpenAPI spec
              <ArrowUpRight aria-hidden />
            </ButtonLink>
          </div>
        </div>

        <Reveal delay={0.1}>
          <ApiIllustration />
        </Reveal>
      </div>
    </Section>
  );
}
