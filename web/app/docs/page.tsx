import type { Metadata } from "next";
import Link from "next/link";
import type { ReactNode } from "react";
import { CodeBlock, InlineCode as C } from "@/components/code-block";
import { DataTable } from "@/components/data-table";
import { Disclosure } from "@/components/disclosure";
import { DocsSidebar } from "@/components/docs/docs-sidebar";
import { EndpointBrowser } from "@/components/docs/endpoint-browser";
import { Note } from "@/components/note";
import { SectionBar } from "@/components/section-bar";
import { ApiStatus } from "@/components/api-status";
import { ApiBaseUrl } from "@/components/api-base-url";
import { StatBand } from "@/components/stat-band";
import { chaintracksCurl, chaintracksV1, chaintracksV2 } from "@/lib/chaintracks";
import { groupedRoutes } from "@/lib/routes";
import { alternateTypes, docsPath, site } from "@/lib/site";
import { JsonLd } from "@/components/json-ld";
import { docsGraph } from "@/lib/structured-data";

const description =
  "Every BSV Broadcaster API server route with its request and response contract, headers and examples, plus the Chaintracks block-header API.";

export const metadata: Metadata = {
  title: "API reference · BSV Broadcaster",
  description,
  alternates: { canonical: docsPath, types: alternateTypes },
  openGraph: {
    type: "article",
    url: docsPath,
    title: "BSV Broadcaster API reference",
    description,
    publishedTime: site.published,
    modifiedTime: site.updated,
  },
};

/** Module-level so the rail's effect does not re-run on every render. */
const sections = [
  ...groupedRoutes().map(({ group }) => ({ id: group.id, label: group.label })),
  { id: "chaintracks-api", label: "Chaintracks" },
];

/**
 * The API reference, on its own page and laid out for reading it.
 *
 * On a wide screen: a sticky table of contents on the left listing every
 * group and route, and the routes in one column beside it, with a filter that
 * stays under the header. Below `lg` the contents become the section rail.
 *
 * No reveal-on-scroll here. A reader who jumps to `#post-tx` should find it
 * already drawn, not fading in.
 */
export default function DocsPage(): ReactNode {
  return (
    <>
      <JsonLd data={docsGraph()} />

      {/* The page's own child, not wrapped: a sticky element only sticks inside its parent. */}
      <SectionBar label="API sections" sections={sections} className="lg:hidden" />

      <div className="container-page grid grid-cols-[minmax(0,1fr)] gap-10 lg:grid-cols-[14.5rem_minmax(0,1fr)] xl:gap-14">
        <aside className="hidden lg:block">
          <DocsSidebar />
        </aside>

        <div className="min-w-0 pt-8 pb-20 md:pt-10">
          <nav aria-label="Breadcrumb" className="text-sm text-ink-muted">
            <Link href="/" className="transition-colors hover:text-foreground">
              Broadcaster
            </Link>
            <span aria-hidden className="mx-2">
              /
            </span>
            <span aria-current="page" className="text-foreground">
              API reference
            </span>
          </nav>

          <header className="mt-6 max-w-3xl">
            <h1 className="headline-lg">API reference</h1>
            <p className="body-lg mt-4 text-ink-muted">
              Every route on the API server, with its request and response contract, headers
              and examples. Open a route to read it; each one has its own link. The defaults
              cover the everyday flow: check, submit, track.
            </p>
          </header>

          <StatBand
            wide
            className="mt-8"
            items={[
              { label: "API server base URL", value: <ApiBaseUrl />, aside: <ApiStatus /> },
              { label: "Content encoding", value: "UTF-8" },
              { label: "Status codes", value: "2xx · 4xx · 5xx" },
            ]}
          />

          <div className="mt-8">
            <EndpointBrowser />
          </div>

          <section
            id="chaintracks-api"
            aria-labelledby="chaintracks-heading"
            className="mt-20 scroll-mt-48 lg:scroll-mt-24"
          >
            <div className="border-b border-border pb-3">
              <p className="label-sm text-primary uppercase">Block headers</p>
              <h2 id="chaintracks-heading" className="headline-md mt-2">
                <a href="#chaintracks-api" className="hover:text-primary">
                  Chaintracks API
                </a>
              </h2>
            </div>

            <div id="chaintracks-overview" className="scroll-mt-48 lg:scroll-mt-24">
              <p className="body-md mt-5 max-w-3xl text-ink-muted">
                A separate BSV Broadcaster listener for block-header tracking. Its default local
                base URL is <C>http://localhost:8083</C>; use the Chaintracks Service or Ingress
                URL configured by your deployment rather than the API server base URL above. New
                integrations should use <C>/chaintracks/v2</C>. The <C>/chaintracks/v1</C>{" "}
                surface remains for legacy RPC-compatible clients.
              </p>

              <StatBand
                className="mt-6"
                items={[
                  { label: "Default listener", value: "http://<bsv-broadcaster-host>:8083" },
                  { label: "Availability check", value: "GET /health" },
                  { label: "Current API", value: "/chaintracks/v2" },
                ]}
              />

              <CodeBlock
                code={chaintracksCurl}
                title="chaintracks.sh"
                className="mt-6 bg-surface-raised dark:bg-code-bg"
              />
              <Note title="Operational check:" className="mt-4">
                <C>/health</C> (and its <C>/ready</C> alias) confirms that the Chaintracks
                listener is live. A successful <C>/network</C> or <C>/height</C> request confirms
                that the Chaintracks API is serving data. <C>/tip</C> is a stronger chain-state
                check: it returns a header and <C>X-Block-Height</C> once a tip is available, or{" "}
                <C>404</C> before then.
              </Note>
            </div>

            <div id="chaintracks-v2" className="mt-14 scroll-mt-48 lg:scroll-mt-24">
              <h3 className="headline-sm mb-2">Current v2 endpoints</h3>
              <p className="body-md mb-5 text-ink-muted">
                All paths below are <C>GET</C> requests relative to the Chaintracks base URL.
              </p>
              <DataTable head={["Path", "Purpose"]} rows={chaintracksV2} />
            </div>

            <div id="chaintracks-v1" className="mt-10 scroll-mt-48 lg:scroll-mt-24">
              <Disclosure
                label="Legacy v1 compatibility endpoints"
                hint="Seven RPC-style paths kept for existing clients."
              >
                <p className="body-md mb-5 max-w-3xl text-ink-muted">
                  V1 legacy RPC requests return a <C>{`{"status":"success","value":...}`}</C>{" "}
                  envelope (or an error envelope). The non-stream JSON and binary v2-style
                  endpoints above are also available when only <C>v2</C> is replaced with{" "}
                  <C>v1</C>; the two SSE endpoints are v2-only.
                </p>
                <DataTable head={["Path", "Purpose"]} rows={chaintracksV1} />
              </Disclosure>
            </div>

            <Note title="Need schemas and response details?" className="mt-10">
              The{" "}
              <a
                href={site.openapi}
                target="_blank"
                rel="noreferrer noopener"
                className="font-medium text-primary underline-offset-2 hover:underline"
              >
                OpenAPI reference
              </a>{" "}
              covers the legacy v1 RPC endpoints and most v2 operations. This page also records
              the v1 direct-path compatibility aliases, <C>/headers.bin</C>, and stored-header-file
              download paths that are not represented there.
            </Note>
          </section>
        </div>
      </div>
    </>
  );
}
