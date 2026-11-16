import { ArrowRight, ArrowUpRight } from "@phosphor-icons/react/dist/ssr";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Reveal } from "@/components/reveal";
import { HeroStars } from "@/components/hero-stars";
import { ApiStatus } from "@/components/api-status";
import { ApiBaseUrl } from "@/components/api-base-url";
import { Quickstart } from "@/components/quickstart";
import { StatBand } from "@/components/stat-band";
import { ButtonLink } from "@/components/ui/button";
import { ApiCta } from "@/components/api-cta";
import { FaqSection } from "@/components/faq-section";
import { JsonLd } from "@/components/json-ld";
import { homeGraph } from "@/lib/structured-data";
import { alternateTypes, docsPath, site } from "@/lib/site";

export const metadata: Metadata = {
  alternates: { canonical: "/", types: alternateTypes },
};

export default function Home(): ReactNode {
  return (
    <>
      {/* Hero: text column and the quickstart terminal over the site's blue wash, laid out as the
          BSV Faucet's hero (text and facts left, the working panel right). */}
      <section className="relative overflow-x-clip pt-10 pb-12 md:pt-16 md:pb-20">
        <div
          aria-hidden
          className="hero-glow pointer-events-none absolute -top-16 right-0 select-none"
        />

        {/* Stars behind the terminal. A sibling of the glow, outside the z-10 content layer, so
            the blend reaches the page; the grid mirrors the hero's so they sit behind the card. */}
        <div aria-hidden className="pointer-events-none absolute inset-0 hidden lg:block">
          <div className="container-page grid h-full grid-cols-[minmax(0,1fr)_minmax(0,32rem)] gap-12 pt-16">
            <span />
            <div className="relative">
              <HeroStars className="-right-64 -left-40 -top-32 h-[44rem]" />
            </div>
          </div>
        </div>

        <div className="relative z-10 container-page">
          <div className="grid grid-cols-[minmax(0,1fr)] items-start gap-12 lg:grid-cols-[minmax(0,1fr)_minmax(0,32rem)]">
            <div className="max-w-3xl lg:pt-6">
              <p className="label-sm text-primary uppercase">BSV Transaction Broadcaster</p>
              <h1 className="mt-4 display-lg">
                The world&apos;s most scalable transaction broadcast manager
              </h1>
              <p className="body-lg mt-6 max-w-2xl text-ink-muted">
                A high-throughput, callback-driven gateway for submitting BSV transactions,
                tracking their lifecycle, and streaming status updates to subscribers in real
                time.
              </p>
              <div className="mt-8 flex flex-wrap gap-3">
                <ButtonLink href={docsPath} size="lg" variant="primary">
                  API reference
                  <ArrowRight aria-hidden />
                </ButtonLink>
                <ButtonLink
                  href={site.openapi}
                  target="_blank"
                  rel="noreferrer noopener"
                  size="lg"
                  variant="secondary"
                >
                  OpenAPI reference
                  <ArrowUpRight aria-hidden />
                </ButtonLink>
              </div>

              <StatBand
                wide
                className="mt-10"
                items={[
                  { label: "API server base URL", value: <ApiBaseUrl />, aside: <ApiStatus /> },
                  { label: "Content encoding", value: "UTF-8" },
                  { label: "Status codes", value: "2xx · 4xx · 5xx" },
                ]}
              />
            </div>

            <Reveal delay={0.1}>
              <Quickstart
                className="bg-surface-raised shadow-raised dark:bg-code-bg dark:shadow-none"
              />
            </Reveal>
          </div>
        </div>
      </section>

      <ApiCta />

      <FaqSection />

      <JsonLd data={homeGraph()} />
    </>
  );
}
