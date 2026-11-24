import { ArrowUpRight } from "@phosphor-icons/react/dist/ssr";
import Link from "next/link";
import type { ReactNode } from "react";
import { Logo } from "@/components/brand/logo";
import { nav, site } from "@/lib/site";

const resources = [
  { label: "Source code", href: site.repo },
  { label: "OpenAPI reference", href: site.openapi },
  { label: "bsvblockchain.org", href: site.parent },
];

/** The bsvblockchain.org footer layout: surface band, logo and claim, link columns, legal row. */
export function SiteFooter(): ReactNode {
  return (
    <footer className="border-t border-border bg-surface">
      <div className="container-page py-14 md:py-20">
        <div className="grid gap-12 lg:grid-cols-[minmax(0,20rem)_minmax(0,1fr)] lg:gap-20">
          <div>
            <Link href="/" aria-label={`${site.name}, home`}>
              <Logo />
            </Link>
            <p className="body-md mt-4 max-w-xs text-ink-muted">
              Built for scale. BSV Broadcaster powers BSV transaction broadcasts on the world&apos;s
              most performant ledger.
            </p>
          </div>

          <nav aria-label="Footer" className="grid grid-cols-2 gap-8 sm:grid-cols-3">
            <div>
              <p className="label-sm mb-4 text-ink-muted uppercase">Site</p>
              <ul className="space-y-2.5">
                {nav.map((link) => (
                  <li key={link.href}>
                    <Link
                      href={link.href}
                      className="text-sm text-ink-muted transition-colors hover:text-foreground"
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <p className="label-sm mb-4 text-ink-muted uppercase">Resources</p>
              <ul className="space-y-2.5">
                {resources.map((link) => (
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
          </nav>
        </div>

        <div className="mt-14 border-t border-border pt-8">
          <p className="text-sm text-ink-muted">
            © {site.copyrightYear} {site.association}. A Swiss non-profit association.
          </p>
        </div>
      </div>
    </footer>
  );
}
