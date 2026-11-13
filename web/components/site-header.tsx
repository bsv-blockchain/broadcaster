"use client";

import { ArrowUpRight } from "@phosphor-icons/react/dist/ssr";
import { AnimatePresence, motion } from "motion/react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState, type ReactNode } from "react";
import { Logo } from "@/components/brand/logo";
import { ThemeToggle } from "@/components/theme-toggle";
import { ButtonLink } from "@/components/ui/button";
import { duration, easeOut } from "@/lib/motion";
import { isCurrent, nav, site } from "@/lib/site";
import { cn } from "@/lib/utils";

/**
 * The bsvblockchain.org header, reduced to what a docs page needs: the
 * official wordmark with the product name, in-page links, the icon-only theme
 * switch and one action. Transparent over the hero glow until the page
 * scrolls, then a blurred bar with a hairline.
 *
 * Below `md` the links move into the reference's full-screen menu, opened by
 * three CSS lines that morph into an X.
 */
export function SiteHeader(): ReactNode {
  const [scrolled, setScrolled] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const pathname = usePathname();

  useEffect(() => {
    const onScroll = (): void => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    document.body.style.overflow = menuOpen ? "hidden" : "";
    if (!menuOpen) return;
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === "Escape") setMenuOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => {
      document.body.style.overflow = "";
      window.removeEventListener("keydown", onKey);
    };
  }, [menuOpen]);

  return (
    <>
      <header
        className={cn(
          "sticky top-0 z-50 transition-colors duration-200",
          scrolled || menuOpen
            ? "border-b border-border bg-background/92 backdrop-blur-md"
            : "border-b border-transparent",
        )}
      >
        <div className="container-page flex h-16 items-center gap-4">
          <Link
            href="/"
            onClick={() => setMenuOpen(false)}
            className="flex min-w-0 items-center gap-3 rounded-sm"
            aria-label={`${site.name}, home`}
          >
            {/* The faucet's lockup: wordmark, hairline, product name in the display face. */}
            <Logo className="h-auto w-full max-w-36 min-[480px]:h-[0.9375rem] min-[480px]:w-auto min-[480px]:max-w-none" />
            <span aria-hidden className="h-5 w-px flex-none bg-border" />
            <span className="font-display text-[1.0625rem] font-semibold tracking-[-0.02em] whitespace-nowrap">
              Broadcaster
            </span>
          </Link>

          <nav aria-label="Main" className="ms-auto hidden items-center gap-0.5 md:flex">
            {nav.map((item) => {
              const current = isCurrent(item.href, pathname);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={current ? "page" : undefined}
                  className={cn(
                    "rounded-md px-2.5 py-2 text-sm font-medium whitespace-nowrap transition-colors hover:text-foreground",
                    current ? "text-foreground" : "text-ink-muted",
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="ms-auto flex items-center gap-1 md:ms-2 md:gap-2">
            <ThemeToggle />
            <ButtonLink
              href={site.repo}
              target="_blank"
              rel="noreferrer noopener"
              size="sm"
              variant="primary"
              className="hidden sm:inline-flex"
            >
              View on GitHub
              <ArrowUpRight aria-hidden />
            </ButtonLink>
            <MenuToggle open={menuOpen} onToggle={() => setMenuOpen((v) => !v)} />
          </div>
        </div>
      </header>

      {/* A sibling of <header>, not a child: the header's backdrop-filter would
          otherwise become the containing block for this fixed overlay. */}
      <MobileMenu open={menuOpen} pathname={pathname} onClose={() => setMenuOpen(false)} />
    </>
  );
}

/** Three lines drawn in CSS, so the morph to an X is a pure transform. */
function MenuToggle({ open, onToggle }: { open: boolean; onToggle: () => void }): ReactNode {
  return (
    <button
      type="button"
      onClick={onToggle}
      aria-expanded={open}
      aria-controls="mobile-menu"
      aria-label={open ? "Close menu" : "Open menu"}
      className="-me-2 inline-flex size-11 items-center justify-center rounded-md text-foreground md:hidden"
    >
      <span className="relative block h-3.5 w-6">
        <motion.span
          className="absolute inset-x-0 top-0 block h-0.5 origin-center rounded-full bg-current"
          animate={open ? { rotate: 45, y: 6 } : { rotate: 0, y: 0 }}
          transition={{ duration: duration.fast, ease: easeOut }}
        />
        <motion.span
          className="absolute inset-x-0 top-1.5 block h-0.5 rounded-full bg-current"
          animate={{ opacity: open ? 0 : 1 }}
          transition={{ duration: duration.fast }}
        />
        <motion.span
          className="absolute inset-x-0 bottom-0 block h-0.5 origin-center rounded-full bg-current"
          animate={open ? { rotate: -45, y: -6 } : { rotate: 0, y: 0 }}
          transition={{ duration: duration.fast, ease: easeOut }}
        />
      </span>
    </button>
  );
}

/** Full-screen on the page background, options centred in `headline-md`, as DESIGN.md specifies. */
function MobileMenu({
  open,
  pathname,
  onClose,
}: {
  open: boolean;
  pathname: string;
  onClose: () => void;
}): ReactNode {
  return (
    <AnimatePresence>
      {open ? (
        <motion.div
          id="mobile-menu"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: duration.base, ease: easeOut }}
          className="fixed inset-x-0 top-16 bottom-0 z-40 overflow-y-auto bg-background md:hidden"
        >
          <nav
            aria-label="Mobile"
            className="flex min-h-full flex-col justify-center px-6 py-10"
          >
            <ul className="mx-auto w-full max-w-sm space-y-1 text-center">
              {nav.map((item) => (
                <li key={item.href}>
                  <Link
                    href={item.href}
                    onClick={onClose}
                    aria-current={isCurrent(item.href, pathname) ? "page" : undefined}
                    className={cn(
                      "inline-flex min-h-11 w-full items-center justify-center py-2 headline-md",
                      isCurrent(item.href, pathname) && "text-primary",
                    )}
                  >
                    {item.label}
                  </Link>
                </li>
              ))}
            </ul>

            <div className="mx-auto mt-10 flex w-full max-w-sm flex-col gap-2">
              <ButtonLink href={site.repo} target="_blank" rel="noreferrer noopener" className="w-full">
                View on GitHub
                <ArrowUpRight aria-hidden />
              </ButtonLink>
              <ButtonLink
                href={site.openapi}
                target="_blank"
                rel="noreferrer noopener"
                variant="secondary"
                className="w-full"
              >
                OpenAPI reference
                <ArrowUpRight aria-hidden />
              </ButtonLink>
            </div>
          </nav>
        </motion.div>
      ) : null}
    </AnimatePresence>
  );
}
