"use client";

import { useEffect, useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The section rail from bsvblockchain.org's /why-bsv: pinned under the header,
 * blurred, anchors rather than tabs so every link works on its own.
 *
 * It also marks where the reader is. The active section is the last one whose
 * top has passed the rail, which is what a reader means by "the one I'm in".
 */
export function SectionBar({
  sections,
  label,
  className,
}: {
  sections: { id: string; label: string }[];
  label: string;
  className?: string;
}): ReactNode {
  const [active, setActive] = useState<string | null>(null);

  useEffect(() => {
    let frame = 0;
    const update = (): void => {
      frame = 0;
      // Header (64px) plus this rail (~56px) plus a little air.
      const line = 140;
      let current: string | null = null;
      for (const { id } of sections) {
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
    return () => {
      if (frame) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, [sections]);

  return (
    <nav
      aria-label={label}
      className={cn(
        "sticky top-16 z-30 border-b border-border bg-background/92 backdrop-blur-md",
        className,
      )}
    >
      <div className="container-page no-scrollbar flex gap-1 overflow-x-auto py-2">
        {sections.map((section) => (
          <a
            key={section.id}
            href={`#${section.id}`}
            aria-current={active === section.id ? "location" : undefined}
            className={cn(
              "shrink-0 rounded-md px-3 py-2 text-sm font-medium transition-colors",
              active === section.id
                ? "bg-surface text-foreground"
                : "text-ink-muted hover:bg-surface hover:text-foreground",
            )}
          >
            {section.label}
          </a>
        ))}
      </div>
    </nav>
  );
}
