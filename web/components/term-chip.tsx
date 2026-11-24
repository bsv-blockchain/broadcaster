"use client";

import { ArrowRight, ArrowUpRight } from "@phosphor-icons/react";
import Link from "next/link";
import type { ReactNode } from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { lookup } from "@/lib/glossary";
import { cn } from "@/lib/utils";

const chip =
  "rounded-sm bg-info-bg px-1 py-px font-mono text-[0.875em] whitespace-nowrap text-primary";

/**
 * A code term in running copy. When the glossary knows it, it is a link to
 * where it is documented, with a one-sentence tooltip on hover or focus; the
 * dotted underline is the hint that it goes somewhere. Unknown terms stay
 * plain code.
 */
export function TermChip({ term }: { term: string }): ReactNode {
  const entry = lookup(term);
  if (!entry) return <code className={chip}>{term}</code>;

  const Icon = entry.external ? ArrowUpRight : ArrowRight;
  const className = cn(
    chip,
    "underline decoration-primary/40 decoration-dotted underline-offset-[3px] transition-colors hover:bg-info-bd/40 hover:decoration-primary focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
  );

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        {entry.external ? (
          <a href={entry.href} target="_blank" rel="noreferrer noopener" className={className}>
            <code>{term}</code>
          </a>
        ) : (
          <Link href={entry.href} className={className}>
            <code>{term}</code>
          </Link>
        )}
      </TooltipTrigger>
      <TooltipContent
        side="top"
        sideOffset={6}
        collisionPadding={12}
        className="max-w-[min(18rem,calc(100vw-1.5rem))] px-3.5 py-2.5 text-start text-pretty"
      >
        <span className="block leading-relaxed">{entry.tip}</span>
        <span className="mt-1.5 flex items-center gap-1 opacity-70">
          {entry.external ? "Opens" : "Go to"} {entry.where}
          <Icon aria-hidden className="size-3" />
        </span>
      </TooltipContent>
    </Tooltip>
  );
}
