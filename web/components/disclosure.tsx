"use client";

import { CaretDown } from "@phosphor-icons/react";
import { type ReactNode, useId, useState } from "react";
import { cn } from "@/lib/utils";

/**
 * A labelled show and hide, for evidence that has earned its place on a page but
 * not the top of it.
 *
 * The closed panel keeps its children in the document rather than mounting them
 * on first open. That costs a little markup on a page nobody may expand, and it
 * buys the thing this site cares about more: the numbers are in the HTML, so they
 * are in the print, in the page source and in anything reading the page that
 * never clicks. A disclosure that empties the page when closed is a way of
 * publishing a figure without publishing it.
 *
 * The label says what is inside, never "more" or "details". A reader deciding
 * whether to spend a click deserves to know what the click buys.
 */
export function Disclosure({
  label,
  hint,
  defaultOpen = false,
  className,
  children,
}: {
  label: string;
  /** What is inside, in a few words. Sits next to the label, dimmer. */
  hint?: string;
  defaultOpen?: boolean;
  className?: string;
  children: ReactNode;
}): ReactNode {
  const [open, setOpen] = useState(defaultOpen);
  const panelId = useId();

  return (
    <div className={className}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        aria-controls={panelId}
        className={cn(
          "flex w-full items-center gap-3 rounded-lg border bg-surface-raised px-5 py-4 text-start transition-colors",
          "focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
          open ? "border-primary-edge" : "border-border hover:border-info-bd",
        )}
      >
        <CaretDown
          aria-hidden
          className={cn(
            "size-4 shrink-0 text-ink-muted transition-transform duration-200",
            open && "rotate-180",
          )}
        />
        <span className="min-w-0">
          <span className="block text-sm font-medium">{label}</span>
          {hint ? <span className="mt-0.5 block text-sm text-ink-muted">{hint}</span> : null}
        </span>
      </button>

      <div id={panelId} hidden={!open} className="pt-6">
        {children}
      </div>
    </div>
  );
}
