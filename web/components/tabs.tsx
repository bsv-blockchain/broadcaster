"use client";

import { useId, useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The BSV Faucet's segmented control: one inset track holding every option,
 * the active segment wearing the primary button surface, the rest flat until
 * hovered. Every panel stays in the document; inactive ones are hidden.
 */
export function Tabs({
  items,
}: {
  items: { label: string; content: ReactNode }[];
}): ReactNode {
  const [active, setActive] = useState(0);
  const id = useId();

  return (
    <div>
      <div
        role="tablist"
        className="no-scrollbar mb-3 inline-flex max-w-full gap-1 overflow-x-auto rounded-lg border border-border bg-surface p-1"
      >
        {items.map((item, i) => (
          <button
            key={item.label}
            type="button"
            role="tab"
            id={`${id}-tab-${i}`}
            aria-selected={active === i}
            aria-controls={`${id}-panel-${i}`}
            onClick={() => setActive(i)}
            className={cn(
              "flex min-h-10 items-center justify-center rounded-md px-3 py-1.5 font-mono text-sm font-medium leading-tight whitespace-nowrap outline-none transition-[color,background-color,filter,box-shadow] duration-150",
              "focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-surface",
              active === i
                ? "btn-surface btn-primary-surface text-primary-foreground"
                : "text-ink-muted hover:bg-surface-raised hover:text-foreground",
            )}
          >
            {item.label}
          </button>
        ))}
      </div>
      {items.map((item, i) => (
        <div
          key={item.label}
          role="tabpanel"
          id={`${id}-panel-${i}`}
          aria-labelledby={`${id}-tab-${i}`}
          hidden={active !== i}
        >
          {item.content}
        </div>
      ))}
    </div>
  );
}
