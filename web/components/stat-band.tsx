import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Facts in one joined panel, as the BSV Faucet's hero SpecStrip (after
 * bsvblockchain.org's StatBand): cells share hairline dividers over a
 * border-coloured ground, label over a medium-weight value.
 */
export function StatBand({
  items,
  wide = false,
  className,
}: {
  /** `aside` sits at the right, centred across the label and value rows, e.g. a live status. */
  items: { label: string; value: ReactNode; aside?: ReactNode }[];
  /** The first fact gets a full row, for a value too long to share one (an address). */
  wide?: boolean;
  className?: string;
}): ReactNode {
  return (
    <dl
      className={cn(
        wide
          ? "grid gap-px overflow-hidden rounded-lg bg-border sm:grid-cols-2 [&>*:first-child]:sm:col-span-2"
          : "grid gap-px overflow-hidden rounded-lg bg-border sm:grid-cols-3",
        className,
      )}
    >
      {items.map((item) => (
        <div
          key={item.label}
          className={cn(
            "bg-surface-raised p-4 md:p-5",
            // With an aside, a second column spanning the label and value rows, centred on both.
            item.aside && "grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4",
          )}
        >
          <dt className="label-sm text-ink-muted uppercase">{item.label}</dt>
          <dd className="tnum mt-2 text-sm leading-snug font-medium [overflow-wrap:anywhere] text-balance text-foreground">
            {item.value}
          </dd>
          {item.aside ? (
            <div className="col-start-2 row-span-2 row-start-1 self-center">{item.aside}</div>
          ) : null}
        </div>
      ))}
    </dl>
  );
}
