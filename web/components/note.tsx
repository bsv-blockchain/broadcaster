import { Info } from "@phosphor-icons/react/dist/ssr";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * A callout in the BSV Faucet's blue info style (its InfoBand): sRGB primary
 * tint, matching hairline, foreground text. Static, so an icon stands where
 * the faucet's live status uses a pulsing dot.
 */
export function Note({
  title,
  children,
  className,
}: {
  title?: string;
  children: ReactNode;
  className?: string;
}): ReactNode {
  return (
    <aside
      className={cn(
        "flex max-w-3xl gap-2.5 rounded-md border border-info-bd bg-info-bg p-3 text-sm leading-relaxed text-foreground",
        className,
      )}
    >
      <Info aria-hidden weight="fill" className="mt-0.5 size-4 shrink-0 text-primary" />
      <p className="min-w-0">
        {title ? <span className="font-medium">{title} </span> : null}
        {children}
      </p>
    </aside>
  );
}
