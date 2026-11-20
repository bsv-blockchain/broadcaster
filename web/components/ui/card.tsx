import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The primary containment unit (as on bsvblockchain.org).
 * Depth comes from tonal layering plus a 1px stroke, not shadow.
 */
export function Card({ className, ...props }: ComponentProps<"div">): ReactNode {
  return (
    <div
      className={cn("rounded-lg border border-border bg-surface-raised p-6", className)}
      {...props}
    />
  );
}
