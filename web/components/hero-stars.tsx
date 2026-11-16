"use client";

import { useTheme } from "next-themes";
import { useSyncExternalStore, type ReactNode } from "react";
import { StarField } from "@/components/star-field";
import { cn } from "@/lib/utils";

/**
 * Streaks rising behind the hero's quickstart terminal: transactions leaving
 * for the network, drawn as weather rather than as a diagram.
 *
 * The canvas is transparent, so it needs no blend mode: in dark mode the
 * streaks add light over the navy (blue to cyan), in light mode they lay down
 * translucent brand ink (blue to navy). A radial mask dissolves the field
 * before its edges.
 */

const WIDE = "(min-width: 1024px)";

function subscribeWide(notify: () => void): () => void {
  const media = window.matchMedia(WIDE);
  media.addEventListener("change", notify);
  return () => media.removeEventListener("change", notify);
}

/** Desktop only: below lg the terminal stacks under the copy with no gutter around it. */
export function HeroStars({ className }: { className?: string }): ReactNode {
  const { resolvedTheme } = useTheme();
  const wide = useSyncExternalStore(
    subscribeWide,
    () => window.matchMedia(WIDE).matches,
    () => false,
  );

  if (!wide || !resolvedTheme) return null;
  const dark = resolvedTheme === "dark";

  return (
    <div
      aria-hidden
      className={cn("pointer-events-none absolute select-none", className)}
      style={{
        maskImage: "radial-gradient(closest-side, #000 55%, transparent 100%)",
        WebkitMaskImage: "radial-gradient(closest-side, #000 55%, transparent 100%)",
      }}
    >
      <StarField
        key={dark ? "dark" : "light"}
        mode={dark ? "glow" : "ink"}
        colors={dark ? ["#003FFF", "#00E6FF"] : ["#003FFF", "#1B1EA9"]}
      />
    </div>
  );
}
