"use client";

import { useSyncExternalStore } from "react";

/**
 * Motion primitives shared by every animated component.
 *
 * House rules (DESIGN.md): only `transform` and `opacity` animate, entering
 * uses ease-out, moving in place uses ease-in-out, and nothing animates at
 * rest. Durations are the three named steps, never ad-hoc numbers.
 */

export const easeOut = [0.16, 1, 0.3, 1] as const;
export const easeInOut = [0.65, 0, 0.35, 1] as const;
/** Long, unhurried ease reserved for the hero entrance. */
export const easeOutExpo = [0.22, 1, 0.36, 1] as const;

export const duration = {
  fast: 0.15,
  base: 0.25,
  slow: 0.4,
} as const;

function subscribe(callback: () => void): () => void {
  const query = window.matchMedia("(prefers-reduced-motion: reduce)");
  query.addEventListener("change", callback);
  return () => query.removeEventListener("change", callback);
}

export function useReducedMotion(): boolean {
  return useSyncExternalStore(
    subscribe,
    () => window.matchMedia("(prefers-reduced-motion: reduce)").matches,
    () => false,
  );
}

/** Enter-on-scroll defaults, applied through `whileInView`. */
export const riseIn = {
  hidden: { opacity: 0, y: 18 },
  visible: { opacity: 1, y: 0 },
};

export const fadeIn = {
  hidden: { opacity: 0 },
  visible: { opacity: 1 },
};

export const staggerChildren = (stagger = 0.06, delayChildren = 0) => ({
  hidden: {},
  visible: { transition: { staggerChildren: stagger, delayChildren } },
});

/** Viewport config used by every scroll-triggered section, so the whole page
 *  reveals at one consistent trigger point. */
export const viewportOnce = { once: true, amount: 0.25 } as const;
