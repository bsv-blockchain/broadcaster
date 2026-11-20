"use client";

import { useEffect } from "react";

/**
 * Tracks the pointer across anything marked `data-glow` and writes its position
 * into `--pointer-x` / `--pointer-y` on that element, which the button surface
 * reads for its highlight (see the button block in globals.css).
 *
 * One delegated listener for the whole document rather than a listener per
 * button, so a page with forty buttons costs the same as a page with one.
 *
 * Pointer events rather than mouse events, which is what makes this work on a
 * phone: `pointerdown` fires on tap, so the highlight lands where the finger
 * lands, and a drag across the control tracks it.
 *
 * Writes are coalesced into one animation frame. Without that, a fast mouse
 * fires often enough to queue redundant style writes, and this element repaints
 * a gradient on every one.
 */
export function PointerGlow(): null {
  useEffect(() => {
    // The highlight is decorative; readers who asked for less motion get the
    // static gradient and no per-frame paint at all.
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    let frame = 0;
    let pending: { target: HTMLElement; x: number; y: number } | null = null;

    const flush = (): void => {
      frame = 0;
      if (!pending) return;
      const { target, x, y } = pending;
      target.style.setProperty("--pointer-x", `${x}px`);
      target.style.setProperty("--pointer-y", `${y}px`);
      pending = null;
    };

    const onPointer = (event: PointerEvent): void => {
      const target = (event.target as HTMLElement | null)?.closest<HTMLElement>(
        "[data-glow]",
      );
      if (!target) return;

      const rect = target.getBoundingClientRect();
      pending = {
        target,
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      };

      if (!frame) frame = requestAnimationFrame(flush);
    };

    // Leaving returns the highlight to centre, so the next approach does not
    // start from wherever the pointer happened to exit.
    const onLeave = (event: PointerEvent): void => {
      const target = (event.target as HTMLElement | null)?.closest<HTMLElement>(
        "[data-glow]",
      );
      if (!target) return;
      target.style.removeProperty("--pointer-x");
      target.style.removeProperty("--pointer-y");
    };

    document.addEventListener("pointermove", onPointer, { passive: true });
    document.addEventListener("pointerdown", onPointer, { passive: true });
    document.addEventListener("pointerout", onLeave, { passive: true });

    return () => {
      if (frame) cancelAnimationFrame(frame);
      document.removeEventListener("pointermove", onPointer);
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("pointerout", onLeave);
    };
  }, []);

  return null;
}
