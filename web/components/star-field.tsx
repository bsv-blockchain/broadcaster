"use client";

import { type ReactNode, useEffect, useRef } from "react";
import { cn } from "@/lib/utils";

/**
 * A field of short streaks rising from a point below the canvas and fanning
 * out as they climb, fading in and out over their life. Drawn on a transparent
 * 2D canvas, so it composites over the page without blend-mode tricks: `glow`
 * adds light (for dark backgrounds), `ink` lays down translucent colour (for
 * light ones).
 *
 * Plain Canvas 2D, no WebGL and no dependencies. It runs only while on screen
 * and the tab is visible, and under reduced motion it paints one still frame.
 */
export type StarFieldProps = {
  /** Two sRGB hex colours; each streak takes a mix of the two. */
  colors: [string, string];
  mode: "glow" | "ink";
  /** Streaks alive at once. */
  count?: number;
  /** Half-angle of the fan, in degrees from vertical. */
  spread?: number;
  /** Rise speed, as fractions of the canvas height per second. */
  speed?: number;
  /** Trail length, as a fraction of the distance travelled per second. */
  trail?: number;
  className?: string;
};

type Star = {
  x: number;
  y: number;
  vx: number;
  vy: number;
  age: number;
  life: number;
  width: number;
  mix: number;
};

function hexToRgb(hex: string): [number, number, number] {
  const n = Number.parseInt(hex.replace("#", ""), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

/** Deterministic PRNG, so the still frame under reduced motion is stable. */
function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function StarField({
  colors,
  mode,
  count = 260,
  spread = 55,
  speed = 0.16,
  trail = 0.22,
  className,
}: StarFieldProps): ReactNode {
  const ref = useRef<HTMLCanvasElement>(null);
  const [from, to] = colors;

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext("2d");
    if (!canvas || !ctx) return;

    const random = mulberry32(0x5eed);
    const a = hexToRgb(from);
    const b = hexToRgb(to);
    const half = (spread * Math.PI) / 180;
    let width = 0;
    let height = 0;

    const spawn = (star: Partial<Star> = {}, fresh = false): Star => {
      const angle = (random() * 2 - 1) * half;
      const v = speed * (0.55 + random() * 0.9);
      const life = 2.2 + random() * 3.2;
      return Object.assign(star, {
        // Origin: a soft point below the bottom centre.
        x: 0.5 + (random() - 0.5) * 0.12,
        y: 1.08 + random() * 0.06,
        vx: Math.sin(angle) * v,
        vy: -Math.cos(angle) * v,
        age: fresh ? random() * life : 0,
        life,
        width: 0.6 + random() * 1.4,
        mix: random(),
      }) as Star;
    };

    const stars: Star[] = Array.from({ length: count }, () => spawn({}, true));
    // Start mid-flight rather than all at the origin.
    for (const s of stars) {
      s.x += s.vx * s.age;
      s.y += s.vy * s.age;
    }

    const resize = (): void => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const rect = canvas.getBoundingClientRect();
      width = rect.width;
      height = rect.height;
      canvas.width = Math.max(1, Math.round(width * dpr));
      canvas.height = Math.max(1, Math.round(height * dpr));
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const draw = (): void => {
      ctx.clearRect(0, 0, width, height);
      ctx.globalCompositeOperation = mode === "glow" ? "lighter" : "source-over";
      ctx.lineCap = "round";
      for (const s of stars) {
        const t = s.age / s.life;
        const fade = Math.sin(Math.PI * Math.min(1, Math.max(0, t)));
        if (fade <= 0.01) continue;
        const alpha = (mode === "glow" ? 0.85 : 0.5) * fade;
        const r = Math.round(a[0] + (b[0] - a[0]) * s.mix);
        const g = Math.round(a[1] + (b[1] - a[1]) * s.mix);
        const bl = Math.round(a[2] + (b[2] - a[2]) * s.mix);
        const x = s.x * width;
        const y = s.y * height;
        const tx = x - s.vx * trail * height;
        const ty = y - s.vy * trail * height;
        const gradient = ctx.createLinearGradient(tx, ty, x, y);
        gradient.addColorStop(0, `rgba(${r},${g},${bl},0)`);
        gradient.addColorStop(1, `rgba(${r},${g},${bl},${alpha})`);
        ctx.strokeStyle = gradient;
        ctx.lineWidth = s.width;
        ctx.beginPath();
        ctx.moveTo(tx, ty);
        ctx.lineTo(x, y);
        ctx.stroke();
        // The head: a brighter dot.
        ctx.fillStyle = `rgba(${r},${g},${bl},${Math.min(1, alpha * 1.2)})`;
        ctx.beginPath();
        ctx.arc(x, y, s.width * 0.9, 0, Math.PI * 2);
        ctx.fill();
      }
    };

    const step = (dt: number): void => {
      for (const s of stars) {
        s.age += dt;
        s.x += s.vx * dt;
        s.y += s.vy * dt;
        if (s.age >= s.life || s.y < -0.1) spawn(s);
      }
    };

    resize();
    const still = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (still) {
      draw();
      return;
    }

    let frame = 0;
    let last = 0;
    let onScreen = true;
    const loop = (now: number): void => {
      const dt = last ? Math.min(0.05, (now - last) / 1000) : 0;
      last = now;
      step(dt);
      draw();
      frame = requestAnimationFrame(loop);
    };
    const start = (): void => {
      if (frame || !onScreen || document.visibilityState !== "visible") return;
      last = 0;
      frame = requestAnimationFrame(loop);
    };
    const stop = (): void => {
      cancelAnimationFrame(frame);
      frame = 0;
    };
    const visibility = (): void => (document.visibilityState === "visible" ? start() : stop());

    const observer = new IntersectionObserver(([entry]) => {
      onScreen = entry?.isIntersecting ?? false;
      if (onScreen) start();
      else stop();
    });
    observer.observe(canvas);
    const sized = new ResizeObserver(() => {
      resize();
      if (!frame) draw();
    });
    sized.observe(canvas);
    document.addEventListener("visibilitychange", visibility);
    start();

    return () => {
      stop();
      observer.disconnect();
      sized.disconnect();
      document.removeEventListener("visibilitychange", visibility);
    };
  }, [from, to, mode, count, spread, speed, trail]);

  return <canvas ref={ref} aria-hidden className={cn("block h-full w-full", className)} />;
}
