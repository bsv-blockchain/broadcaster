"use client";

import type { ReactNode } from "react";
import { useReducedMotion } from "@/lib/motion";
import { cn } from "@/lib/utils";

/**
 * The API in one loop: what a client calls on the left, Broadcaster in the
 * middle, what the client hears back on the right.
 *
 * A request leaves `POST /tx`, reaches Broadcaster, and the statuses come back
 * in order: RECEIVED, SEEN_ON_NETWORK, MINED. MINED lights "SSE · webhook",
 * because MINED is the status every subscriber gets, webhooks included. A
 * subscriber opens `GET /events` first and a batch arrives on `POST /txs` in
 * between, so the hub never looks idle.
 *
 * Everything runs on one SVG timeline (SMIL), so the packets and the chips they
 * light cannot drift apart. Colours are theme tokens through Tailwind's fill and
 * stroke utilities, so it follows light and dark mode. Under reduced motion it
 * is a still picture with every status lit.
 */

/** One loop, in seconds. */
const LOOP = 7;

/** A moment on the loop, as the 0–1 fraction SMIL keyTimes want. */
const at = (seconds: number): number => Math.min(1, Math.max(0, seconds / LOOP));
const times = (...seconds: number[]): string => seconds.map((s) => at(s).toFixed(4)).join(";");

const PATHS = {
  inTx: "M170 110 C 200 110, 196 182, 222 186",
  inTxs: "M170 190 L 222 190",
  inEvents: "M170 270 C 200 270, 196 198, 222 194",
  outReceived: "M298 186 C 324 182, 320 110, 350 110",
  outSeen: "M298 190 L 350 190",
  outMined: "M298 194 C 324 198, 320 270, 350 270",
} as const;

type PathKey = keyof typeof PATHS;

/** A packet that travels `path` between `from` and `to` seconds, then vanishes. */
function Packet({ path, from, to, tone = "primary" }: { path: PathKey; from: number; to: number; tone?: "primary" | "pos" }): ReactNode {
  const fill = tone === "pos" ? "fill-pos" : "fill-primary";
  return (
    <g opacity={0}>
      <circle r={10} className={cn(fill, "opacity-20")} />
      <circle r={4.5} className={fill} />
      <animateMotion
        dur={`${LOOP}s`}
        repeatCount="indefinite"
        calcMode="linear"
        keyPoints="0;0;1;1"
        keyTimes={times(0, from, to, LOOP)}
        path={PATHS[path]}
      />
      <animate
        attributeName="opacity"
        dur={`${LOOP}s`}
        repeatCount="indefinite"
        values="0;0;1;1;0;0"
        keyTimes={times(0, from, from + 0.08, to, to + 0.12, LOOP)}
      />
    </g>
  );
}

/** Fades a layer in at `on` and holds it until the loop resets. */
function Hold({ on, off = LOOP - 0.5 }: { on: number; off?: number }): ReactNode {
  return (
    <animate
      attributeName="opacity"
      dur={`${LOOP}s`}
      repeatCount="indefinite"
      values="0;0;1;1;0;0"
      keyTimes={times(0, on, on + 0.25, off, off + 0.4, LOOP)}
    />
  );
}

function Request({
  y,
  method,
  path,
  lit,
  animated,
}: {
  y: number;
  method: "GET" | "POST";
  path: string;
  lit: [number, number];
  animated: boolean;
}): ReactNode {
  return (
    <g>
      <rect x={20} y={y} width={150} height={40} rx={9} className="fill-surface-raised stroke-border" strokeWidth={1.25} />
      <rect
        x={20}
        y={y}
        width={150}
        height={40}
        rx={9}
        className="fill-info-bg stroke-primary-edge"
        strokeWidth={1.25}
        opacity={0}
      >
        {animated ? <Hold on={lit[0]} off={lit[1]} /> : null}
      </rect>
      <text
        x={34}
        y={y + 24.5}
        className={cn("font-mono text-[11px] font-semibold", method === "POST" ? "fill-pos" : "fill-primary")}
      >
        {method}
      </text>
      <text x={method === "POST" ? 74 : 66} y={y + 24.5} className="fill-foreground font-mono text-[12px] font-medium">
        {path}
      </text>
    </g>
  );
}

function Status({
  y,
  label,
  on,
  tone = "primary",
  animated,
}: {
  y: number;
  label: string;
  on: number;
  tone?: "primary" | "pos";
  animated: boolean;
}): ReactNode {
  const pos = tone === "pos";
  return (
    <g>
      <rect x={350} y={y} width={150} height={34} rx={17} className="fill-surface-raised stroke-border" strokeWidth={1.25} />
      <g opacity={animated ? 0 : 1}>
        {animated ? <Hold on={on} /> : null}
        <rect
          x={350}
          y={y}
          width={150}
          height={34}
          rx={17}
          className={pos ? "fill-pos-bg stroke-pos" : "fill-info-bg stroke-primary-edge"}
          strokeWidth={1.25}
        />
      </g>
      <circle cx={367} cy={y + 17} r={3.5} className="fill-border" />
      <circle cx={367} cy={y + 17} r={3.5} className={pos ? "fill-pos" : "fill-primary"} opacity={animated ? 0 : 1}>
        {animated ? <Hold on={on} /> : null}
      </circle>
      <text x={379} y={y + 21} className="fill-foreground font-mono text-[11px] font-medium">
        {label}
      </text>
    </g>
  );
}

export function ApiIllustration({ className }: { className?: string }): ReactNode {
  const reduced = useReducedMotion();
  const animated = !reduced;

  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-xl border border-border bg-surface-raised shadow-raised dark:shadow-none",
        className,
      )}
    >
      {/* Dot grid, and a primary wash behind the hub. */}
      <div
        aria-hidden
        className="absolute inset-0 opacity-70"
        style={{
          backgroundImage: "radial-gradient(var(--border) 1px, transparent 1.2px)",
          backgroundSize: "18px 18px",
          maskImage: "radial-gradient(ellipse 80% 75% at 50% 50%, #000 40%, transparent 100%)",
          WebkitMaskImage: "radial-gradient(ellipse 80% 75% at 50% 50%, #000 40%, transparent 100%)",
        }}
      />
      <div
        aria-hidden
        className="absolute inset-0"
        style={{
          backgroundImage:
            "radial-gradient(closest-side at 50% 50%, color-mix(in oklch, var(--primary) 16%, transparent), transparent 70%)",
        }}
      />

      <svg
        viewBox="0 0 520 360"
        role="img"
        aria-label="A client calls POST /tx; Broadcaster answers with RECEIVED, SEEN_ON_NETWORK and MINED, pushed over SSE and webhooks."
        className="relative block h-auto w-full"
      >
        <text x={20} y={66} className="fill-ink-muted text-[10px] font-semibold tracking-[0.12em]">
          YOU CALL
        </text>
        <text x={350} y={66} className="fill-ink-muted text-[10px] font-semibold tracking-[0.12em]">
          YOU HEAR BACK
        </text>

        {/* Wires: a quiet base line, and a slow dashed current over it. */}
        {(Object.keys(PATHS) as PathKey[]).map((key) => (
          <g key={key}>
            <path d={PATHS[key]} fill="none" className="stroke-border" strokeWidth={1.5} />
            <path
              d={PATHS[key]}
              fill="none"
              className="stroke-primary opacity-35"
              strokeWidth={1.5}
              strokeDasharray="3 7"
              strokeLinecap="round"
            >
              {animated ? (
                <animate attributeName="stroke-dashoffset" from="20" to="0" dur="1.4s" repeatCount="indefinite" />
              ) : null}
            </path>
          </g>
        ))}

        <Request y={90} method="POST" path="/tx" lit={[0, 1.3]} animated={animated} />
        <Request y={170} method="POST" path="/txs" lit={[2.0, 3.0]} animated={animated} />
        <Request y={250} method="GET" path="/events" lit={[0.4, 1.6]} animated={animated} />

        {/* Broadcaster: a rotating dashed orbit, a pulse, and the broadcast mark. */}
        <g transform="translate(260 190)">
          {animated ? (
            <circle r={40} fill="none" className="stroke-primary" strokeWidth={1.5}>
              <animate attributeName="r" values="40;68" dur="2.2s" repeatCount="indefinite" />
              <animate attributeName="opacity" values="0.45;0" dur="2.2s" repeatCount="indefinite" />
            </circle>
          ) : null}
          <circle r={56} fill="none" className="stroke-primary-edge" strokeWidth={1.25} strokeDasharray="2 6" strokeLinecap="round">
            {animated ? (
              <animateTransform attributeName="transform" type="rotate" from="0" to="360" dur="24s" repeatCount="indefinite" />
            ) : null}
          </circle>
          <rect x={-38} y={-38} width={76} height={76} rx={20} className="fill-primary" />
          <rect x={-38} y={-38} width={76} height={38} rx={20} className="fill-white opacity-10" />
          <g fill="none" stroke="white" strokeWidth={2.75} strokeLinecap="round">
            <path d="M -11 -11 A 15.5 15.5 0 0 0 -11 11" />
            <path d="M 11 -11 A 15.5 15.5 0 0 1 11 11" />
            <path d="M -18.5 -18.5 A 26 26 0 0 0 -18.5 18.5" opacity={0.6} />
            <path d="M 18.5 -18.5 A 26 26 0 0 1 18.5 18.5" opacity={0.6} />
          </g>
          <circle r={4.5} fill="white" />
          <text y={66} textAnchor="middle" className="fill-foreground text-[12px] font-semibold">
            Broadcaster
          </text>
        </g>

        <Status y={93} label="RECEIVED" on={1.75} animated={animated} />
        <Status y={173} label="SEEN_ON_NETWORK" on={3.15} animated={animated} />
        <Status y={253} label="MINED" on={4.55} tone="pos" animated={animated} />

        <g opacity={animated ? 0 : 1}>
          {animated ? <Hold on={4.75} /> : null}
          <text x={425} y={318} textAnchor="middle" className="fill-pos font-mono text-[10.5px] font-medium">
            → SSE · webhook
          </text>
        </g>

        {animated ? (
          <>
            <Packet path="inEvents" from={0.4} to={1.25} />
            <Packet path="inTx" from={0.1} to={0.95} />
            <Packet path="outReceived" from={1.15} to={1.75} />
            <Packet path="inTxs" from={2.0} to={2.65} />
            <Packet path="outSeen" from={2.6} to={3.15} />
            <Packet path="outMined" from={3.95} to={4.55} tone="pos" />
          </>
        ) : null}
      </svg>
    </div>
  );
}
