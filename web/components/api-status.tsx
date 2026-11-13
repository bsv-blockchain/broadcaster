"use client";

import { type ReactNode, useEffect, useState } from "react";
import { useApiBaseUrl } from "@/components/api-base-url";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

type Status = {
  up: boolean;
  /** The API server's own chain-tip height, when it answered. */
  blockHeight?: number;
  version?: string;
  checkedAt: string;
};

/** The fields of the API server's `GET /health` this dot reads. */
type Health = { healthy?: boolean; blockHeight?: number; version?: string };

/**
 * One liveness check of the API server serving this page. "Up" means it
 * answered 2xx within four seconds and did not report itself unhealthy.
 */
async function checkHealth(): Promise<Status> {
  const checkedAt = new Date().toISOString();
  try {
    const res = await fetch("/health", {
      cache: "no-store",
      signal: AbortSignal.timeout(4000),
      headers: { accept: "application/json" },
    });
    if (!res.ok) return { up: false, checkedAt };
    const health = (await res.json()) as Health;
    return { up: health.healthy !== false, blockHeight: health.blockHeight, version: health.version, checkedAt };
  } catch {
    // Unreachable, timed out or not JSON: reported as down.
    return { up: false, checkedAt };
  }
}

const POLL_MS = 30_000;

/** "12 s ago", from a timestamp, for the tooltip. */
function ago(iso: string, now: number): string {
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  return s < 60 ? `${s} s ago` : `${Math.round(s / 60)} min ago`;
}

/**
 * A live dot for the API server: green with a ping while `/health` answers,
 * red when it does not, grey until the first answer arrives.
 *
 * The page is served by the API server itself, so it polls that server's own
 * `/health` (same origin) every 30 seconds while the tab is visible. The ping is decoration and stops
 * under reduced motion; the word next to the dot carries the meaning.
 */
export function ApiStatus({ className }: { className?: string }): ReactNode {
  const baseUrl = useApiBaseUrl();
  const [status, setStatus] = useState<Status | null>(null);
  const [now, setNow] = useState(0);

  useEffect(() => {
    let alive = true;
    let timer = 0;

    const check = async (): Promise<void> => {
      const next = await checkHealth();
      if (alive) {
        setStatus(next);
        setNow(Date.now());
      }
    };

    const schedule = (): void => {
      window.clearInterval(timer);
      if (document.visibilityState !== "visible") return;
      void check();
      timer = window.setInterval(() => void check(), POLL_MS);
    };

    schedule();
    document.addEventListener("visibilitychange", schedule);
    return () => {
      alive = false;
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", schedule);
    };
  }, []);

  const state = status === null ? "checking" : status.up ? "up" : "down";
  const label = { checking: "Checking", up: "Operational", down: "Unreachable" }[state];

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          tabIndex={0}
          role="status"
          aria-live="polite"
          className={cn(
            "inline-flex items-center gap-2 rounded-full text-xs font-medium normal-case tracking-normal focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
            state === "up" ? "text-pos" : state === "down" ? "text-neg" : "text-ink-muted",
            className,
          )}
        >
          <span aria-hidden className="relative flex size-2">
            {state === "up" ? (
              <span className="absolute inline-flex size-full animate-ping rounded-full bg-pos opacity-60 motion-reduce:hidden" />
            ) : null}
            <span
              className={cn(
                "relative inline-flex size-2 rounded-full",
                state === "up" ? "bg-pos" : state === "down" ? "bg-neg" : "bg-ink-muted/60",
              )}
            />
          </span>
          {label}
        </span>
      </TooltipTrigger>
      <TooltipContent
        side="top"
        align="end"
        sideOffset={6}
        collisionPadding={12}
        className="max-w-[min(18rem,calc(100vw-1.5rem))] px-3.5 py-2.5 text-start text-pretty"
      >
        {state === "checking" ? (
          <span>Asking {baseUrl}/health…</span>
        ) : state === "up" ? (
          <>
            <span className="block">The API server answered its health check.</span>
            <span className="mt-1.5 block opacity-70">
              {status?.blockHeight
                ? `Chain tip ${status.blockHeight.toLocaleString("en-US")} · `
                : ""}
              {status?.version ? `${status.version} · ` : ""}checked {ago(status!.checkedAt, now)}
            </span>
          </>
        ) : (
          <>
            <span className="block">The API server did not answer its health check.</span>
            <span className="mt-1.5 block opacity-70">
              Checked {ago(status!.checkedAt, now)}; retrying every 30 s.
            </span>
          </>
        )}
      </TooltipContent>
    </Tooltip>
  );
}
