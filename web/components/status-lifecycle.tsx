"use client";

import {
  ArrowUUpLeft,
  ArrowsClockwise,
  Broadcast,
  CaretLeft,
  CaretRight,
  Cube,
  Eye,
  Pause,
  Play,
  Tray,
  UsersThree,
  XCircle,
  type Icon,
} from "@phosphor-icons/react";
import { useEffect, useState, type ReactNode } from "react";
import { CodeBlock } from "@/components/code-block";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  branches,
  happyPath,
  measuredGap,
  type LifecycleIcon,
  type LifecycleState,
  type WebhookDelivery,
} from "@/lib/lifecycle";
import { useReducedMotion } from "@/lib/motion";
import { cn } from "@/lib/utils";

type Branch = (typeof branches)[number];

const all: LifecycleState[] = [...happyPath, ...branches];
const STEP_MS = 1400;

const icons: Record<LifecycleIcon, Icon> = {
  received: Tray,
  accepted: Broadcast,
  seen: Eye,
  "seen-multiple": UsersThree,
  mined: Cube,
  retry: ArrowsClockwise,
  rejected: XCircle,
  reorg: ArrowUUpLeft,
};

const webhookWord: Record<WebhookDelivery, string> = {
  default: "Webhook",
  full: "Webhook with X-FullStatusUpdates",
  never: "No webhook",
};

/**
 * One sentence per channel, for the tooltips. Sources: docs/sse.md (token
 * scoping) and services/webhook/service.go upstream (what is posted when).
 */
const sseExplainer =
  "Pushed live on GET /events; add ?callbackToken= to receive only the transactions you submitted under that token.";

const webhookExplainer: Record<WebhookDelivery, string> = {
  default: "POSTed to the X-CallbackUrl you submitted with; no extra header needed.",
  full: "POSTed to your X-CallbackUrl only if you also sent X-FullStatusUpdates:\u00a0true when submitting.",
  never: "Never POSTed to a webhook; watch GET /events or poll GET /tx/:txid to see it.",
};

/** The two delivery lanes drawn under the happy path. */
const lanes: {
  label: string;
  kind: (s: LifecycleState) => WebhookDelivery;
  explainer: string;
}[] = [
  { label: "SSE", kind: () => "default", explainer: sseExplainer },
  {
    label: "Webhook",
    kind: (s) => s.webhook,
    explainer:
      "POSTed to your X-CallbackUrl: a dot is sent by default, a ring only with X-FullStatusUpdates:\u00a0true.",
  },
];

/** A label that explains itself on hover or focus: dotted underline, shadcn tooltip. */
function Hint({
  explainer,
  className,
  children,
}: {
  explainer: string;
  className?: string;
  children: ReactNode;
}): ReactNode {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          tabIndex={0}
          className={cn(
            "cursor-help rounded-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
            className,
          )}
        >
          {children}
        </span>
      </TooltipTrigger>
      <TooltipContent side="bottom" sideOffset={6} className="max-w-64 text-center">
        {explainer}
      </TooltipContent>
    </Tooltip>
  );
}

/** Where on the happy path a state sits: its own index, or its parent's for a side state. */
function pathIndex(id: string): number {
  const own = happyPath.findIndex((s) => s.id === id);
  if (own >= 0) return own;
  const branch = branches.find((b) => b.id === id);
  return branch ? happyPath.findIndex((s) => s.id === branch.from) : 0;
}

/** Sent, sent only with the header, or never: a filled dot, a ring, a dash. */
function Marker({
  kind,
  active = false,
}: {
  kind: WebhookDelivery;
  active?: boolean;
}): ReactNode {
  return (
    <span aria-hidden className="flex size-4 items-center justify-center">
      {kind === "default" ? (
        <span
          className={cn(
            "size-2.5 rounded-full bg-primary",
            active && "ring-4 ring-info-bg",
          )}
        />
      ) : kind === "full" ? (
        <span
          className={cn(
            "size-2.5 rounded-full border-2 border-primary",
            active && "ring-4 ring-info-bg",
          )}
        />
      ) : (
        <span className="h-0.5 w-2.5 rounded-full bg-ink-muted/40" />
      )}
    </span>
  );
}

function Node({
  state,
  phase,
  size = "lg",
}: {
  state: LifecycleState;
  phase: "done" | "current" | "todo";
  size?: "lg" | "sm";
}): ReactNode {
  const Glyph = icons[state.icon];
  return (
    <span
      aria-hidden
      className={cn(
        "relative z-10 flex shrink-0 items-center justify-center rounded-full transition-[background-color,border-color,color,box-shadow] duration-300",
        size === "lg" ? "size-11" : "size-9",
        phase === "done" && "btn-primary-surface text-primary-foreground",
        phase === "current" &&
          "border-2 border-primary bg-surface-raised text-primary ring-4 ring-info-bg",
        phase === "todo" &&
          "border border-border bg-surface-raised text-ink-muted",
      )}
    >
      <Glyph
        weight={phase === "todo" ? "regular" : "bold"}
        className={size === "lg" ? "size-5" : "size-4"}
      />
    </span>
  );
}

function BranchChip({
  branch,
  selected,
  onSelect,
}: {
  branch: Branch;
  selected: boolean;
  onSelect: () => void;
}): ReactNode {
  const Glyph = icons[branch.icon];
  const neg = branch.tone === "neg";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          onClick={onSelect}
          aria-pressed={selected}
          className={cn(
            "inline-flex min-h-9 items-center gap-1.5 rounded-full border px-3 py-1 font-mono text-xs font-medium whitespace-nowrap transition-colors",
            "focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
            neg
              ? selected
                ? "border-neg bg-neg-bg text-neg"
                : "border-border text-neg hover:bg-neg-bg"
              : selected
                ? "border-primary-edge bg-info-bg text-foreground"
                : "border-border text-ink-muted hover:bg-surface hover:text-foreground",
          )}
        >
          <Glyph aria-hidden weight="bold" className="size-3.5" />
          {branch.id === "reorg" ? "reorg" : branch.status}
        </button>
      </TooltipTrigger>
      <TooltipContent
        side="bottom"
        sideOffset={6}
        className="max-w-64 text-center"
      >
        <span className="block font-medium">{branch.fromLabel}</span>
        <span className="mt-0.5 block opacity-80">{branch.explainer}</span>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * The status lifecycle as a stepper a reader drives.
 *
 * Built to be read at a glance: icon nodes for the happy path, and under them
 * two lanes that answer the integrator's real question, "will this reach me?",
 * for every state at once (a dot is sent, a ring is sent only with
 * X-FullStatusUpdates, a dash is never). Side states hang off the step they
 * leave from. Choosing a state shows one line about it and the frame exactly
 * as GET /events sends it, with the fields that state adds highlighted.
 *
 * "Play" walks the happy path once and stops; nothing moves on its own, and
 * under reduced motion there is no Play. Data and its sources: lib/lifecycle.ts.
 */
export function StatusLifecycle(): ReactNode {
  const [selected, setSelected] = useState("received");
  const [playing, setPlaying] = useState(false);
  const reduced = useReducedMotion();

  const current = all.find((s) => s.id === selected) ?? happyPath[0];
  const progress = pathIndex(selected);
  const onPath = happyPath.some((s) => s.id === selected);
  const order = all.map((s) => s.id);
  const position = order.indexOf(selected);
  const CurrentGlyph = icons[current.icon];

  useEffect(() => {
    if (!playing) return;
    const index = happyPath.findIndex((s) => s.id === selected);
    if (index < 0 || index >= happyPath.length - 1) {
      const stop = setTimeout(() => setPlaying(false), 0);
      return () => clearTimeout(stop);
    }
    const next = setTimeout(
      () => setSelected(happyPath[index + 1].id),
      STEP_MS,
    );
    return () => clearTimeout(next);
  }, [playing, selected]);

  const choose = (id: string): void => {
    setPlaying(false);
    setSelected(id);
  };

  const togglePlay = (): void => {
    if (playing) {
      setPlaying(false);
      return;
    }
    if (!onPath || selected === happyPath[happyPath.length - 1].id)
      setSelected(happyPath[0].id);
    setPlaying(true);
  };

  const phase = (i: number): "done" | "current" | "todo" =>
    onPath
      ? i < progress
        ? "done"
        : i === progress
          ? "current"
          : "todo"
      : i <= progress
        ? "done"
        : "todo";

  const control =
    "inline-flex size-9 items-center justify-center rounded-md border border-border text-ink-muted transition-colors hover:bg-surface hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-40";

  return (
    <div className="rounded-xl border border-border bg-surface-raised p-5 md:p-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h3 className="headline-sm font-sans tracking-normal">Status lifecycle</h3>
          <p className="text-sm text-ink-muted">
            Choose a state to see what reaches you.
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            className={control}
            aria-label="Previous state"
            disabled={position <= 0}
            onClick={() => choose(order[position - 1])}
          >
            <CaretLeft aria-hidden className="size-4" />
          </button>
          <button
            type="button"
            className={control}
            aria-label="Next state"
            disabled={position >= order.length - 1}
            onClick={() => choose(order[position + 1])}
          >
            <CaretRight aria-hidden className="size-4" />
          </button>
          {!reduced ? (
            <button
              type="button"
              onClick={togglePlay}
              className="inline-flex h-9 items-center gap-1.5 rounded-md border border-border px-3 text-sm font-medium text-foreground transition-colors hover:bg-surface focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
            >
              {playing ? (
                <Pause aria-hidden weight="fill" className="size-3.5" />
              ) : (
                <Play aria-hidden weight="fill" className="size-3.5" />
              )}
              {playing ? "Pause" : "Play"}
            </button>
          ) : null}
        </div>
      </div>

      {/* Desktop: nodes on one line, the two delivery lanes under them, side states last. */}
      <div className="mt-10 hidden grid-cols-[4.5rem_minmax(0,1fr)] gap-x-4 lg:grid">
        <span />
        <div className="relative">
          <span
            aria-hidden
            className="absolute top-5.5 right-[10%] left-[10%] h-0.5 -translate-y-1/2 bg-border"
          />
          <span
            aria-hidden
            className="absolute top-5.5 left-[10%] h-0.5 -translate-y-1/2 bg-primary transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
            style={{ width: `${(progress / (happyPath.length - 1)) * 80}%` }}
          />
          <a
            href={measuredGap.source}
            target="_blank"
            rel="noreferrer noopener"
            title={measuredGap.detail}
            className="absolute -top-5 left-[20%] -translate-x-1/2 rounded-sm px-1.5 text-xs whitespace-nowrap text-ink-muted underline decoration-ink-muted/50 decoration-dotted underline-offset-4 hover:text-foreground"
          >
            {measuredGap.label}
          </a>
          <ol className="relative grid grid-cols-5">
            {happyPath.map((step, i) => (
              <li key={step.id} className="flex justify-center">
                <button
                  type="button"
                  onClick={() => choose(step.id)}
                  aria-pressed={selected === step.id}
                  className="group flex flex-col items-center gap-2.5 rounded-lg px-2 pb-1 focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
                >
                  <Node state={step} phase={phase(i)} />
                  <span
                    className={cn(
                      "font-mono text-xs font-medium transition-colors",
                      selected === step.id
                        ? "text-primary"
                        : "text-foreground group-hover:text-primary",
                    )}
                  >
                    {step.status}
                  </span>
                </button>
              </li>
            ))}
          </ol>
        </div>

        {/* The lanes: one glance answers "will this reach me?" for every state. */}
        {lanes.map((lane, li) => (
          <div key={lane.label} className="contents">
            <span
              className={cn(
                "flex items-center justify-end",
                li === 0 ? "mt-6" : "mt-1",
              )}
            >
              <Hint
                explainer={lane.explainer}
                className="font-mono text-xs text-ink-muted underline decoration-ink-muted/50 decoration-dotted underline-offset-4"
              >
                {lane.label}
              </Hint>
            </span>
            <div
              className={cn(
                "grid grid-cols-5 rounded-md bg-surface py-2",
                li === 0 ? "mt-6" : "mt-1",
              )}
            >
              {happyPath.map((step) => (
                <span key={step.id} className="flex justify-center">
                  <Marker
                    kind={lane.kind(step)}
                    active={selected === step.id}
                  />
                </span>
              ))}
            </div>
          </div>
        ))}

        <span />
        <div className="mt-3 grid grid-cols-5">
          {happyPath.map((step) => (
            <div key={step.id} className="flex flex-col items-center gap-2">
              {branches
                .filter((b) => b.from === step.id)
                .map((branch) => (
                  <div key={branch.id} className="flex flex-col items-center">
                    <span
                      aria-hidden
                      className="h-3 w-px border-l border-dashed border-ink-muted/40"
                    />
                    <BranchChip
                      branch={branch}
                      selected={selected === branch.id}
                      onSelect={() => choose(branch.id)}
                    />
                  </div>
                ))}
            </div>
          ))}
        </div>
      </div>

      {/* Below lg: stacked, each state's webhook marker beside it, side states under their step. */}
      <ol className="mt-8 lg:hidden">
        {happyPath.map((step, i) => {
          const off = branches.filter((b) => b.from === step.id);
          const last = i === happyPath.length - 1;
          return (
            <li key={step.id} className="relative pb-4 last:pb-0">
              {!last ? (
                <span
                  aria-hidden
                  className={cn(
                    "absolute top-9 bottom-0 left-4.5 w-0.5 -translate-x-1/2",
                    i < progress ? "bg-primary" : "bg-border",
                  )}
                />
              ) : null}
              <button
                type="button"
                onClick={() => choose(step.id)}
                aria-pressed={selected === step.id}
                className="flex w-full items-center gap-3 rounded-lg text-start focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
              >
                <Node state={step} phase={phase(i)} size="sm" />
                <span
                  className={cn(
                    "min-w-0 flex-1 font-mono text-sm font-medium [overflow-wrap:anywhere]",
                    selected === step.id ? "text-primary" : "text-foreground",
                  )}
                >
                  {step.status}
                </span>
                <span className="flex shrink-0 items-center gap-1 font-mono text-[0.6875rem] text-ink-muted">
                  webhook <Marker kind={step.webhook} />
                </span>
              </button>
              {off.length > 0 ? (
                <div className="mt-2 ms-12 flex flex-wrap gap-2">
                  {off.map((branch) => (
                    <BranchChip
                      key={branch.id}
                      branch={branch}
                      selected={selected === branch.id}
                      onSelect={() => choose(branch.id)}
                    />
                  ))}
                </div>
              ) : null}
            </li>
          );
        })}
      </ol>

      <p className="mt-5 flex flex-wrap items-center gap-x-4 gap-y-1.5 text-xs text-ink-muted lg:ms-22">
        <span className="inline-flex items-center gap-1">
          <Marker kind="default" /> Sent
        </span>
        <span className="inline-flex items-center gap-1">
          <Marker kind="full" /> Only with{" "}
          <code className="font-mono text-foreground">X-FullStatusUpdates</code>
        </span>
        <span className="inline-flex items-center gap-1">
          <Marker kind="never" /> Never
        </span>
        <span>
          <code className="font-mono text-foreground">GET /tx/:txid</code>{" "}
          returns the current state any time.
        </span>
      </p>

      {/* The chosen state: one line, what reaches you, what it adds, and the frame. */}
      <div
        aria-live="polite"
        className="mt-8 grid items-start gap-6 border-t border-border pt-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)] lg:gap-8"
      >
        <div>
          <div className="flex items-center gap-3">
            <span
              className={cn(
                "flex size-10 shrink-0 items-center justify-center rounded-lg",
                current.tone === "neg"
                  ? "bg-neg-bg text-neg"
                  : "bg-info-bg text-primary",
              )}
            >
              <CurrentGlyph aria-hidden weight="bold" className="size-5" />
            </span>
            <p className="min-w-0 font-mono text-base font-semibold [overflow-wrap:anywhere]">
              {current.status}
              {current.id === "reorg" ? (
                <span className="font-sans font-normal text-ink-muted">
                  {" "}
                  · reorg
                </span>
              ) : null}
            </p>
          </div>
          <p className="body-md mt-4 text-foreground">{current.summary}</p>

          <div className="mt-4 flex flex-wrap gap-2">
            <Hint
              explainer={sseExplainer}
              className="inline-flex items-center gap-1.5 rounded-full border border-border px-2.5 py-1 text-xs text-foreground transition-colors hover:border-info-bd"
            >
              <Marker kind="default" /> SSE
            </Hint>
            <Hint
              explainer={webhookExplainer[current.webhook]}
              className={cn(
                "inline-flex items-center gap-1.5 rounded-full border border-border px-2.5 py-1 text-xs transition-colors hover:border-info-bd",
                current.webhook === "never" ? "text-ink-muted" : "text-foreground",
              )}
            >
              <Marker kind={current.webhook} /> {webhookWord[current.webhook]}
            </Hint>
          </div>

          {current.adds ? (
            <p className="mt-4 flex flex-wrap items-center gap-1.5 text-xs text-ink-muted">
              Adds
              {current.adds.map((field) => (
                <code
                  key={field}
                  className="rounded-sm bg-info-bg px-1.5 py-0.5 font-mono text-foreground"
                >
                  {field}
                </code>
              ))}
            </p>
          ) : null}
          {current.note ? (
            <p className="mt-3 text-sm text-ink-muted">{current.note}</p>
          ) : null}
        </div>
        <CodeBlock
          title="GET /events frame"
          code={current.frame}
          highlight={current.adds}
          wrap
        />
      </div>

      <p className="mt-6 text-xs text-ink-muted">
        States can be skipped: later ones arrive as the network reports them.
        Timestamps are illustrative, except the measured{" "}
        <a
          href={measuredGap.source}
          target="_blank"
          rel="noreferrer noopener"
          className="font-medium text-primary underline-offset-2 hover:underline"
        >
          {measuredGap.label}
        </a>
        .
      </p>
    </div>
  );
}
