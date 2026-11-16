import type { ReactNode } from "react";
import { CopyButton } from "@/components/copy-button";
import { cn } from "@/lib/utils";

/**
 * Syntax colour without a highlighter, as the BSV Faucet does it, which in
 * turn follows bsvblockchain.org's code tint: one pass of one regular expression with named groups. It is not a
 * shell or JSON parser and does not need to be; the samples are ours.
 *
 * Order matters: comments and strings come first, so nothing inside them is
 * re-tinted. `//` comments are added for the annotated JSON samples here.
 */
const TOKENS = new RegExp(
  [
    String.raw`(?<comment>#[^\n]*|\/\/[^\n]*)`,
    String.raw`(?<key>"[^"\n]*"(?=\s*:))`, // a JSON key: a string followed by a colon
    String.raw`(?<string>'[^'\n]*'|"[^"\n]*")`,
    String.raw`(?<url>https?:\/\/[^\s'"]+)`,
    String.raw`(?<flag>(?<=\s)-{1,2}[A-Za-z][\w-]*)`,
    String.raw`(?<number>\b\d+\b)`,
    String.raw`(?<punct>[{}\[\]:,\\])`,
    String.raw`(?<word>[A-Za-z_][\w-]*)`,
  ].join("|"),
  "g",
);

const COMMANDS = new Set(["curl"]);
const METHODS = new Set(["POST", "GET"]);

function tint(groups: Record<string, string | undefined>, highlight?: Set<string>): string | null {
  if (groups.comment) return "text-code-dim italic";
  if (groups.key) {
    // A key the reader should notice (a field this state adds) gets the info tint behind it.
    return highlight?.has(groups.key.slice(1, -1))
      ? "rounded-sm bg-info-bg font-semibold whitespace-nowrap text-(--code-keyword) ring-1 ring-info-bd"
      : "text-(--code-keyword)";
  }
  if (groups.string) return "text-(--code-string)";
  if (groups.url) return "text-(--code-type) underline decoration-(--code-bd) underline-offset-4";
  if (groups.flag) return "text-(--code-meta)";
  if (groups.number) return "text-(--code-number)";
  if (groups.punct) return "text-code-dim";
  if (groups.word && COMMANDS.has(groups.word)) return "font-medium text-(--code-keyword)";
  if (groups.word && METHODS.has(groups.word)) return "text-(--code-type)";
  return null;
}

/** One line of a sample, split into tinted spans and plain text. */
function Highlighted({ line, highlight }: { line: string; highlight?: Set<string> }): ReactNode {
  const parts: ReactNode[] = [];
  let last = 0;
  for (const m of line.matchAll(TOKENS)) {
    const cls = tint(m.groups ?? {}, highlight);
    if (!cls) continue;
    if (m.index > last) parts.push(line.slice(last, m.index));
    parts.push(
      <span key={m.index} className={cls}>
        {m[0]}
      </span>,
    );
    last = m.index + m[0].length;
  }
  if (last < line.length) parts.push(line.slice(last));
  return <>{parts}</>;
}

function Lines({
  code,
  wrap = false,
  highlight,
}: {
  code: string;
  wrap?: boolean;
  highlight?: string[];
}): ReactNode {
  const keys = highlight?.length ? new Set(highlight) : undefined;
  return (
    <pre
      className={cn(
        "scrollbar-faint p-6 font-mono text-sm leading-[1.7] text-code-fg",
        wrap ? "whitespace-pre-wrap [overflow-wrap:anywhere]" : "overflow-x-auto",
      )}
    >
      {code.split("\n").map((line, i) => (
        <span key={i} className="block min-h-[1lh]">
          <Highlighted line={line} highlight={keys} />
        </span>
      ))}
    </pre>
  );
}

/**
 * The family's code panel. It follows the theme: an inset light panel in light
 * mode, the BSV Faucet's deep navy terminal in dark mode (tokens in
 * globals.css), hairline border, copy top right. With a
 * `title` it carries the faucet's bar (title left, copy right); `window` adds
 * the three neutral dots, kept for the one quickstart panel. No amber on the
 * page, so the window furniture is neutral, not traffic lights.
 */
export function CodeBlock({
  code,
  title,
  window: asWindow = false,
  copy = true,
  wrap = false,
  highlight,
  className,
}: {
  code: string;
  /** JSON keys to mark, e.g. the fields a status adds to a frame. */
  highlight?: string[];
  /** Wrap long lines instead of scrolling, for one-line payloads whose end matters (an SSE frame). */
  wrap?: boolean;
  title?: ReactNode;
  window?: boolean;
  /** False for placeholder samples where copying would only paste a placeholder. */
  copy?: boolean;
  className?: string;
}): ReactNode {
  const bar = title !== undefined || asWindow;

  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-lg border border-(--code-bd) bg-code-bg",
        className,
      )}
    >
      {bar ? (
        <div className="flex min-h-12 items-center gap-2 border-b border-(--code-bd) px-4 py-1.5">
          {asWindow ? (
            <>
              <span aria-hidden className="size-3 rounded-full bg-(--code-furniture)" />
              <span aria-hidden className="size-3 rounded-full bg-(--code-furniture)" />
              <span aria-hidden className="size-3 rounded-full bg-(--code-furniture)" />
            </>
          ) : null}
          {title !== undefined ? (
            <span className={cn("font-mono text-xs text-code-dim", asWindow && "ms-2")}>{title}</span>
          ) : null}
          {copy ? <CopyButton text={code} className="ms-auto" /> : null}
        </div>
      ) : copy ? (
        <CopyButton text={code} className="absolute top-3 right-3 bg-code-bg" />
      ) : null}
      <Lines code={code} wrap={wrap} highlight={highlight} />
    </div>
  );
}

/** Inline code in running text, as the faucet sets it: the mono face in the foreground colour, no chip. */
export function InlineCode({ children }: { children: ReactNode }): ReactNode {
  return <code className="font-mono text-[0.9375em] text-foreground">{children}</code>;
}
