"use client";

import { Check, Copy } from "@phosphor-icons/react";
import { useRef, useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The BSV Faucet terminal's copy button: outlined, icon and word, "Copied"
 * announced politely. Lives only on code panels and takes their theme tokens.
 */
export function CopyButton({ text, className }: { text: string; className?: string }): ReactNode {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const copy = (): void => {
    void navigator.clipboard?.writeText(text);
    setCopied(true);
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setCopied(false), 1500);
  };

  return (
    <button
      type="button"
      onClick={copy}
      className={cn(
        "inline-flex h-9 items-center gap-1.5 rounded-md border border-(--code-furniture) px-3 text-xs font-medium text-code-fg transition-colors hover:bg-(--code-hover)",
        "focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
        className,
      )}
    >
      {copied ? (
        <Check aria-hidden weight="bold" className="size-3.5" />
      ) : (
        <Copy aria-hidden className="size-3.5" />
      )}
      <span aria-live="polite">{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}
