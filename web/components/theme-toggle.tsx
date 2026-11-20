"use client";

import { Moon, Sun } from "@phosphor-icons/react/dist/ssr";
import { useTheme } from "next-themes";
import { useSyncExternalStore, type ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Light and dark, one button, no label.
 *
 * The site follows the operating system until this is pressed; pressing it
 * pins a choice for that reader and next-themes remembers it. There is no
 * third "system" position, because a control with three states and no text
 * needs an explanation, and a reader who wants to go back to following the
 * system has already got a switch for that in the system.
 *
 * The icon shows the theme it will switch *to*, which is the convention every
 * reader has already learned, and the accessible name says so in words.
 *
 * Nothing renders until mount: the server cannot know the resolved theme, and
 * an icon guessed on the server is an icon that flips under the cursor. The
 * placeholder holds the exact button size so the header never shifts.
 */
export function ThemeToggle({ className }: { className?: string }): ReactNode {
  const { resolvedTheme, setTheme } = useTheme();
  // False on the server and during hydration, true once on the client.
  const mounted = useSyncExternalStore(
    () => () => {},
    () => true,
    () => false,
  );

  const dark = resolvedTheme === "dark";

  return (
    <button
      type="button"
      onClick={() => setTheme(dark ? "light" : "dark")}
      aria-label={mounted ? (dark ? "Switch to light mode" : "Switch to dark mode") : "Switch theme"}
      className={cn(
        "inline-flex size-9 shrink-0 items-center justify-center rounded-md text-ink-muted transition-colors",
        "hover:bg-surface hover:text-foreground",
        "focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
        className,
      )}
    >
      {mounted ? (
        dark ? (
          <Sun aria-hidden weight="regular" className="size-4.5" />
        ) : (
          <Moon aria-hidden weight="regular" className="size-4.5" />
        )
      ) : (
        <span aria-hidden className="size-4.5" />
      )}
    </button>
  );
}
