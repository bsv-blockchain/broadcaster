import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Page anatomy helpers. Sections separate with vertical air rather than
 * divider lines: when choosing between a border and space, choose space.
 */

type Tone = "page" | "surface" | "navy";

const toneClass: Record<Tone, string> = {
  page: "",
  surface: "bg-surface",
  navy: "bg-brand-navy text-white dark:bg-[oklch(0.24_0.06_270)]",
};

export function Section({
  tone = "page",
  className,
  children,
  ...props
}: ComponentProps<"section"> & { tone?: Tone }): ReactNode {
  return (
    <section className={cn("section-y", toneClass[tone], className)} {...props}>
      <div className="container-page">{children}</div>
    </section>
  );
}

/** Small uppercase kicker above a section title. The one place uppercase is allowed. */
export function Eyebrow({
  className,
  children,
  ...props
}: ComponentProps<"p">): ReactNode {
  return (
    <p
      className={cn("label-sm mb-3 text-primary uppercase", className)}
      {...props}
    >
      {children}
    </p>
  );
}

export function SectionHeader({
  eyebrow,
  title,
  lede,
  align = "start",
  className,
}: {
  eyebrow?: string;
  title: ReactNode;
  lede?: ReactNode;
  align?: "start" | "center";
  className?: string;
}): ReactNode {
  return (
    <header
      className={cn(
        "mb-10 max-w-2xl md:mb-14",
        align === "center" && "mx-auto text-center",
        className,
      )}
    >
      {eyebrow ? <Eyebrow>{eyebrow}</Eyebrow> : null}
      <h2 className="headline-lg">{title}</h2>
      {lede ? <p className="body-lg mt-4 text-ink-muted">{lede}</p> : null}
    </header>
  );
}
