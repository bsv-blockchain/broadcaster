"use client";

import { motion, useInView, type Transition } from "motion/react";
import { useRef, type ReactNode } from "react";
import { cn } from "@/lib/utils";
import { easeOutExpo, useReducedMotion } from "@/lib/motion";

/**
 * Large headings that reveal on scroll: each word sits behind an ink block
 * that drops away, word by word. Only `transform` animates.
 *
 * `mutedFrom` dims every word from that index on, which is how the page
 * puts emphasis on the first clause of a two-clause headline without
 * resorting to a second colour or a bold weight.
 */
export function RevealHeadline({
  children,
  as: Tag = "h2",
  className,
  id,
  stagger = 0.06,
  delay = 0,
  amount = 0.4,
  mutedFrom,
  blockClassName,
}: {
  children: string;
  as?: "h1" | "h2" | "h3" | "p" | "span";
  className?: string;
  id?: string;
  stagger?: number;
  delay?: number;
  amount?: number;
  mutedFrom?: number;
  /** Colour of the covering block; override on dark or tinted sections. */
  blockClassName?: string;
}): ReactNode {
  const ref = useRef<HTMLElement>(null);
  const inView = useInView(ref, { once: true, amount });
  const reduced = useReducedMotion();

  const tokens = children.split(/(\s+)/);
  const transition: Transition = { duration: 0.7, ease: easeOutExpo };

  if (reduced) {
    return (
      <Tag ref={ref as never} id={id} className={className}>
        {tokens.map((token, i) =>
          /^\s+$/.test(token) ? (
            <span key={`s-${i}`}> </span>
          ) : (
            <span
              key={`w-${i}`}
              className={
                mutedFrom !== undefined &&
                tokens.slice(0, i).filter((t) => !/^\s+$/.test(t)).length >= mutedFrom
                  ? "text-foreground/40"
                  : undefined
              }
            >
              {token}
            </span>
          ),
        )}
      </Tag>
    );
  }

  return (
    <Tag ref={ref as never} id={id} className={className}>
      {tokens.map((token, i) => {
        if (/^\s+$/.test(token)) return <span key={`s-${i}`}> </span>;

        const wordIndex = tokens
          .slice(0, i)
          .filter((t) => !/^\s+$/.test(t)).length;
        const isMuted = mutedFrom !== undefined && wordIndex >= mutedFrom;

        return (
          <span
            key={`w-${i}`}
            className="relative inline-block overflow-hidden align-baseline pb-[0.15em]"
          >
            <span className={cn("relative", isMuted && "text-foreground/40")}>
              {token}
            </span>
            <motion.span
              aria-hidden
              initial={{ y: "0%" }}
              animate={inView ? { y: "110%" } : { y: "0%" }}
              transition={{ ...transition, delay: delay + wordIndex * stagger }}
              className={cn(
                "absolute inset-x-0 -top-[0.06em] -bottom-[0.2em] bg-foreground will-change-transform",
                blockClassName,
              )}
            />
          </span>
        );
      })}
    </Tag>
  );
}
