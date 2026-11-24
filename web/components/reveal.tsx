"use client";

import { motion } from "motion/react";
import type { ComponentProps, ReactNode } from "react";
import { duration, easeOut, riseIn, viewportOnce } from "@/lib/motion";

/**
 * The one scroll-enter animation used across the site: an 18px rise with a
 * fade, triggered once. Grids wrap children in <RevealGroup> so siblings
 * stagger instead of each element firing on its own threshold.
 */
export function Reveal({
  delay = 0,
  className,
  children,
  ...props
}: ComponentProps<typeof motion.div> & { delay?: number }): ReactNode {
  return (
    <motion.div
      initial="hidden"
      whileInView="visible"
      viewport={viewportOnce}
      variants={riseIn}
      transition={{ duration: duration.slow, ease: easeOut, delay }}
      className={className}
      {...props}
    >
      {children}
    </motion.div>
  );
}

export function RevealGroup({
  stagger = 0.07,
  className,
  children,
  ...props
}: ComponentProps<typeof motion.div> & { stagger?: number }): ReactNode {
  return (
    <motion.div
      initial="hidden"
      whileInView="visible"
      viewport={viewportOnce}
      variants={{ hidden: {}, visible: { transition: { staggerChildren: stagger } } }}
      className={className}
      {...props}
    >
      {children}
    </motion.div>
  );
}

export function RevealItem({
  className,
  children,
  ...props
}: ComponentProps<typeof motion.div>): ReactNode {
  return (
    <motion.div
      variants={riseIn}
      transition={{ duration: duration.slow, ease: easeOut }}
      className={className}
      {...props}
    >
      {children}
    </motion.div>
  );
}
