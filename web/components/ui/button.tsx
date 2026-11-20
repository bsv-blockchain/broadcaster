import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Buttons from bsvblockchain.org. Labels never wrap
 * and never truncate. `ButtonLink` replaces the reference's `asChild` slot,
 * since every button on this site navigates.
 */
const buttonVariants = cva(
  "inline-flex h-11 shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium outline-none transition-[transform,background-color,border-color,color] duration-150 hover:-translate-y-px focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background active:translate-y-0 active:scale-[0.98] disabled:pointer-events-none disabled:opacity-50 [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        primary: "btn-surface btn-primary-surface text-primary-foreground",
        secondary:
          "btn-surface btn-secondary-surface border border-border text-foreground",
        ghost: "bg-transparent text-ink-muted hover:bg-secondary hover:text-foreground",
      },
      size: {
        default: "px-5",
        sm: "h-9 px-3.5 text-[0.8125rem]",
        lg: "h-12 px-6 text-[0.9375rem]",
        icon: "size-11 px-0",
      },
    },
    defaultVariants: { variant: "primary", size: "default" },
  },
);

export function ButtonLink({
  className,
  variant,
  size,
  ...props
}: ComponentProps<"a"> & VariantProps<typeof buttonVariants>): ReactNode {
  return (
    <a
      data-glow=""
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  );
}

export { buttonVariants };
