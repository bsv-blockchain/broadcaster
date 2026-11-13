import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Badges from bsvblockchain.org: status, never
 * decoration. Tints follow the BSV Faucet: `info` uses the sRGB primary tint
 * (a translucent oklch blue fringes yellow in Chrome), `pos` and `neg` the
 * status tints, and there is no amber anywhere.
 */
const badgeVariants = cva(
  "inline-flex w-fit shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 label-sm whitespace-nowrap [&_svg]:size-3",
  {
    variants: {
      variant: {
        neutral: "border border-border text-ink-muted",
        info: "bg-info-bg text-primary",
        pos: "bg-pos-bg text-pos",
        neg: "bg-neg-bg text-neg",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

export function Badge({
  className,
  variant,
  ...props
}: ComponentProps<"span"> & VariantProps<typeof badgeVariants>): ReactNode {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />;
}

/** As the faucet's endpoint pills: POST in the success green, GET in primary blue. */
export function MethodBadge({ method }: { method: string }): ReactNode {
  return (
    <Badge
      variant={method === "POST" ? "pos" : "info"}
      className="tnum min-w-14 justify-center font-mono font-semibold tracking-normal"
    >
      {method}
    </Badge>
  );
}

const requirementVariant = {
  required: "neg",
  recommended: "info",
  optional: "neutral",
} as const;

export function RequirementBadge({ requirement }: { requirement: string }): ReactNode {
  const key = requirement.toLowerCase() as keyof typeof requirementVariant;
  return (
    <Badge variant={requirementVariant[key] ?? "neutral"} className="uppercase">
      {requirement}
    </Badge>
  );
}
