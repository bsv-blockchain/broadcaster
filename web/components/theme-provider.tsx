"use client";

import { ThemeProvider as NextThemesProvider } from "next-themes";
import type { ComponentProps, ReactNode } from "react";

export function ThemeProvider({
  children,
  ...props
}: ComponentProps<typeof NextThemesProvider>): ReactNode {
  return <NextThemesProvider {...props}>{children}</NextThemesProvider>;
}
