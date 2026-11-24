"use client";

import { type ReactNode, useSyncExternalStore } from "react";
import { site } from "@/lib/site";

const noop = (): void => {};
const subscribe = (): (() => void) => noop;

/**
 * The API base URL a reader should use: the origin serving this page.
 *
 * Every broadcaster operator serves this page from its own API server, so the
 * page's origin is that server. The prerendered HTML carries `site.apiBaseUrl`
 * (the public deployment) and hydration swaps in `window.location.origin`;
 * `useSyncExternalStore` keeps the server and first client render identical,
 * so there is no hydration mismatch.
 */
export function useApiBaseUrl(): string {
  return useSyncExternalStore(
    subscribe,
    () => window.location.origin,
    () => site.apiBaseUrl,
  );
}

/** The base URL as text, for prose and stat cells. */
export function ApiBaseUrl(): ReactNode {
  return useApiBaseUrl();
}
