import type { MetadataRoute } from "next";
import { site } from "@/lib/site";

/** Static export: built once, served as a file by the Go server. */
export const dynamic = "force-static";

/**
 * Everything on this site is public documentation, so every crawler is welcome,
 * and the AI crawlers are named so the intent is explicit rather than implied
 * by the wildcard.
 */
const aiCrawlers = [
  "GPTBot",
  "ChatGPT-User",
  "OAI-SearchBot",
  "ClaudeBot",
  "Claude-User",
  "Claude-SearchBot",
  "Google-Extended",
  "Amazonbot",
  "Bytespider",
  "CCBot",
  "PerplexityBot",
  "Applebot-Extended",
  "meta-externalagent",
];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      { userAgent: "*", allow: "/" },
      ...aiCrawlers.map((userAgent) => ({ userAgent, allow: "/" })),
    ],
    sitemap: `${site.url}/sitemap.xml`,
    host: site.url,
  };
}
