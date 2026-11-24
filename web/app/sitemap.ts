import type { MetadataRoute } from "next";
import { docsPath, site } from "@/lib/site";

/** Static export: built once, served as a file by the Go server. */
export const dynamic = "force-static";

export default function sitemap(): MetadataRoute.Sitemap {
  const lastModified = new Date(`${site.updated}T00:00:00Z`);
  return [
    { url: `${site.url}/`, lastModified, changeFrequency: "monthly", priority: 1 },
    { url: `${site.url}${docsPath}`, lastModified, changeFrequency: "weekly", priority: 0.9 },
  ];
}
