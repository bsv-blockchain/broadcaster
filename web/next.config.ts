import type { NextConfig } from "next";

/**
 * The site is a static export (`out/`) that the Go API server embeds and serves
 * at `/`, `/docs` and the machine-readable paths. A static export ignores
 * `headers()`, so the Content-Security-Policy, the security headers and the
 * content types of the machine-readable files are set by the Go server
 * (services/api_server/site.go), not here. Keep any page within that CSP:
 * no other origins, no inline event handlers that need 'unsafe-eval'.
 */
const nextConfig: NextConfig = {
  output: "export",
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
};

export default nextConfig;
