import { llmsIndex } from "@/lib/llms";

/** Static export: built once, served as a file by the Go server. */
export const dynamic = "force-static";

/** https://llmstxt.org index of this site. Prerendered: it reads no request data. */
export function GET(): Response {
  return new Response(llmsIndex(), {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
