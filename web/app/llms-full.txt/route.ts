import { llmsFull } from "@/lib/llms";

/** Static export: built once, served as a file by the Go server. */
export const dynamic = "force-static";

/** The whole API reference as one Markdown file. Prerendered: it reads no request data. */
export function GET(): Response {
  return new Response(llmsFull(), {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
