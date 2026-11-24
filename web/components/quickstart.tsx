"use client";

import type { ReactNode } from "react";
import { useApiBaseUrl } from "@/components/api-base-url";
import { CodeBlock } from "@/components/code-block";

/** POST /tx with the text/plain body, using the examples from the endpoint's own contract. */
function quickstart(baseUrl: string): string {
  return `curl -X POST ${baseUrl}/tx \\
  -H "Content-Type: text/plain" \\
  -d "0100000001abc...def00000000"

# -> 202
{
  "txid": "<hex>",
  "status": 202,
  "txStatus": "RECEIVED"
}`;
}

/** The hero's terminal, against the API server serving this page. */
export function Quickstart({ className }: { className?: string }): ReactNode {
  return <CodeBlock window title="broadcast.sh" code={quickstart(useApiBaseUrl())} className={className} />;
}
