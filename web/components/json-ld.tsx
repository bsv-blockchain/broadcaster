import type { ReactNode } from "react";
import { serialize } from "@/lib/structured-data";

/** Schema.org JSON-LD, rendered into the page HTML so crawlers read it without running scripts. */
export function JsonLd({ data }: { data: object }): ReactNode {
  return (
    <script
      type="application/ld+json"
      // Escaped by `serialize`; this is data, not code.
      dangerouslySetInnerHTML={{ __html: serialize(data) }}
    />
  );
}
