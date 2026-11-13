import { site } from "@/lib/site";

/**
 * Questions integrators ask, answered from the docs and the API contract.
 *
 * Shown on the front page and published as FAQPage JSON-LD and in llms.txt, so
 * the wording an assistant quotes is the wording on the page. Answers are plain
 * text with `backticks` for code and **asterisks** for the terms worth spotting
 * when skimming; the page renders both, llms.txt keeps them as Markdown, and the
 * JSON-LD strips them.
 *
 * Sources: docs/overview.md, docs/sse.md, docs/migration-arcade-to-bsv-broadcaster.md,
 * content/routes.json, and services/webhook/service.go upstream for webhook delivery.
 */
export const faq: { id: string; question: string; answer: string }[] = [
  {
    id: "what-is-bsv-broadcaster",
    question: "What is BSV Broadcaster?",
    answer:
      "A **Teranode-native** transaction broadcast and status-tracking service. It accepts BSV transactions, **validates** them against policy, **broadcasts** them to Teranode, **tracks** their lifecycle and emits status updates over **webhooks** and **Server-Sent Events**. It was built as a Teranode-native replacement for **ARC**.",
  },
  {
    id: "submit-a-transaction",
    question: "How do I submit a transaction?",
    answer: `Send it to \`POST /tx\`, or a **batch** to \`POST /txs\`, on the API server (${site.apiBaseUrl} for the public deployment, or your operator's own host). The body can be **raw bytes** (\`application/octet-stream\`, the most efficient), **hex** (\`text/plain\`) or a **JSON envelope**. Policy validation runs before the \`202\` response, which carries the txid and the status \`RECEIVED\`.`,
  },
  {
    id: "track-status",
    question: "How do I track a transaction's status?",
    answer:
      "Three ways. **Look it up** any time with `GET /tx/:txid`. **Stream** updates over Server-Sent Events on `GET /events`, scoped by the `X-CallbackToken` you submitted with. Or **register a webhook** with the `X-CallbackUrl` header when you submit.",
  },
  {
    id: "webhook-statuses",
    question: "Which statuses are sent to a webhook?",
    answer:
      "**By default** only `MINED` and `REJECTED`. With `X-FullStatusUpdates: true` **every transition** is posted except `RECEIVED`, which is **never** sent to a webhook. The **SSE stream** carries every status, including `RECEIVED`.",
  },
  {
    id: "merkle-proofs",
    question: "Do I get a merkle proof?",
    answer:
      "Yes. The `MINED` update carries `blockHash`, `blockHeight` and `merklePath` (a **BUMP**), so there is **no need to poll** for the proof.",
  },
  {
    id: "arcade",
    question: "Is BSV Broadcaster the same as Arcade?",
    answer:
      "Yes. The project formerly called **Arcade** was **renamed** BSV Broadcaster. Legacy `ARCADE_*` environment variables are **still read** and mapped to their `BSV_BROADCASTER_*` names, with a deprecation warning at startup.",
  },
];

/** The answer as plain prose, for JSON-LD. */
export function plainAnswer(answer: string): string {
  return answer.replace(/`([^`]+)`/g, "$1").replace(/\*\*([^*]+)\*\*/g, "$1");
}
