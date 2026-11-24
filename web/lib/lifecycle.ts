/**
 * The transaction status lifecycle, as the status explorer draws it.
 *
 * Sources, so this can be re-checked when the service changes:
 * - States and transitions: docs/overview.md, "Status state machine".
 * - Frame shapes: docs/sse.md, "Frame format", "Mined frames carry the merkle
 *   proof", "Rejected frames carry the reason".
 * - Who receives what: services/webhook/service.go upstream (`shouldDeliver`,
 *   `handleUpdate`). Default webhooks post only MINED and REJECTED; with
 *   X-FullStatusUpdates every transition except RECEIVED, which is never posted.
 *   This follows the code, which is stricter than the X-FullStatusUpdates header
 *   description (that one lists RECEIVED).
 * - SEEN_MULTIPLE_NODES is a real status change (`applySeenCallback` in
 *   services/api_server/handlers.go), whatever overview.md's phase 2 says.
 *
 * Deliberately left out: STUMP_PROCESSING (in-memory only, never persisted or
 * published) and IMMUTABLE (in the enum, no producer yet). Showing either would
 * teach a client to wait for something that never arrives.
 *
 * Timestamps are illustrative except for one gap: RECEIVED to
 * ACCEPTED_BY_NETWORK is set to 1.44 s, the measured mean in
 * docs/100tps-propagation-results.md. Each `id` is the timestamp in Unix
 * nanoseconds, as on the real stream. The txid is elided as in the docs.
 */

export type WebhookDelivery = "default" | "full" | "never";

export type LifecycleIcon =
  | "received"
  | "accepted"
  | "seen"
  | "seen-multiple"
  | "mined"
  | "retry"
  | "rejected"
  | "reorg";

export type LifecycleState = {
  id: string;
  status: string;
  /** Shorter label for a narrow node. */
  title: string;
  icon: LifecycleIcon;
  /** One line. Anything longer belongs in the docs, not the diagram. */
  summary: string;
  /** Fields this state adds to the base three (txid, txStatus, timestamp). */
  adds?: string[];
  webhook: WebhookDelivery;
  /** The frame as it arrives on GET /events. */
  frame: string;
  note?: string;
  tone?: "neg" | "neutral";
};

const frame = (id: string, data: string): string => `id: ${id}\nevent: status\ndata: ${data}`;

export const happyPath: LifecycleState[] = [
  {
    id: "received",
    status: "RECEIVED",
    title: "Received",
    icon: "received",
    summary: "Stored at intake. The first frame on a stream opened before you submit.",
    webhook: "never",
    frame: frame(
      "1777400454683456789",
      `{"txid":"abc...","txStatus":"RECEIVED","timestamp":"2026-04-28T18:20:54.683456789Z"}`,
    ),
    note: "SSE only: never posted to a webhook.",
  },
  {
    id: "accepted",
    status: "ACCEPTED_BY_NETWORK",
    title: "Accepted",
    icon: "accepted",
    summary: "Broadcast to Teranode and acknowledged by the first endpoint to answer.",
    webhook: "full",
    frame: frame(
      "1777400456123456789",
      `{"txid":"abc...","txStatus":"ACCEPTED_BY_NETWORK","timestamp":"2026-04-28T18:20:56.123456789Z"}`,
    ),
  },
  {
    id: "seen",
    status: "SEEN_ON_NETWORK",
    title: "Seen",
    icon: "seen",
    summary: "Found in a miner's subtree.",
    webhook: "full",
    frame: frame(
      "1777400456871204113",
      `{"txid":"abc...","txStatus":"SEEN_ON_NETWORK","timestamp":"2026-04-28T18:20:56.871204113Z"}`,
    ),
  },
  {
    id: "seen-multiple",
    status: "SEEN_MULTIPLE_NODES",
    title: "Seen widely",
    icon: "seen-multiple",
    summary: "More than one miner has it.",
    webhook: "full",
    frame: frame(
      "1777400458402917650",
      `{"txid":"abc...","txStatus":"SEEN_MULTIPLE_NODES","timestamp":"2026-04-28T18:20:58.402917650Z"}`,
    ),
  },
  {
    id: "mined",
    status: "MINED",
    title: "Mined",
    icon: "mined",
    summary: "In a block, with the merkle proof attached. No need to poll for it.",
    adds: ["blockHash", "blockHeight", "merklePath"],
    webhook: "default",
    frame: frame(
      "1777400512987654321",
      `{"txid":"abc...","txStatus":"MINED","timestamp":"2026-04-28T18:21:52.987654321Z","blockHash":"0000...","blockHeight":958781,"merklePath":"<BUMP hex>"}`,
    ),
    note: "The last status sent today. Don't wait for IMMUTABLE.",
  },
];

/** Side states, each hanging off the step it leaves from. */
export const branches: (LifecycleState & {
  from: string;
  fromLabel: string;
  /** One sentence for the chip's tooltip: when it happens and what to do. */
  explainer: string;
})[] = [
  {
    id: "pending-retry",
    from: "received",
    fromLabel: "No verdict yet",
    explainer:
      "No node has accepted or rejected it yet, so it is rebroadcast with backoff for about 24 hours before becoming REJECTED.",
    status: "PENDING_RETRY",
    title: "Pending retry",
    icon: "retry",
    tone: "neutral",
    summary: "No verdict yet. Rebroadcast with backoff for about 24 hours, then REJECTED.",
    webhook: "full",
    frame: frame(
      "1777400459512003471",
      `{"txid":"abc...","txStatus":"PENDING_RETRY","timestamp":"2026-04-28T18:20:59.512003471Z"}`,
    ),
  },
  {
    id: "rejected",
    from: "accepted",
    fromLabel: "Verdict against",
    explainer:
      "Intake or the network refused it; code 466 means a double spend, and fixing the cause then resubmitting retries it.",
    status: "REJECTED",
    title: "Rejected",
    icon: "rejected",
    tone: "neg",
    summary: "A verdict against the transaction, at intake or from the network.",
    adds: ["status", "extraInfo"],
    webhook: "default",
    note: "466 means double spend. Resubmitting re-validates.",
    frame: frame(
      "1777400461204338905",
      `{"txid":"abc...","txStatus":"REJECTED","timestamp":"2026-04-28T18:21:01.204338905Z","status":466,"extraInfo":"UTXO_SPENT (70): ... utxo already spent by tx 7dfb...[0]"}`,
    ),
  },
  {
    id: "reorg",
    from: "mined",
    fromLabel: "Block orphaned",
    explainer:
      "Its block lost a chain reorganisation, so it drops back to SEEN_ON_NETWORK until it is mined again.",
    status: "SEEN_ON_NETWORK",
    title: "Reorg",
    icon: "reorg",
    tone: "neutral",
    summary: "Block orphaned: back to SEEN_ON_NETWORK until it is mined again.",
    adds: ["extraInfo"],
    webhook: "full",
    frame: frame(
      "1777401067640215338",
      `{"txid":"abc...","txStatus":"SEEN_ON_NETWORK","timestamp":"2026-04-28T18:31:07.640215338Z","extraInfo":"reorg_unmined"}`,
    ),
    note: "The re-mined MINED frame carries the new block and proof.",
  },
];

/** The one measured interval, and where it was measured. */
export const measuredGap = {
  after: "received",
  label: "1.44 s mean",
  source:
    "https://github.com/bsv-blockchain/broadcaster/blob/main/docs/100tps-propagation-results.md",
  detail: "Measured at a sustained 100 TPS against teratestnet.",
};
