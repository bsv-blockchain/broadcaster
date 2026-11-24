/** The Chaintracks block-header API, shared by the docs page and llms-full.txt. */

export const chaintracksV2: [string, string][] = [
  ["/chaintracks/v2/network", "Configured network."],
  ["/chaintracks/v2/height", "Current best-chain height known to Chaintracks."],
  ["/chaintracks/v2/tip", "Current tip header as JSON."],
  ["/chaintracks/v2/tip.bin", "Current serialized 80-byte tip header."],
  ["/chaintracks/v2/headers?height={height}&count={count}", "Consecutive serialized headers as raw bytes."],
  ["/chaintracks/v2/headers.bin?height={height}&count={count}", "The same header run, with start-height and header-count response headers."],
  ["/chaintracks/v2/header/height/{height}", "Header at a height as JSON."],
  ["/chaintracks/v2/header/height/{height}.bin", "Serialized 80-byte header at a height."],
  ["/chaintracks/v2/header/hash/{hash}", "Header by block hash as JSON."],
  ["/chaintracks/v2/header/hash/{hash}.bin", "Serialized 80-byte header by block hash."],
  ["/chaintracks/v2/tip/stream", "Server-Sent Events stream of the current and future tip headers."],
  ["/chaintracks/v2/reorg/stream", "Server-Sent Events stream of reorganization events."],
  ["/chaintracks/{file}", "Range-enabled download of an individual stored header file."],
];

export const chaintracksV1: [string, string][] = [
  ["/chaintracks/v1/getChain", "Configured network."],
  ["/chaintracks/v1/getPresentHeight", "Current best-chain height."],
  ["/chaintracks/v1/findChainTipHashHex", "Current tip hash."],
  ["/chaintracks/v1/findChainTipHeaderHex", "Current tip header."],
  ["/chaintracks/v1/findHeaderHexForHeight?height={height}", "Header by height."],
  ["/chaintracks/v1/findHeaderHexForBlockHash?hash={hash}", "Header by block hash."],
  ["/chaintracks/v1/getHeaders?height={height}&count={count}", "Consecutive headers in a hex-encoded response value."],
];

export const chaintracksCurl = `curl -fsS http://localhost:8083/health
# {"status":"ok"}

curl -fsS http://localhost:8083/chaintracks/v2/network
curl -fsS http://localhost:8083/chaintracks/v2/height
curl -fsS http://localhost:8083/chaintracks/v2/tip`;
