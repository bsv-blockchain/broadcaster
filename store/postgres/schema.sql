-- Schema for the Postgres store backend. Applied idempotently by
-- Store.EnsureIndexes() via pgx.Exec; safe to run repeatedly.
--
-- The whole file runs inside ONE transaction: statements that cannot run in
-- a transaction block (CREATE INDEX CONCURRENTLY, ALTER TYPE ... ADD VALUE,
-- VACUUM) must not be added here. Any byte change to this file — comments
-- included — changes its checksum and triggers one serialized reapply on the
-- next rollout.

-- Schema-identity bookkeeping (issue #278). EnsureIndexes stores a SHA-256 of
-- this entire file after a successful apply; when the stored checksum matches
-- the binary's, startup skips every statement below — no DDL, no ACCESS
-- EXCLUSIVE lock requests queueing behind live traffic.
-- Escape hatch after manual DDL drift (e.g. a hand-dropped index):
--   DELETE FROM schema_info;  -- forces a full idempotent reapply on next start
CREATE TABLE IF NOT EXISTS schema_info (
    id         BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),  -- single-row table
    checksum   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    txid                 TEXT PRIMARY KEY,
    status               TEXT NOT NULL,
    status_code          INT,
    block_hash           TEXT,
    block_height         BIGINT,
    merkle_path          BYTEA,
    extra_info           TEXT,
    competing_txs        JSONB,
    raw_tx               BYTEA,
    retry_count          INT NOT NULL DEFAULT 0,
    next_retry_at        TIMESTAMPTZ,
    timestamp_at         TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    merkle_registered_at TIMESTAMPTZ
);

-- Idempotent column add for stores created before merkle_registered_at was
-- introduced. Existing rows keep NULL until the next successful /watch call
-- repopulates the marker — see issue #145.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS merkle_registered_at TIMESTAMPTZ;
-- Propagation partition key the row's message was published with, so a child
-- submitted on its own can key like its in-flight parent and reach the
-- dispatcher that holds it (dependency-aware dispatch across submissions).
-- NULL for rows written before the column existed.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS partition_key TEXT;
-- Orphaned-anchor history (issue #279): every block this tx was once MINED
-- against before a reorg superseded the anchor, as a JSONB array of
-- {blockHash, blockHeight, orphanedAt} in models.OrphanedAnchor shape,
-- capped at 5 entries by the writers. NULL for the overwhelming majority of
-- rows that never lived through a reorg.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS orphaned_anchors JSONB;
-- Last non-empty network response across a PENDING_RETRY row's durable
-- attempts, quoted by the give-up reason. NULL outside the retry queue.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS retry_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_tx_status        ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_tx_block_hash    ON transactions(block_hash);
CREATE INDEX IF NOT EXISTS idx_tx_updated       ON transactions(timestamp_at);
-- Partial index keeps the reaper's hot query from scanning the whole
-- transactions table — only the handful of rows currently in the retry
-- state are indexed.
CREATE INDEX IF NOT EXISTS idx_tx_retry_ready
    ON transactions(next_retry_at)
    WHERE status = 'PENDING_RETRY';

CREATE TABLE IF NOT EXISTS bumps (
    block_hash   TEXT PRIMARY KEY,
    block_height BIGINT NOT NULL,
    bump_data    BYTEA NOT NULL
);

-- STUMP rows are content-addressed: (block_hash, subtree_index, content_hash).
-- Several merkle-services may deliver the same subtree's STUMP; identical
-- bytes collapse to one row, divergent variants (a service that missed
-- registrations during an outage) coexist and are merged at build time.
CREATE TABLE IF NOT EXISTS stumps (
    block_hash    TEXT NOT NULL,
    subtree_index INT NOT NULL,
    content_hash  TEXT NOT NULL DEFAULT '',
    stump_data    BYTEA NOT NULL,
    PRIMARY KEY (block_hash, subtree_index, content_hash)
);
-- Idempotent upgrade for stores created with the (block_hash, subtree_index)
-- key: add the column, then swap the primary key once.
ALTER TABLE stumps ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT '';
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'stumps'::regclass AND contype = 'p' AND array_length(conkey, 1) = 3
    ) THEN
        ALTER TABLE stumps DROP CONSTRAINT IF EXISTS stumps_pkey;
        ALTER TABLE stumps ADD PRIMARY KEY (block_hash, subtree_index, content_hash);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_stump_block_hash ON stumps(block_hash);

-- Per-block processing status. One row per block hash; tracks the milestones
-- (header observed, BLOCK_PROCESSED received, compound BUMP built) and reorg
-- state. Writers use partial UPDATEs (only their own column on conflict) so
-- concurrent paths converge correctly.
CREATE TABLE IF NOT EXISTS block_processing (
    block_hash     TEXT PRIMARY KEY,
    block_height   BIGINT NOT NULL,
    header_seen_at TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    bump_built_at  TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'active',
    orphaned_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_bp_block_height  ON block_processing(block_height DESC);
CREATE INDEX IF NOT EXISTS idx_bp_status_height ON block_processing(status, block_height DESC);
-- Partial index for the bump-builder watchdog's stale-row scan. Only rows
-- that have observed a header but not yet seen BLOCK_PROCESSED are eligible
-- candidates, so the predicate keeps the index size proportional to the
-- backlog rather than the full block history.
CREATE INDEX IF NOT EXISTS idx_bp_stale_seen
    ON block_processing(header_seen_at)
    WHERE processed_at IS NULL AND status = 'active';
-- Reorg reconciliation marker (issue #279): stamped by the anchor
-- reconciler once every tx anchored to this orphaned block has been
-- re-anchored or reverted; reset to NULL when the block is resurrected.
ALTER TABLE block_processing ADD COLUMN IF NOT EXISTS reconciled_at TIMESTAMPTZ;
-- Union of the subtree indices merkle-services announced STUMPs for while
-- the block was incomplete (multi merkle-service). NULL when none retained.
ALTER TABLE block_processing ADD COLUMN IF NOT EXISTS expected_subtrees INT[];
-- Partial index over the reconciler's work queue — orphaned rows still
-- awaiting tx reconciliation. Stays proportional to the (tiny) backlog.
CREATE INDEX IF NOT EXISTS idx_bp_orphaned_unreconciled
    ON block_processing(orphaned_at)
    WHERE status = 'orphaned' AND reconciled_at IS NULL;

CREATE TABLE IF NOT EXISTS submissions (
    submission_id         TEXT PRIMARY KEY,
    txid                  TEXT NOT NULL,
    callback_url          TEXT,
    callback_token        TEXT,
    full_status_updates   BOOLEAN NOT NULL DEFAULT FALSE,
    last_delivered_status TEXT,
    retry_count           INT NOT NULL DEFAULT 0,
    next_retry_at         TIMESTAMPTZ,
    attempts              INT NOT NULL DEFAULT 0,
    last_attempt_at       TIMESTAMPTZ,
    last_result           TEXT,
    created_at            TIMESTAMPTZ NOT NULL
);
-- Idempotent column adds for stores created before the exactly-once webhook
-- delivery columns existed (commit d0a3a39). Without these, a deployed
-- database upgraded in place would silently miss last_delivered_status and
-- the CAS predicate would match no rows, leaving WebhookCASLostTotal as
-- the only outward symptom.
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS last_delivered_status TEXT;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS retry_count           INT NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS next_retry_at         TIMESTAMPTZ;
-- Delivery-attempt bookkeeping (issue #249): lifetime attempt counter plus
-- the last attempt's time and outcome, surfaced on GET /tx?callbackToken=…
-- so receivers can self-diagnose callbacks their edge is rejecting.
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS attempts              INT NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS last_attempt_at       TIMESTAMPTZ;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS last_result           TEXT;
CREATE INDEX IF NOT EXISTS idx_sub_token  ON submissions(callback_token);
-- Covering index for the txid-side access path: GetSubmissionsByTxID (webhook
-- dispatch) and TokensForTxIDs (SSE fan-out membership). Carrying
-- callback_token in the index turns the fan-out's batch resolution into an
-- Index Only Scan — measured on 200k rows: Heap Fetches 0 and 97 shared
-- buffers, against 217 with the heap fetch.
CREATE INDEX IF NOT EXISTS idx_sub_txid_token ON submissions(txid, callback_token);
-- idx_sub_txid is a strict prefix of idx_sub_txid_token, so it answers nothing
-- the composite cannot. It must be DROPPED rather than merely left alone: while
-- it exists the planner costs the narrower index lower, picks it, and pays the
-- heap fetch anyway — verified by EXPLAIN, the composite is simply never
-- chosen. Dropping it also removes one index write per submission INSERT on a
-- 1000+/s ingest path. A downgrade to a release whose schema still creates it
-- self-heals: that script's CREATE INDEX IF NOT EXISTS puts it back.
DROP INDEX IF EXISTS idx_sub_txid;
-- Partial index keyed off the webhook reaper's scan predicate; stays
-- proportional to the in-retry backlog, not the full submissions table.
CREATE INDEX IF NOT EXISTS idx_sub_retry_ready
    ON submissions(next_retry_at)
    WHERE retry_count > 0;

CREATE TABLE IF NOT EXISTS leases (
    name       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS datahub_endpoints (
    url        TEXT PRIMARY KEY,
    network    TEXT NOT NULL DEFAULT '',
    source     TEXT NOT NULL,
    last_seen  TIMESTAMPTZ NOT NULL,
    -- Policy this node advertised in the node_status fee_policy alongside the
    -- URL. NULL means "advertised none" — a configured seed, or a teranode
    -- predating fee_policy — which is why these are nullable rather than
    -- DEFAULT 0: zero is a meaningful policy value and must not be confused
    -- with absence. The upsert COALESCEs them so a write carrying no policy
    -- leaves a previously recorded one in place.
    mining_fee_satoshis         BIGINT,
    mining_fee_bytes            BIGINT,
    max_tx_size_policy          BIGINT,
    max_script_size_policy      BIGINT,
    max_tx_sigops_counts_policy BIGINT
);
-- Idempotent column add for tables created before the network scoping was
-- introduced. Existing rows keep the empty default, which excludes them from
-- every ListDatahubEndpoints query — they re-register the next time the peer
-- announces, with the correct network attached.
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT '';
-- Likewise for deployments created before endpoints carried an advertised
-- policy. Existing rows read back with a nil policy until their peer announces
-- again; no backfill is possible or needed.
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS mining_fee_satoshis         BIGINT;
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS mining_fee_bytes            BIGINT;
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS max_tx_size_policy          BIGINT;
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS max_script_size_policy      BIGINT;
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS max_tx_sigops_counts_policy BIGINT;
CREATE INDEX IF NOT EXISTS idx_dh_last_seen ON datahub_endpoints(last_seen);
CREATE INDEX IF NOT EXISTS idx_dh_network   ON datahub_endpoints(network);

-- Peer transaction policy observed from node_status announcements. Primary key
-- is peer_id (a libp2p peer runs on a single network, so peer_id is unique);
-- network is a filter column, not part of the key. Read by the policy refresher
-- / GET /policy path to compute network-wide values (issue #212). last_seen
-- drives TTL filtering.
CREATE TABLE IF NOT EXISTS peer_policies (
    peer_id                TEXT PRIMARY KEY,
    network                TEXT NOT NULL DEFAULT '',
    mining_fee_satoshis    BIGINT NOT NULL,
    mining_fee_bytes       BIGINT NOT NULL,
    max_tx_size_policy     BIGINT NOT NULL DEFAULT 0,
    max_script_size_policy BIGINT NOT NULL DEFAULT 0,
    last_seen              TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pp_network   ON peer_policies(network);
CREATE INDEX IF NOT EXISTS idx_pp_last_seen ON peer_policies(last_seen);

-- Backfill for deployments created before the size limits were observed. 0
-- means "peer has not advertised a limit"; the next node_status announcement
-- from a teranode carrying fee_policy fills them in.
ALTER TABLE peer_policies ADD COLUMN IF NOT EXISTS max_tx_size_policy     BIGINT NOT NULL DEFAULT 0;
ALTER TABLE peer_policies ADD COLUMN IF NOT EXISTS max_script_size_policy BIGINT NOT NULL DEFAULT 0;

-- API keys (rate-limiting design, 2026-10-21). Only the SHA-256 of the full
-- key is stored; hash is the lookup path on every authenticated request.
CREATE TABLE IF NOT EXISTS api_keys (
    id         TEXT PRIMARY KEY,
    hash       TEXT NOT NULL UNIQUE,
    hint       TEXT NOT NULL,
    name       TEXT NOT NULL DEFAULT '',
    tier       TEXT NOT NULL,
    status     TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_created ON api_keys(created_at, id);

-- Shared per-key, per-minute quota counters. last_grant is written by the
-- same upsert that bumps used, so ReserveQuota learns its grant atomically
-- without a second read. Rows are swept QuotaRetention after window_start.
CREATE TABLE IF NOT EXISTS api_quota (
    key_id       TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    used         BIGINT NOT NULL,
    last_grant   BIGINT NOT NULL,
    PRIMARY KEY (key_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_api_quota_window ON api_quota(window_start);
