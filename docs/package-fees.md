# Transaction fees and unconfirmed ancestors

Arcade's `GET /policy` reports the fee rate enforced at transaction intake.
`policy.miningFee` is a ratio of satoshis to bytes; it is not a package quote
or an advertisement of child-pays-for-parent (CPFP) support.

## Current submission behavior

Both `/tx` and `/txs` validate fees for each parsed transaction. A batch is
not a fee-sharing package: an under-floor parent is rejected even if its
child pays enough for their combined size. The batch aborts before any
transaction is published for propagation. Structural and script checks still
apply independently of the fee check.

Dependency-aware propagation orders transactions around parent acceptance.
It does not aggregate fees or give an underfunded parent credit for a child.
Consequently, bypassing intake fee validation alone is not a CPFP solution.

## Admission and mining are separate policies

The legacy Bitcoin SV node can admit a transaction below its mining rate into
its secondary mempool, subject to its rolling mempool admission floor. A
paying child can then promote a connected ancestor group into the primary
mempool for mining. This does not imply that every relay in front of the node
accepts those parents. Arcade currently enforces its advertised mining rate
at admission for each transaction.

Source: Bitcoin SV node revision
[`879fc8b`](https://github.com/bitcoin-sv/bitcoin-sv/tree/879fc8b42168dd0e608dafd51b39c6dabad37d4d),
[`src/validation.cpp`](https://github.com/bitcoin-sv/bitcoin-sv/blob/879fc8b42168dd0e608dafd51b39c6dabad37d4d/src/validation.cpp)
and [`src/txmempool.cpp`](https://github.com/bitcoin-sv/bitcoin-sv/blob/879fc8b42168dd0e608dafd51b39c6dabad37d4d/src/txmempool.cpp).
The legacy node also has a [functional CPFP test](https://github.com/bitcoin-sv/bitcoin-sv/blob/879fc8b42168dd0e608dafd51b39c6dabad37d4d/test/functional/bsv-cpfp.py#L86-L132)
that checks low-fee ancestors are absent from a mining candidate until a
paying child arrives, then mines the group. This test was reviewed in source,
not executed for this change. Node source behavior is not evidence of a
particular deployment's settings.

Teranode `v0.16.0-beta-9`, the version pinned by Arcade, instead enforces the
mining fee during per-transaction policy validation, before admitting the
transaction to block assembly. Its assembly path receives validated
transactions and does not implement ancestor-fee selection. The source also
labels CPFP fee calculation as not actively implemented. The same behavior
was found on main at `40faeb0f0bfcf94baf1eaa69517b5e5889af0d89`.

Pinned Teranode sources:
[BDK fee policy](https://github.com/bsv-blockchain/teranode/blob/eec632537b940967dd705092bd63054af235c421/services/validator/ScriptVerifierGoBDK.go#L206-L224),
[admission before assembly](https://github.com/bsv-blockchain/teranode/blob/eec632537b940967dd705092bd63054af235c421/services/validator/Validator.go#L790-L824),
[CPFP setting caveat](https://github.com/bsv-blockchain/teranode/blob/eec632537b940967dd705092bd63054af235c421/settings/policy_settings.go#L16-L17),
and [candidate construction](https://github.com/bsv-blockchain/teranode/blob/eec632537b940967dd705092bd63054af235c421/services/blockassembly/BlockAssembler.go#L1407-L1553).

Accepting a valid block containing low-fee transactions is a separate consensus
operation. It does not establish that the node admits those transactions via
its normal policy path or constructs such a block itself. Disabling policy
checks to demonstrate block validity would not test CPFP support.

## Wallet implications

For an ordinary transaction, estimate its serialized transaction size and
apply the advertised rate, rounding up to whole satoshis. BEEF transport bytes
are not the transaction size used for its mining fee. Recalculate after input
selection and signing-size changes. The final wallet fee can differ from a
payload-only estimate.

Unconfirmed does not mean underfunded. Confirmed ancestors contribute no
shortfall, and sufficiently funded unconfirmed ancestors need no additional
payment. New parent transactions should meet the applicable fee policy when
they are created.

If a relay and miner explicitly support the relevant package policy, a wallet
can calculate a candidate child fee as:

```text
max(ceil(rate * childBytes),
    ceil(rate * (childBytes + ancestorBytes)) - ancestorFees)
```

Here `ancestorBytes` and `ancestorFees` describe the unique, unconfirmed
ancestors of that child. Their fees must be derived from authenticated source
output values, and confirmation state must be verified. Recompute the set
when funding inputs change; do not assume unknown ancestry is confirmed or
has no shortfall. The candidate must also satisfy the target miner's package
limits and grouping rules. This equation does not establish that Arcade or a
particular miner accepts the package.

## Acceptance prerequisite for a future implementation

A functional change needs an isolated test through the intended node version
and submission interface. Use a valid under-floor parent and a child that
covers the shortfall. Verify both admission and inclusion by the node's normal
block-template/mining path. Also exercise insufficient combined fees, invalid
scripts, missing ancestry, duplicate/shared ancestors, and already-confirmed
parents. A mocked downstream success or an HTTP success is insufficient.

If the node supports the required package policy, Arcade must validate and
forward the connected package using that supported interface, while retaining
script, value, double-spend, and resource-limit checks. If the node rejects
each under-floor transaction independently, node support is required before
Arcade can offer this behavior. Advertise a capability only once the entire
configured submission path supports it.

The regression tests accompanying this document characterize Arcade's
current per-transaction fee contract. They do not enable package acceptance
or change the advertised fee rate.
