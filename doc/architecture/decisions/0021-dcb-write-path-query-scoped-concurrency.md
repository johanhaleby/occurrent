# 21. DCB write-path query-scoped concurrency

Date: 2026-06-22

## Status

Accepted

## Context

`SpringMongoEventStore.appendDcb` appends DCB events under an optional `DcbAppendCondition`, which is the DCB optimistic-concurrency check: fail the append if any event matching the condition's `DcbQuery` exists after the position the command read. The current implementation has three concurrency problems.

1. Every append reserves its global `dcbposition` block with a `findAndModify` `$inc` on a single counter document, inside the append transaction. Under concurrency that one document is a hot spot. Concurrent appends contend on it and MongoDB raises a `TransientTransactionError`, which aborts the whole transaction.

2. There is no retry for transient transaction errors. `MongoTransactionManager` does not retry them, and the only retry in play is the application service retrying `DcbAppendConditionNotFulfilledException`. So contention surfaces as a spurious command failure rather than a retry.

3. The conflict-serialization keying is not skew-safe. The append writes per-key checkpoint documents to force concurrent appends on the same boundary to conflict, but it keys a tag item as `tag:<tag>` and a type-only item as `type:<type>`. Two overlapping queries that match the same event by different dimensions, for example `types(X)` and `tagsAllOf("t")` both matching an event `{type:X, tag:t}`, get disjoint keys, so they do not conflict. Today the global counter serialization in problem 1 hides this, because every append contends on the counter anyway. Remove that serialization for scalability and write skew becomes possible: two commands each read a boundary, each append a matching event, and both commit even though the second should have failed.

The goal is query-scoped concurrency. Appends to disjoint boundaries proceed in parallel, appends to the same boundary serialize, and no pair of appends that can match a common event is ever allowed to both commit when one should fail.

## Decision

### Assign positions outside the transaction

Move the `dcbposition` counter `findAndModify` out of the append transaction. It is a single atomic update, which MongoDB serializes efficiently at the document level without raising a transaction conflict. The reserved block is reused if the transaction is retried, and is simply abandoned if the append fails its condition. `dcbposition` stays strictly increasing but may have gaps, which the DCB contract permits (ADR 0017). The unique sparse index on `dcbposition` still guarantees no two events share a position.

### Detect and serialize conflicts with versioned per-attribute markers

Assigning positions outside the transaction has a consequence: a reader can observe a global head that runs ahead of the data it can see, because a concurrent append reserves its position block before it commits. A conflict check phrased over positions ("fail if a matching event exists after the position I read") is therefore unsound. The reader's boundary can sit past an append that is still in flight, which then commits at a position at or below that boundary and is never seen, so the conflict is lost silently. The conflict check must not depend on positions.

Instead, every distinct tag and type has a marker document carrying a monotonically increasing version. An append derives its marker keys from per-attribute units, never combined:

- For a tag `t`, the key `tag:t`.
- For a type `X`, the key `type:X`.

Inside the transaction the append increments the marker version for the union of:

- the appended events' keys (every tag and the type of each appended event), on every append whether or not it carries a condition, and
- the condition query's keys when a condition is present (every tag and every included type named by any query item).

Incrementing a shared marker document forces a write-write conflict in MongoDB, so two transactions that touch a common marker cannot both commit. The sharing argument is unchanged: if append A1 commits an event E1 that matches append A2's query Q2, then some item of Q2 is satisfied by E1, so A1 touches that item's `tag:t` or `type:X` marker from its event keys and A2 touches the same marker from its query keys, and the two serialize. The reverse direction (E2 matching Q1) holds by the same argument. Because an unconditional append also increments its events' markers, it cannot slip past a concurrent conditional append on an overlapping tag or type.

A read captures a consistency token for its query: the sum of the versions of the query's markers. Versions are incremented at commit and never at reservation, so the token reflects only committed appends and is immune to the position overshoot above. The authoritative conflict check compares the token the condition carries against the current token inside the transaction. If any of the query's markers advanced since the read, a matching append committed in between, so the condition fails. The marker increments still force serialization, so the loser re-runs this comparison against the winner's committed increment rather than racing it under snapshot isolation.

The token is distinct from the read's `lastSequencePosition`. The position remains a global cursor for replay and catch-up. The token is the optimistic-concurrency boundary, and the two are different types (`DcbConsistencyToken` versus `long`) so a caller cannot pass one where the other is required.

### Retry transient transaction errors

Wrap the append transaction in a bounded retry that recognises MongoDB transient transaction errors (the `TransientTransactionError` error label, surfaced by Spring as `UncategorizedMongoDbException` / `MongoException.hasErrorLabel`). On retry the reserved position block is reused and the condition is re-evaluated, so a retry either commits or fails the condition correctly. `DcbAppendConditionNotFulfilledException` is not retried here, it propagates to the application service, which re-reads and retries the command.

### A MatchAll condition is a whole-store lock

A `MatchAll` condition matches every event, so no per-attribute marker can be shared with an arbitrary concurrent append without making every append write a single global marker, which is global serialization. A `MatchAll` append condition is therefore a whole-store optimistic lock keyed `all`: it is skew-safe against other `MatchAll` conditions, but it is NOT skew-safe against a concurrent tag-scoped or type-scoped append (the two share no marker, so both can commit even when the `MatchAll` condition should have failed). This is a correctness limitation under concurrency, not merely a performance one, so a `MatchAll` append condition must not be relied on as a consistency boundary on a store that takes concurrent writes. It is fine on a single-writer store or as a one-shot "the store is empty" guard. Tag-scoped and type-scoped boundaries, the normal DCB usage, are fully skew-safe.

A query item with neither types nor tags cannot be constructed (`DcbQueryItem` rejects it), so there is no separate negative-only or empty-item case to reason about. An item always carries at least one positive tag or type, which is what makes its marker shareable. Excluded types only narrow a positive item and never remove its positive marker keys, so they do not affect skew-safety. They do reduce precision: see the over-approximation note in the consequences.

## Consequences

Positive:

- Appends to disjoint boundaries no longer contend on a global counter or a global checkpoint, so they scale concurrently.
- The conflict keying is skew-safe for tag-scoped and type-scoped boundaries, proven against the type-versus-tag, tag-versus-tag, and type-versus-type overlap cases by adversarial multi-threaded tests.
- Contention surfaces as a retry, not a spurious command failure.
- The conflict check is a lookup of the query's marker documents by `_id`, which is always index-backed, and the read query is index-backed by the `dcbtags` and `dcbposition` indexes, confirmed with `explain`.
- The conflict check is decoupled from positions, so it is sound even though positions are assigned outside the transaction and the read head can run ahead of committed data.

Negative:

- `dcbposition` can have gaps, because a reserved block is abandoned when an append fails its condition or exhausts its transient retries. DCB only requires monotonic increasing positions, so gaps are acceptable, but callers must not assume contiguity (they already must not, per ADR 0017).
- The marker collection holds one document per distinct tag and per distinct type that has taken part in an append. For high-cardinality tags, for example a tag per entity, this is one marker per entity. That is the natural granularity of the consistency boundary and matches the cardinality the boundary already implies. Markers are not removed when an event stream is deleted, and there is no automatic reclamation. A marker only carries a version counter, so it is safe to drop a marker while no append that references it is in flight: the next append that touches that key recreates it with the upsert. An operator can therefore prune the checkpoint collection during quiescence if marker cardinality becomes a concern, at the cost of resetting those counters (which is harmless, because a reader's token is only ever compared against the same store's current value).
- A `MatchAll` or negative-only append condition is a whole-store lock as described above. This is a documented limitation, not a silent one.
- The token-based check is a safe over-approximation. It is derived from positive markers only, so it cannot represent excluded types or multi-attribute conjunctions precisely. An append of an excluded-type event that still carries a query's positive tag, or an append that carries only some of a conjunction's tags, advances a shared marker and conservatively conflicts even though it does not match the full query. This never misses a real conflict, reads still apply excluded types and conjunctions precisely, and the false conflict self-heals through the application service, which re-reads the still-filtered boundary and retries. The cost is reduced concurrency between appends on a shared tag or type, which is the boundary the caller chose.
