# 56. DCB read direction and limit

Date: 2026-07-13

## Status

Accepted

## Context

A gapless business sequence &mdash; an invoice number that must run 1, 2, 3 with no holes &mdash; is one of the
canonical DCB use cases (see the invoice-number example at https://dcb.events/examples/). The pattern is: read the last
event that assigned a number, take that number plus one, and append the new event under an append condition that fails
if any matching event was committed since the read. The append condition serialises concurrent writers, so the sequence
stays gapless.

The read is the problem. Occurrent's DCB read API (`DcbReadOptions`) only carried a `PositionRange`. It had no way to
say "give me just the last matching event": a read always returned every event matching the criteria within the window,
in ascending position order. So obtaining the current highest invoice number meant reading the whole `InvoiceCreated`
history and taking the last element &mdash; O(n) in the number of invoices, growing without bound. That is fine for a
demo and wrong for anything real.

DCB sequence positions themselves cannot stand in for the business number: ADR 0021 allows gaps between reserved
position blocks, so the store's positions are not gapless. The gapless counter has to come from the matched domain
events, which is exactly why reading the last one efficiently matters.

## Decision

**Add an optional read direction and limit to `DcbReadOptions`.** The record gains a `Direction` (`FORWARD` or
`BACKWARD`, defaulting to `FORWARD`) and an `OptionalInt limit`. `direction` chooses which end of the matching range the
`limit` keeps &mdash; `FORWARD` keeps the lowest-position (oldest) matches, `BACKWARD` keeps the highest-position
(newest) matches &mdash; and `limit` caps how many are returned. The convenience factory `backwardsLimited(1)` reads a
gapless sequence's last entry in a single round trip. A three-argument canonical constructor is joined by a
source-compatible `DcbReadOptions(PositionRange)` constructor and by `backwards()`, `forwards()`, and `limit(int)`
withers, so every existing call site keeps compiling and behaving exactly as before.

**Direction and limit never change the order of the returned events.** A `DcbEventStream` still lists its events in
ascending DCB sequence-position order regardless of the options. Direction and limit only select *which* matching
events are returned, not how they are ordered. So `backwardsLimited(1)` returns the single highest-position matching
event, and `backwardsLimited(3)` returns the three highest-position matching events still in ascending order. Keeping
the returned order invariant means a caller that folds a read never has to care whether it was limited, and it avoids a
second, direction-dependent ordering contract that every store and every reader would otherwise have to agree on. The
store fetches in whatever order is efficient &mdash; MongoDB sorts descending and limits, then reverses the page back to
ascending &mdash; but that is an implementation detail behind a uniform contract.

**Direction and limit never affect the consistency token.** The `DcbConsistencyToken` a read returns reflects the whole
matching set observed at read time &mdash; the query's marker snapshot on MongoDB, the store head in memory &mdash; not
the returned page. A limited read therefore still guards an append against *any* later matching event, not just against
a change to the one event it happened to return. This is the invariant that makes the gapless-sequence pattern correct:
reading only the last invoice event still detects a concurrent invoice appended anywhere in the matching range. Each
store has a test asserting that a limited or backward read returns the same token as the equivalent unlimited forward
read.

## Consequences

- Gapless sequences read their last entry in one round trip instead of folding the whole history. The invoice-number
  example under `example/domain/dcb-patterns` uses `backwardsLimited(1)` directly on the event store, because the
  decider fold, which reads the whole matching set to build state, is the O(n) path this decision exists to avoid.
- The change is additive and backward compatible. It extends the DCB read surface frozen for consistency in ADR 0053
  rather than altering any existing behaviour; the default `FORWARD`, no-limit read is unchanged on every store.
- All four DCB-capable stores (in-memory, native MongoDB, Spring blocking, Spring reactor) implement direction and
  limit, so the contract holds uniformly across the blocking and reactor `DcbEventStore` APIs, which share the one
  `DcbReadOptions` type.
