# 56. Composable DCB read selection

Date: 2026-07-13
Amended: 2026-07-27

## Status

Accepted

## Context

A gapless business sequence, such as invoice numbers that must run 1, 2, 3 with no holes, is one of the DCB use cases
described at https://dcb.events/examples/. The pattern reads the last event that assigned a number, takes that number
plus one, and appends the new event under a condition that fails if any matching event was committed since the read.
The append condition serialises concurrent writers, so the sequence stays gapless.

The read is the problem. Occurrent's DCB read API (`DcbReadOptions`) only carried a `PositionRange`. It had no way to
say "give me just the last matching event". A read always returned every event matching the criteria within the window
in ascending position order. Obtaining the current highest invoice number therefore meant reading the whole
`InvoiceCreated` history and taking the last element, with a cost that grows with the number of invoices.

DCB sequence positions themselves cannot stand in for the business number: ADR 0021 allows gaps between reserved
position blocks, so the store's positions are not gapless. The gapless counter has to come from the matched domain
events, which is exactly why reading the last one efficiently matters.

## Decision

**Make the parts of a DCB read composable.** `DcbReadOptions` contains a `PositionRange`, a `Direction` (`FORWARD` or
`BACKWARD`), a non-negative `skip`, and an optional positive `limit`. The static position factories create forward
options with no skip or limit. Callers then compose the selection they need with `forwards()`, `backwards()`,
`skip(int)`, and `limit(int)`. For example, `fromBeginning().backwards().limit(1)` reads a gapless sequence's newest
entry in one round trip.

Direction determines which end selection starts from. `FORWARD` starts with the lowest-position matches and
`BACKWARD` starts with the highest-position matches. Skip is relative to that direction and is applied before the
limit. `fromBeginning().backwards().skip(1).limit(2)` therefore skips the newest match and selects the 2 matches before
it.

The one-argument `DcbReadOptions(PositionRange)` constructor remains and retains the original forward, zero-skip,
unlimited behavior. The three-argument constructor remains as a source-compatible bridge and supplies a zero skip.

**Direction, skip, and limit never change the order of the returned events.** A `DcbEventStream` always lists its
events in ascending DCB sequence-position order. The options select which matching events are returned. For example,
`fromBeginning().backwards().limit(3)` returns the 3 highest-position matching events in ascending order. A caller can
fold every returned stream in the same order regardless of how the events were selected. A store may fetch them in a
different order internally. MongoDB can sort descending, skip, and limit for a backward selection, then reverse the
result before returning it.

**Direction, skip, and limit never affect the consistency token.** The `DcbConsistencyToken` reflects the whole
matching set observed at read time, not the returned subset. A partial read therefore still guards an append against
any later matching event. This makes the gapless-sequence pattern correct, because reading only the newest invoice
event still detects a concurrent matching append. Each store verifies that a partial or backward read returns the same
token as the equivalent unlimited forward read.

## Consequences

- Gapless sequences read their last entry in one round trip instead of folding the whole history. The invoice-number
  example under `example/domain/dcb-patterns` uses `fromBeginning().backwards().limit(1)` directly on the event store,
  because a decider would fold the whole matching set to build state.
- The change is additive and backward compatible. It extends the DCB read surface frozen for consistency in ADR 0053
  while preserving the original forward, zero-skip, unlimited defaults.
- All four DCB-capable stores (in-memory, native MongoDB, Spring blocking, Spring reactor) implement direction and
  skip followed by limit, so the contract is uniform across the blocking and reactor `DcbEventStore` APIs.
- A large `skip` can be expensive on MongoDB because the server must advance past the skipped matches before returning
  the selected events. Position ranges or a more specific `DcbCriteria` should be preferred when they can express the
  same boundary.
