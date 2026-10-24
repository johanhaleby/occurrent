# 150. A global position is not a saga redelivery key

Date: 2026-10-24

## Status

Accepted. Resolves [#1298](https://github.com/johanhaleby/occurrent/issues/1298). Amends
[ADR 109](0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md), which shipped in 0.32.0 and is
therefore corrected by reference rather than edited. Also changes the definition of a failing input's identity in
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md).

## Context

ADR 109 says a saga tells a redelivered event from a new one by its `streamid` together with its `streamversion`, or by
its `position`. The position half shipped in 0.31.0. An event with no stream key moved the instance's
`positionWatermark` up to its position, and `SagaExecutionSupport.isRedelivery` then treated every later event at or
below that watermark as one the saga had already applied.

That is only correct if events reach the saga in position order, and on the MongoDB event stores they do not. A store
reserves an event's position before the transaction that writes the event starts, so the event at position 41 can
commit after the event at position 42. A subscription delivers in commit order. The saga receives 42, moves the
watermark to 42, receives 41 and skips it. The saga never applies that event and nothing is logged, which is the loss
`AGENTS.md` rules out for every design. [#1230](https://github.com/johanhaleby/occurrent/issues/1230) holds the
details of how a position can be committed late.

The failure needs an event that has a position and no stream key. An event read from an Occurrent event store
has `streamid` and `streamversion`, so it takes the per-stream path and is not affected. A feed that delivers a
position without them is a custom one, and so is a `streamversion` that cannot be parsed, which the executor treated as
absent and so sent down the same path.

## Decision

**A saga recognises a redelivery by `streamid` with `streamversion` and by nothing else.** A position does not
substitute for them. An event that has only a position is keyless, exactly like an event that has nothing. Under
`RedeliveryDetection.REQUIRED` the saga refuses it with `SagaRedeliveryDetectionException`, and under `BEST_EFFORT` it
applies the event with a warning and accepts that a redelivery runs the reactions again. The default stays `REQUIRED`.

**An unreadable `streamid` or `streamversion` fails the delivery under both modes.** It is thrown after the saga has
worked out which instance the event belongs to, so an event for another saga does not trigger it. An unreadable value
means the feed is broken, which is a different thing from a feed that has no stream key. Treating it as absent is
what let a good `streamid` with a garbage `streamversion` fall through to the position path. The failure keeps the
cause, and it is never quarantined, since quarantine needs a key to identify the input it stopped on.

**`positionWatermark` is deprecated and ignored.** A stored value still loads and is never read. The executor writes
`null` for it, so the field is gone from the document after the instance's next save.

It writes `null` instead of keeping the old value. If a deployment goes back to 0.34.0, that version would otherwise
read a watermark that 0.35.0 stopped updating and skip events below it. A 0.34.0 node reads a document without the
field as `null`, and a 0.35.0 node reads one with the field, so a rolling upgrade and a return to 0.34.0 both work.

**A stored failure record whose input is `position:N` counts as no failure record.** ADR 134 identified a failing input
by its redelivery key, and for a position-only event that key was `position:N`. No event has that key any more. Keeping
such a record would charge the old failure's elapsed time to the next one and could quarantine an instance on its first
failure under the new rules. The next failure starts a fresh budget instead, and the next successful delivery clears the
record. A record that already quarantined an instance stays as it is, and the migration guide has the query that finds
those.

`EventMeta` still holds the position, since `SagaFailure.position()` reports it beside the input.

## Consequences

This breaks a feed that delivers a position and no stream key to a saga on the default `REQUIRED` setting. It shipped in
0.31.0, so it goes in the changelog as a breaking change with a migration guide section. A feed that cannot supply the
stream key chooses `BEST_EFFORT` knowingly, as ADR 109 already says it should for a feed with no metadata at all.

An event the saga applies under `BEST_EFFORT` without a key can no longer be quarantined, in the same way as any other
keyless event under ADR 134.

There is no rewrite recipe. No signature changes, the deprecated record component keeps its constructors and its
accessor, and what changes is how a delivery is treated.

ADR 109 and ADR 134 keep their text and each get a pointer in their Status section.
