# 45. Unified global position for stream and DCB events

Date: 2026-07-03

## Status

Accepted

## Context

Occurrent had two unrelated ways to order and replay events. DCB events carried a global, monotonic, comparable
integer `dcbposition`, reserved from a per-store counter document, one block per append. It backed bounded-range
reads, a high-watermark read discipline, and position-windowed catch-up. Stream events carried only a per-stream
`streamId` and `streamVersion`, with no global order at all. Stream catch-up instead reconciled the history-to-live
handover using wall-clock time (or `$natural` insertion order) plus a count-stability loop.

That reconciliation approach produced real data-loss bugs under clock skew (#199) and a related in-memory snapshot
race (#200). Both fixes were mitigations of the underlying design, not a clean solve, because the stream stack had
no global, comparable ordering value to reconcile against. The reactive stack had no stream catch-up model at all,
so a reactive `@StreamSubscription` failed loud on any history replay request.

DCB was unreleased at the time of this decision, so `dcbposition` could be renamed and relocated without a
backward-compatibility shim for DCB consumers. Backward compatibility for existing stream data was the real
constraint: events already written to a stream-only store have no position field, and a migration path was needed
for anyone upgrading.

## Decision

Make `position` a first-class property of every event, stream and DCB alike, so both stacks share one ordering axis.

**Relocated to core, one vocabulary.** Position stops being a DCB-only concept. The CloudEvent extension, the stored
field, the read bounds, and the subscription token are all named `position` throughout, so a reader never has to
learn that "DCB position" and "stream position" are the same value. `DcbCloudEvents` keeps only its genuinely
DCB-specific helpers (tags, `matches`, boundary tags). The position accessors, the shared `PositionRange` type, and
the reservation methods (`reservePositions`, `currentPosition`) moved to the core and cloudevents-extension layers.

**Position is intrinsic to DCB, a stream-scoped option for STREAM.** `EventStoreCapability` stays the flat
`{STREAM, DCB}` enum, the API-family axis. Position is deliberately not a third peer value in that set: a DCB store
without a global order is not a state that should be representable, and adding position as a capability would force
a validation rule to forbid exactly that illegal combination instead of removing it by construction. So:

* A DCB store always writes position. There is no configuration knob, because DCB cannot function without a global
  order.
* A STREAM-only store treats position as an opt-out, on by default. Turn it off with the fluent
  `EventStoreConfig.withoutStreamPosition()` builder method (blocking and reactor, plus the InMemory equivalent), or
  `occurrent.event-store.stream.position=false` in Spring. This only matters for a store that genuinely never wants
  a global order, such as a per-stream projection store like `entity-history`.
* A combined `{STREAM, DCB}` store forces stream position on, because stream-written and DCB-written events share
  one position sequence once DCB is present. The builder fails fast if `withoutStreamPosition()` is combined with
  the DCB capability.
* The derived predicate `writesPosition()` (`DCB present || (STREAM present && streamPosition enabled)`) is exposed
  publicly on the event store. Every position-requiring API calls a shared `requirePosition()` guard and throws
  `UnsupportedOperationException` with a clear message when `writesPosition()` is false, rather than returning an
  empty result or throwing a null pointer exception.

**Shared position-ordered read.** A `PositionOrderedReader` capability interface reads events in `(afterExclusive,
upToInclusive]` position order for a given filter, with a `currentPosition()` high-watermark, for both blocking and
reactor variants. It reuses the same `PositionRange` the DCB read already composed, so DCB and stream position reads
share one window-and-watermark abstraction instead of duplicating it. The user-facing query DSL
(`DomainEventQueries`, blocking and reactor) exposes position-range reads directly, guarded by `requirePosition()`,
so a caller can build custom position-driven projections and reconcile stream and DCB consumers on one axis without
going through catch-up internals.

**Catch-up selects its reconciliation strategy from the store, not from DCB versus stream.** `CatchupSubscriptionModel`
already held two reconciliation modes in one class. The selection is keyed on the store's `writesPosition()`
predicate rather than on whether the subscription reads stream events or DCB events:

* When `writesPosition()` is true, catch-up uses the position-windowed range loop, the same robust mechanism DCB
  already had, now reused for stream reads via `PositionOrderedReader`. This applies uniformly whether the
  subscription is reading stream events or DCB events.
* When `writesPosition()` is false (a STREAM-only store with position opted out), the legacy time/`$natural` plus
  count-stability path stays, unchanged, as a permanent second mode. Opt-out stores see no behavior change.

DCB events within a position-ordered stream are discriminated by the presence of the DCB tags extension
(`DcbCloudEvents.isDcbEvent`), never by checking `position > 0`. Since stream position is on by default, a plain
stream event also carries a positive position, so a positivity check would misclassify it as DCB.

`ReactorStreamCatchupSubscriptionModel` was added to mirror the existing `ReactorDcbCatchupSubscriptionModel`,
reusing the same handover cache and resume-token machinery. Reactive `@StreamSubscription` history replay is now
supported when position is on. The fail-loud error in the reactive annotation processor now fires only when
position is off for that store.

Because `GlobalSubscriptionPosition` (renamed from `DcbSubscriptionPosition`) and the legacy `TimeBasedSubscriptionPosition`
coexist and are both self-describing, a store that flips `streamPosition` on after backfill can encounter a
persisted subscription with a stale time-based token even though catch-up is now in position mode. Position-mode
catch-up detects that case and performs a defined one-time handoff rather than trusting the stale token.

**On-by-default carries an upgrade hazard, mitigated with a startup guard and a migration module.** Since stream
position defaults on, an existing deployment that upgrades without backfilling gets position on new stream events
but not on historical ones. On store initialization, when `writesPosition()` is true and the event collection
contains events without a `position` field, the store logs a loud warning naming the migration runbook, with a
config flag to escalate to a hard failure instead
(`occurrent.event-store.position.require-backfilled-position` /
`EventStoreConfig.Builder#requireBackfilledPosition`).

**When position is only on by default, an existing un-backfilled store turns it off at startup instead of enabling
it.** The warning above still fires for an explicit choice (`withStreamPosition()` or the property) and for DCB,
which always writes position. But if the store never made an explicit choice and finds an event collection that
already holds events without a `position`, it disables stream position for that store and logs how to turn it on.
This protects the upgrade path where someone bumps the Occurrent version on an existing large store without thinking
about position: it avoids building the `position` index over the whole collection at startup and avoids writing
positions onto a store whose history is not backfilled. The probe is cheap and does not need the position index. It
reads the oldest event by `_id` (always indexed) and treats a missing `position` on it as an un-backfilled store,
because the backfill assigns positions in `_id` order, oldest first. The catch-up path keys off the store's runtime
`writesPosition()`, so a store that disables position this way automatically stays on the legacy time-based catch-up
with no further wiring. This applies to the three MongoDB stores. The in-memory store always starts empty, so it
has no such hazard.

A new module, `eventstore/migration/position-backfill`, provides a runnable backfill tool rather than a one-off
script. It seeds the position counter above the true historical event count (using an accurate count, not an
estimate), then backfills `position` onto existing events ordered by `_id` rather than by any time field, so it
carries no clock-skew risk. The tool is throttled, resumable, and idempotent, and it reuses the store's own
`reservePositions` call and document mapper so it cannot drift from the real schema. The safe upgrade sequence is
documented in `doc/runbooks/position-backfill.md`.

## Consequences

Stream and DCB events share one ordering axis. This removes the #199 class of bug for streams (position-windowed
catch-up cannot be fooled by clock skew the way time-based reconciliation could), makes reactive stream catch-up
possible, and lets a consumer read a bounded position range across stream and DCB events through one query DSL.

The on-by-default choice means most new deployments get position without any action, but an existing deployment
upgrading in place must follow the migration runbook, or its stream catch-up silently stays on the legacy
time-based path (protected by the startup guard) until the backfill completes. This is the single most important
operational fact this feature introduces, and it is called out again in the changelog.

A store that genuinely never wants a global position, such as `entity-history`, can opt out and pays no counter
cost, keeping the pre-existing behavior and the legacy catch-up path exactly as before.

A combined `{STREAM, DCB}` reactive store replays both stream and DCB history through one dual-mode catch-up model
that routes each subscription to the stream or DCB path by its filter and start position, matching the blocking
stack. Reactive stream catch-up no longer depends on the DCB API, so a STREAM-only reactive store can replay
history without the DCB module on its classpath. The blocking stream catch-up is DCB-free the same way, split into
a `stream-catchup-subscription` module with no DCB dependency and a `dcb-catchup-subscription` module that holds the
`CatchupSubscriptionModel` dispatcher. Both stacks route each subscription to the stream or DCB path by filter type
first, since a global position start alone does not tell the two apart.
