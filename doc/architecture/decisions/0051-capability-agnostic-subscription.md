# 51. Capability-agnostic `@Subscription` as the default

Date: 2026-07-06

## Status

Accepted

## Context

A subscription forced a choice between `@StreamSubscription`, which sees stream events filtered by an Occurrent
`Filter`, and `@DcbSubscription`, which sees DCB events filtered by tags. Most consumers do not fit either box. A
projection or a read model reacts to events by type and does not care which write model produced them. Forcing such a
consumer to pick a capability is asking a question it has no reason to answer, and picking the wrong one silently drops
the other half of the history.

The capability distinction only ever leaked into subscriptions through one seam: the catch-up start position.
[ADR 24](0024-stream-and-dcb-subscription-model-split.md) split the subscription model and start-position types into
stream and DCB forms for a load-bearing reason. Stream catch-up started at a time and DCB catch-up started at a
`dcbposition`, the two position types were incompatible, and DCB carried a `tags()` boundary that stream had no
equivalent for. A shared `StartAt` let a caller hand a DCB subscription a time-based start or a stream subscription a
DCB position, and the mismatch failed quietly at runtime. The split turned that whole class of mismatch into a compile
error.

Two later changes removed the ground that split stood on for the neutral case. [ADR 45](0045-unified-global-position.md)
made `position` a first-class property of every event, stream and DCB alike, so both stacks now share one global,
comparable ordering axis instead of two disjoint ones. `GlobalCheckpoint` (`position:N`) is already the
capability-neutral resume token that both existing catch-up models persist and reload. Live delivery was already
capability-agnostic, because the raw change stream sees every event regardless of capability.
[ADR 50](0050-stream-catchup-subscriptions-exclude-dcb-events.md) was what made stream subscriptions capability-scoped
at all, by ANDing `Filter.capability(STREAM)` into `StreamCatchupSubscriptionModel`. Without that guard the underlying
machinery is neutral by construction. So a subscription that wants both stream and DCB events, filtered only by type,
no longer needs a DCB-specific position or a stream-specific time. It needs the global position that now exists for
every event, and that position removes the reason the type split reached the neutral case.

## Decision

**Revive `@Subscription` and the `Subscriptions` DSL as the capability-neutral default.** Both currently exist only as
deprecated aliases, `@Subscription` for `@StreamSubscription` and `Subscriptions` as a subclass of
`StreamSubscriptions`. They become the neutral form that delivers both stream and DCB events, filtered only by the
generic `Filter` over event types. There is no capability parameter and no `tags()`, because the neutral subscription
does not scope by write model. This is the default a projection or read model reaches for when it just wants events by
type.

**Catch-up runs over the unified global position and resumes via `GlobalCheckpoint`.** The neutral subscription reuses
the position-based catch-up path that already exists for stream subscriptions, with one difference: it omits the
`Filter.capability(STREAM)` guard that ADR 50 (#282) added. The caller's own type `Filter` is still honored. No
capability guard is composed on top of it, so both stream and DCB events matching the type filter are replayed and then
handed over to the already-capability-agnostic live change stream. The handover is the same one the stream path uses,
minus the guard.

**Type safety is preserved the way ADR 24 intended.** The neutral subscription exposes only capability-neutral start
positions: now, beginning, the subscription model default, and an explicit global position. It never exposes a
DCB-specific position, a stream-only time-based position, or `tags`. There is therefore no mismatched combination for a
caller to form, so the property ADR 24 established (illegal starts are unrepresentable rather than quietly wrong) holds
for the neutral form as well. The neutral form does not widen the surface back into the union that ADR 24 closed. It
adds a third, strictly neutral surface next to the two scoped ones.

**A neutral filter marker and a third dispatch route.** The neutral subscription carries a capability-neutral
`SubscriptionFilter` marker, distinct from the stream and DCB markers, and the catch-up dispatcher gains a third route
for it alongside the stream and DCB routes that [ADR 25](0025-dual-mode-catch-up-for-stream-and-dcb-applications.md)
established. The neutral route is the position-based catch-up without the capability guard. This ADR fixes the decision,
not the class layout. The concrete type and method names beyond `@Subscription` and `Subscriptions` are left to the
implementation.

**`@StreamSubscription` and `@DcbSubscription` are unchanged.** The stream annotation and DSL remain the explicit
stream-scoped form, capability-guarded per ADR 50. The DCB annotation and DSL remain the explicit DCB-scoped form,
tag-filtered per [ADR 27](0027-add-the-dcbsubscription-annotation.md). A caller who genuinely wants one capability
still says so. The neutral form is the default, not a replacement for the scoped ones.

## Consequences

`@Subscription` changes meaning from stream-only to capability-neutral. This is safe. The change only manifests on a
store that has the DCB capability enabled, and those stores are new because DCB is unreleased, so no existing deployment
observes a behavior change. On a stream-only store the neutral subscription is identical to what `@Subscription`
delivered before, because such a store has no DCB events for the dropped guard to have excluded.

The relationship to the prior subscription ADRs is as follows. ADR 24's start-position type safety is upheld, because
the neutral subscription exposes only neutral start positions and forms no new mismatch. ADR 25's dual-mode dispatch
gains a third neutral route next to its stream and DCB routes. ADR 27's `@DcbSubscription` is untouched. ADR 45's
unified position is the enabler that makes a single neutral catch-up axis possible. ADR 50's capability filter is simply
omitted for the neutral case, which is the whole mechanism of this decision.

On a legacy store that predates the unified global position, an event has no position to catch up over, so the neutral
subscription degrades to time-based catch-up over stream events. This is correct rather than a limitation, because a
store old enough to lack a global position also has no DCB events, so there is nothing for the neutral form to miss
there.
