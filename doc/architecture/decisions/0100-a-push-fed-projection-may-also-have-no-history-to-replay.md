# 100. A push-fed projection may also have no history to replay

Date: 2026-08-05

## Status

Accepted. Closes #528, the gap [ADR 96](0096-a-push-fed-saga-may-have-no-history-to-replay.md) named and deliberately
left open: `@Saga(source = PUSH)` gained a `catchup` attribute, `@Projection(source = PUSH)` did not, and the
asymmetry was which feature was built second rather than a real difference between the two.

## Context

ADR 96's reasoning for `@Saga` applies just as much to `@Projection`: a projection fed by another application's broker
has no local history to replay, so the default catch-up either finds nothing or applies unrelated events that happen
to live in the same store, and the beans it needs (`PositionOrderedReader`, `CheckpointStorage`) may not exist in the
context at all.

Closing the gap is not purely additive, though, because `@Projection` has two feed types where `@Saga` has one.
`@Saga(source = PUSH)` only accepts a `PushSubscriptionModel`, and skipping its catch-up wrapper is enough:
`Subscribable subscribable = pushModel` in place of the `CatchupThenPushSubscriptionModel`. `@Projection` also
accepts a `DomainEventFeed`, and there the same trick does not work. `DomainEventFeed.register(...)` puts the feed
into buffering mode immediately (`BlockingHandover`/`ReactiveHandover` buffer every live event until told to go live),
and only `catchUp()` ever tells it to. Skip the catch-up call for `catchup = NONE` and every live event buffers until
the fixed-size buffer overflows, with no way out.

The engine that backs `catchUp()` already has the branch this needs. `BlockingHandover.catchUp` and
`ReactiveHandover.catchUp` both short-circuit on `Source.isAlreadyCaughtUp()` straight to going live, without ever
calling `replay()`, `keepReplaying()` or `markCaughtUp()`. What was missing was a way to ask for that branch from
outside `CatchupProjectionFeed`, which hardcodes a `Source` whose `isAlreadyCaughtUp()` reads the completion marker.

## Decision

**`CatchupProjectionFeed` and `DomainEventFeed` (both stacks) gain `goLive()`/`goLive(id)`.** It supplies a `Source`
that answers `isAlreadyCaughtUp()` with `true` and treats the other three methods as unreachable, so the engine drains
whatever live events have buffered and starts delivering directly, without a replay and without writing the
completion marker. No marker means a later real `catchUp()` on the same feed still replays the full history, which is
correct: nothing was actually caught up.

This closes a hole in released API, not only `@Saga`'s original one for `@Projection`. `DomainEventFeed` and
`CatchupProjectionFeed` shipped in 0.31.0 with no way to reach the live state without a replay, so a programmatic
caller who registered and only ever called `accept` had no way out either, with or without Spring, with or without
this issue. Per the small-capability-completion rule (AGENTS.md), that earns `goLive()` a place on its own, ahead of
what `@Projection` needed it for.

**`@Projection` gains `catchup`, mirroring `@Saga`'s attribute exactly, with the same rejections.**
`FROM_EVENT_STORE` (the default) is unchanged behaviour. `NONE`: for a `PushSubscriptionModel` feed, skips the
`CatchupThenPushSubscriptionModel` wrapper and uses the bare model; for a `DomainEventFeed`, calls `register(...)`
then `goLive(id)` instead of `catchUp(id)`, in both the auto-start and the `occurrent.subscription.mode = manual`
branch. `startAt`, `startAtGlobalPosition` and `resumeBehavior` are refused under `source = PUSH` regardless of
`catchup`, `startupMode` is honoured only under the default `catchup` (there is no replay for it to move off the
startup path otherwise, so it is refused together with `NONE`), and setting `catchup` on a `source = EVENT_STORE`
projection is refused, matching `@Saga`'s reasoning: ignoring it would leave the projection reading history it was
asked to skip.

The bean-resolution message is shared. `SubscriptionAnnotations.resolveCatchupBean` replaces `@Saga`'s private
`catchupBean` helper, parameterized over the annotation name, so both annotations and both stacks point at the same
`catchup = NONE` hint when a `PositionOrderedReader` or `CheckpointStorage` bean is missing.

**The `waitUntilStarted` computation needs no `catchup` guard of its own.** `SubscriptionAnnotations
.pushCatchUpShouldWaitUntilStarted(startupMode)` already answers `true` whenever `catchup = NONE`, because the
validation above rejects every `startupMode` but `DEFAULT` in that case, and `pushCatchUpShouldWaitUntilStarted`
answers `false` only for `BACKGROUND`. Gating it on `catchup` a second time was tried and reverted during review: it
forced `waitUntilStarted = false` for a bare push subscription that never had a replay to wait for, which reintroduced
the very background-watcher machinery `catchup = NONE` has no use for.

## Consequences

`@Saga` and `@Projection` no longer diverge on `catchup`. The other asymmetry ADR 96 recorded stands: `@Saga` still
refuses a `DomainEventFeed`, because it carries no stream metadata a saga needs for redelivery detection, and that is
a real difference in what the two annotations need from a feed, not a gap.

`goLive()`'s reactor engine has one restriction its blocking twin does not. Calling `catchUp()`/`goLive()` a second
time on the same feed is harmless on the blocking stack, but on the reactor stack both subscribe the feed's one live
sink, which `Sinks.many().unicast()` accepts only one subscriber for, ever. A second call does not even error loudly:
the returned `Mono` completes before the live-phase subscription is attempted, so the rejection is silently dropped,
which `ReactiveHandover`'s own class javadoc already documents as a known gap in `catchUp()`. `goLive()` inherits this
rather than introduces it; both javadocs say "call once" for the same reason.

No migration is needed. `catchup` defaults to `FROM_EVENT_STORE`, so an existing `@Projection(source = PUSH)`
continues to catch up exactly as before.
