# 57. Synchronous subscriptions

Date: 2026-07-14

## Status

Accepted

## Context

Occurrent has two ways to react to newly written events, and neither runs a decoupled, declared-once handler
synchronously in the write path.

The application-service `SideEffect` runs synchronously after the write, but it is a per-`execute` closure
passed at the call site, so the reaction is coupled to every call site rather than declared once. Subscriptions
(`@Subscription`, the `Subscriptions` DSL, `SubscriptionModel`) are declared once and decoupled, but every shipped
model is asynchronous: driven by a MongoDB change stream (which emits only after commit), a catch-up replay, or an
in-memory background dispatcher thread. None runs on the writer's thread, so none can update a projection inside the
write transaction.

The need is for a subscription that runs synchronously, before `execute` returns, so a projection can be updated in
the write path and, when a transaction is available, atomically with the event write. It must work without Spring
(the native driver must not depend on Spring), in which case it is best-effort.

Prior art confirms the shape. Axon models this as two processor types: a Subscribing Event Processor runs in the
publishing thread inside the publish transaction and can roll it back, a Tracking Event Processor runs on its own
thread with a token store for replay. The sync-versus-async choice is per-processor configuration, never
per-published-event. Fraktalio fmodel keeps the core pure and lets you choose where the materialized view's handler
runs. Both say the same thing: the handler decides sync-versus-async, not the command call.

## Decision

**The decision lives with the subscription, not the call.** A subscription is declared either asynchronous (as
today) or synchronous, via a separate `@SynchronousSubscription` annotation. There is no per-`execute` flag and no
`mode` attribute on `@Subscription`. A separate annotation makes illegal states unrepresentable: it carries only an
id, event types, and a filter, none of the async-only knobs (`startAt`, `resumeBehavior`, `startupMode`) that have no
meaning for synchronous, at-write-time dispatch. The existing `@Subscription`/`@StreamSubscription`/`@DcbSubscription`
are unchanged and stay asynchronous, following "mark the exception, not the default": in Occurrent "subscription"
already denotes the asynchronous mechanism, so only the new special case gets an adjective.

**A register-only `SynchronousSubscriptionModel` implements the existing `Subscribable`,** so the `Subscriptions`
DSL and the annotation front-ends target it unchanged. It has no lifecycle, start position, checkpoint, catch-up, or
replay: it only reacts to the events fed to it here and now, invoking matching handlers in registration order on the
calling thread. The in-process filter matcher was extracted from `InMemorySubscriptionModel` into a shared
`SubscriptionFilterMatcher` and reused, so one agnostic synchronous model covers both stream and DCB (a stream filter
matches only stream events, a DCB filter only DCB events).

Because the model reuses `Subscribable`, its `subscribe(...)` still takes a `StartAt`. The synchronous model
ignores it (there is no start position to honor for at-write-time dispatch), but the callers, including the
`@SynchronousSubscription` annotation processors, pass `StartAt.subscriptionModelDefault()` rather than `null`. Reusing
the shared interface is only sound if we do not quietly violate its contract: a `null` `StartAt` would be a latent NPE
if the model ever grew a real implementation or if nullness checking were enabled, so the seam stays honest even where
the value is unused.

**The application service holds two optional, storage-neutral collaborators** (configured via a new `builder(...)`):
a `SynchronousEventDispatcher` (the model) and a `TransactionExecutor`. The dispatcher seam lives in the
application-service layer so the application service does not depend on the subscription modules above it. After a
write that produces events the service re-reads the just-written events, enriched by the store with stream version
and global position (a paginated tail read for stream, a position-range read of the appended block for DCB), and
dispatches them, all inside the transaction executor. This is done only when at least one synchronous subscription is
registered, so an application that uses none pays nothing. The application service also enters the
`TransactionExecutor` only when there is synchronous dispatch to make atomic. Even where a real executor is wired (the
Spring starter wires one by default), an `execute` with no synchronous subscription runs the read/decide/write exactly
as before this feature, with no application-service transaction wrapped around it.

**Transactions are opt-in and best-effort by default.** `TransactionExecutor` defaults to `noTransaction()` (a
pass-through). A Spring-backed executor (`TransactionTemplate` / `TransactionalOperator`) makes the write and the
same-datastore handlers commit atomically without the core depending on Spring and without a call-site
`@Transactional`. A native `ClientSession`-backed executor gives the same guarantee for no-Spring apps by making the
native store's write and append join a thread-bound session. Handler-side `@Transactional` composes: it joins the
write's transaction on the same datastore, or opens an independent one on a different datastore, which requires the
annotation processor to invoke the handler through its Spring proxy rather than the raw target.

## Consequences

- **No double invocation.** A synchronous handler is registered only on the synchronous model, never on the async
  change-stream model, so the change stream does not re-fire it. One handler must never be registered on both.
- **Single writer, local only.** A synchronous subscription reacts only to events written through the local
  application-service instance, on the writer thread. For cluster-wide reaction, use an async subscription.
- **No replay, and a reliability boundary.** With a transaction executor, handler and write are atomic
  (exactly-once relative to the write). Without one (best-effort), a crash after commit but before the handler runs
  loses the reaction, and a handler that throws after the committed write surfaces as an `execute` failure even
  though the events persist. This is the deliberate trade for running in the write path.
- **No free lunch.** Enabling synchronous subscriptions adds one read per event-producing write, to recover the
  global position for handler metadata, paid only while at least one synchronous subscription is registered.
- **Cross-datastore handlers are never atomic** with the event write (no distributed transaction).
