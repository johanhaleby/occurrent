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
are unchanged and stay asynchronous: the knob surface argument cuts the same way for them, since `startAt`,
`resumeBehavior`, and `startupMode` are exactly what those annotations exist to configure, so folding synchronous
dispatch into them would either dead-letter those knobs for the synchronous case or force a mode switch that
re-legalizes the very combination this ADR keeps unrepresentable. ADR 59 later gives `@Projection` (and `@Snapshot`)
a `mode` attribute instead of a separate annotation. That divergence is intentional, not an oversight: a projection's
async-only knobs (the catch-up start position and resume behavior) are optional there too, so a `mode` switch does
not resurrect an illegal combination the way it would on `@Subscription`. See ADR 59 for the reconciling argument.

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

## Amendment (2026-08-04): without a transaction, a failing handler does not strand its siblings

The consequence above says that without a transaction "a handler that throws after the committed write surfaces as an
`execute` failure even though the events persist." That is true of the *throwing* handler and it remains the deliberate
trade. It says nothing about the handlers registered after it, and that turned out to be the part that mattered.

`RegisteringSubscribable.route(Iterable)` looped the handlers unguarded, so the first exception ended the loop. Under a
transaction that is harmless, because the write rolls back and no handler's work survives. Without one the write has already
committed by the time handlers run, so the handlers behind the failure never received that event and never would: a
synchronous subscription has no replay and no redelivery. A handler that did nothing wrong silently lost an event
because a sibling failed.

[ADR 90](0090-a-push-sink-feeds-one-consumer.md) made both push sinks single-consumer for exactly this coupling and
deliberately left the synchronous models fanning out, on the argument that a handler failure here fails the write
instead of stranding a sibling. **That argument holds only where a transaction is present, which is not the default.**
`GenericApplicationService` and `GenericDcbApplicationService` both default to `TransactionExecutor.noTransaction()`.
The Spring starter is the opposite: it auto-configures a `SpringTransactionExecutor` whenever the event store is
enabled, so a Spring Boot application is atomic unless it replaces that bean. So the hole was real, and narrower than
"synchronous subscriptions are broken".

### Decision

Dispatch behaves differently in the two regimes, and the application service tells the model which one it is in.

**Inside a transaction, dispatch still stops at the first failure.** The write is about to roll back, so the handlers
behind it would only do work that is discarded, and a synchronous handler can have effects outside the datastore that no
rollback undoes. Nothing is lost by stopping, so nothing changes here.

**Outside a transaction, every handler is offered every event and the failures are reported afterwards.** One failure is
rethrown exactly as it was, so a caller catching a specific type is unaffected, which is the overwhelmingly common case.
Several are reported as the first with the rest attached through `Throwable.addSuppressed`, so no new exception type
enters the public API.

**A handler that failed is skipped for the rest of that batch.** Handing it the following events would update its read model from them
without the one it failed on, which corrupts the read model rather than salvaging it. Isolation is
between handlers, never within one handler's own event order.

### How the regime reaches the model

`TransactionExecutor` and `ReactiveTransactionExecutor` gained `isTransactional()`, and the application service passes
the answer to `dispatch(List, boolean)` on the two dispatcher interfaces.

**That two-argument method is the interface's only dispatch method, and the one-argument `dispatch(List)` was removed
from both interfaces.** The first attempt added the second form as a `default` delegating to the first, so no existing
implementation had to change. That was the wrong call, and it was reversed before release. A dispatcher owns its own
handler loop, so a silent default means a third-party dispatcher keeps stranding handlers with nothing to tell its
author, and Occurrent cannot fix that from the outside. Deleting the one-argument form turns a silent wrong answer into
a compile error whose migration is to add the parameter and decide what it means, which is exactly the attention the
default fails to ask for. The alternative was not free either, since it kept a permanent two-method interface plus a
warning paragraph in each javadoc, the changelog and the migration guide. Pre-1.0, a break with a one-line migration
beats carrying a known-wrong shape into 1.0.

The models keep a one-argument `dispatch(List)` as a class method, which is what a test or an in-memory write listener
drives, and it stops at the first failure as it always did. Only the interfaces lost it.

**`isTransactional()` is answered for the moment of the call, not fixed per executor.** The application service asks
during dispatch, which runs inside `inTransaction`, so an implementation can read the live state. The two Spring
executors do: the blocking one returns `TransactionSynchronizationManager.isActualTransactionActive()` and the reactive
one reads the same thing out of the subscriber context. Both accept a caller-configured `TransactionTemplate` or
`TransactionalOperator`, so a fixed `true` would lie under `PROPAGATION_NOT_SUPPORTED` or `PROPAGATION_NEVER` and bring
back the stranding this amendment removes. `NativeMongoTransactionExecutor` does answer a fixed `true`, correctly: it
always opens or joins a session transaction and has no propagation setting that could turn that off.

On the reactive stack `isTransactional()` returns `Mono<Boolean>` rather than a `boolean`, because there the transaction
lives in the subscriber context and only a reactive answer can reach it. The asymmetry with the blocking `boolean` is
the platform's rather than a choice.

**Both defaults still mean "no transaction",** `false` and `Mono.just(false)`, for the same reason `Consumers.ONE` is
the default in ADR 90: the safe answer is the default and opting out is explicit. An executor that opens a transaction
and does not override gets isolation where fail-fast was wanted, which costs handler work inside a transaction that is
rolling back regardless. The other default would silently strand siblings, so this errs toward wasted work rather than
toward loss.

### Consequences

- **No handler is left missing an event that was committed,** which is the half of the isolation rule in `AGENTS.md`
  this closes. Read the rule's "blocked by another one being faulty" clause carefully here: under a transaction a
  handler behind a failure is still skipped, and that is not a gap, because the write it would have reacted to is rolled
  back and there is nothing left to miss. Without a transaction, where the write stands, every handler now gets the
  event. So the push sinks and the synchronous models get two different answers, because a broker message carries one
  acknowledgement and a committed write carries none.
- **A best-effort synchronous subscription can now report more than one failure at once.** A caller that inspected only
  the thrown exception still sees the first one unchanged and has to read `getSuppressed()` to see the rest.
- **Handler order across a batch is unchanged.** Events are still offered outermost and handlers innermost, in
  registration order, so a healthy handler sees exactly the sequence it saw before.
- **Fan-out on a synchronous model stays supported.** This was deliberately not solved by refusing several handlers the
  way the push sinks do. Unlike a broker queue there is no per-consumer transport to migrate to, so refusing would break
  working applications with nowhere to go.
