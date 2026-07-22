# 63. Saga DSL

Date: 2026-07-19

## Status

Accepted. This ADR covers the whole design; it lands as a stack of PRs (`dsl/saga-dsl` module family: a
`saga-dsl/common` descriptor plus blocking-stack execution first, see the non-goals below).

## Context

Occurrent's documentation deliberately positions the Dynamic Consistency Boundary (DCB, ADR 47, ADR 52, ADR 53) as
the tool that removes the need for a saga whenever two rules must hold atomically within a single append: express
both rules as one `DcbCriteria` and let one `Decider` decide against them together. That leaves a real gap
uncovered. Some processes are not "two rules in one append" at all: they span multiple aggregates or streams, unfold
over real time, and must tolerate the possibility that a step never happens. "Cancel the order if payment isn't
reserved within 30 minutes" cannot be expressed as a DCB boundary, because there is no single append at which both
facts are known: the payment may simply never arrive, and the deciding transaction has to be triggered by the
passage of time, not by an event.

Occurrent's vocabulary is already settled here and this ADR does not reopen it: "policy" is banned as a type name in
this codebase (renamed to `SideEffect`, ADR 35, ADR 39, ADR 57, ADR 61), and "subscription" means asynchronous,
checkpointed consumption of events (ADR 43, ADR 46, ADR 51, ADR 57). What's missing is a first-class abstraction for
the case above: an asynchronous, stateful, declared-once reaction to events, and to their absence, that issues
commands. That is a saga (process manager). It is explicitly not a substitute for DCB. Where DCB applies, DCB is the
right and cheaper tool; the saga targets genuinely cross-boundary, time-involving, eventually-consistent processes.

The write side already has a self-describing decider shape (`Decider<C, S, E>`, command in, event out, ADR 15,
ADR 52) and the read side a mirrored projection shape (`Projection`, ADR 58). Both are pure data plus pure functions
over a fold, with delivery and storage left to a runner. A saga descriptor should mirror the same discipline rather
than inventing a new paradigm.

## Decision

**The descriptor is the decider's dual.** `Saga<E, S, C>` (input, state, output) mirrors `Decider<C, S, E>` (input,
state, output) with input and output swapped: a decider turns commands into events, a saga turns events (and its own
timeouts) into commands. `Saga` is pure data and pure functions, no I/O, no clock, no store, so it unit-tests exactly
like a `Decider` or a `View`: construct state, feed it inputs, assert on the returned effects, no infrastructure
involved.

**`evolve` folds, `react` decides on the post-evolve state, and the two are kept separate.** `evolve(S state, input)`
folds the input into state, mirroring `Decider.evolve`/`View.evolve`. `react(S evolvedState, input)` is called after
`evolve` and returns effects as data (see below); it never mutates state itself. This split makes replay
effect-free by construction: rehydrating a saga instance (folding its history to reconstruct current state) calls
only `evolve`, never `react`, so replay can never re-issue a command. A single merged handler that returns
`(newState, effects)` in one call was rejected for exactly this reason: replaying history through it to rebuild state
would also re-run and discard the user's effect-producing code on every replayed input, which is wasteful at best and
unsafe if that code has any observable side channel. Running `react` on the already-evolved state, not the
pre-evolve state, also lets a join step ask "am I complete now?" including the input that just arrived, and lets a
terminal transition still emit its closing effects from the state that transition produced.

**Timeouts are inputs, not a side channel.** The input alphabet is a closed sealed union,
`SagaInput<E> = Event(E) | Timeout(SagaTimeout(sagaId, timerName))`, so there is exactly one `evolve` and one
`react`, defined over events and timeouts together, not a separate timeout callback bolted on afterwards. A timeout
input carries no user payload, only the instance id and the timer name that fired: the state the saga already holds
is what `react` needs to decide what a timeout means, and keeping timeouts payload-free keeps user domain types off
the serialization boundary that timers cross (a stored timer only ever needs to round-trip `sagaId` and a name).

**Effects are data; timers carry `Duration`/`Instant`, never a `Deadline`.**
`SagaEffect<C> = IssueCommand(C) | StartTimeout(name, Duration) | StartTimeoutAt(name, Instant) | CancelTimeout(name)`.
A relative timeout carries a `Duration` and an absolute, data-derived deadline carries an `Instant`; neither carries
the deadline module's `Deadline` type. `Deadline.afterX(...)` captures `Instant.now()` at construction time, and
`react` is meant to be pure: if it constructed a `Deadline` directly, two calls with identical arguments at different
wall-clock moments would return unequal effects, which breaks effect-equality assertions in tests and smuggles a
clock into what is supposed to be a deterministic function. The executor, not the descriptor, resolves a relative
`Duration` against a clock at the moment it persists the timer. `IssueCommand` carries no routing information beyond
the command itself: a command already carries the id of whatever it targets, and the executor's command dispatcher
(a plain `ApplicationService`-shaped lambda, or an adapter over a real decider-backed `ApplicationService`) is what
routes it. This keeps the descriptor decoupled from both the storage layer and from deciders specifically:
dispatching to a non-decider receiver is first-class, not a workaround.

**Timers are polled from the saga's own state store, not scheduled through the deadline/JobRunr module.** Issue #124
suggested reusing the deadline module's JobRunr-backed scheduling for saga timeouts; this ADR rejects that in favor
of storing `StartTimeout`/`CancelTimeout` as mutations of the saga's own persisted envelope, saved atomically
(compare-and-set) together with the rest of the instance state. That makes the envelope the single source of truth
for both saga state and its pending timers, so timer bookkeeping is exactly-once by construction, with no second
store to keep in sync and no class of bugs around a fenced or orphaned scheduler job, or a timer that needs
re-arming after a crash between two stores. An executor-side `SagaTimerPoller` periodically reads instances with a
due timer and re-enters them through the same pipeline a live event would use. The trade-off is real and is recorded
here rather than discovered later: firing precision is bounded by the poll interval, which is irrelevant at the
timescale sagas operate on (minutes to days, not milliseconds), and JobRunr's operational surface (a dashboard,
retry policies, recurring-job management) does not apply to saga timers at all. The deadline module itself is
untouched and remains the right tool for general-purpose application scheduling; `saga-dsl` takes no dependency on
it.

**Correlation keys are strings, with a per-type mapping and a required fallback.** `sagaId(E) -> @Nullable
String` returns a plain `String`, not a generic identifier type, because the correlation key must round-trip
whatever the executor's store persists it as, and a generic ID type would force a conversion at that boundary for no
benefit. Correlation is declared per event type on the builder, with a `correlateAll` fallback for the remaining
types; `Saga.build()` fails at build time, not at runtime on the first mismatched event, if any handled event type
has no correlation rule. `startEventTypes()` names the events that create a new saga instance; a correlated event
that is not a start event and does not match an existing instance is skipped rather than silently starting an
instance keyed on the wrong lifecycle event.

**Consistency contract (v1): at-least-once command dispatch, exactly-once timer bookkeeping.** No transaction spans
the saga's state store, the commands it dispatches, and the subscription's checkpoint; those are three different
storage operations and forcing them into one transaction would mean coupling the saga store, the command target, and
the subscription's checkpoint store, which defeats the "dispatch to anything" and "store anywhere" design above. The
executor pipeline dispatches commands before it CAS-saves the resulting state, so a crash can duplicate a dispatch
(between dispatch and save, or on a CAS retry after a concurrent write) but can never lose one. Timer bookkeeping has
no such gap, because `StartTimeout`/`CancelTimeout` are saved atomically with the rest of the state in the same CAS
write. Duplicate dispatch is sound in practice because the recommended command receiver is an
`ApplicationService.execute`-shaped handler backed by a real decider: it re-folds the authoritative event stream on
every call and rejects a command that is already satisfied or stale, so a duplicate is a no-op rather than a double
effect. The residual race is documented rather than papered over: an event and a timeout that interleave (across two
executor nodes, or on one node between the subscription thread and the timer poller) can both attempt to react, and the
CAS loser has already dispatched its command by the time it loses, so a stale command can still reach the receiver even
though its saga-side effect is discarded. A v2 fix is designed
but not built: a document-local outbox that persists pending commands before dispatch and clears them after, giving
exactly-once dispatch. It is deliberately deferred rather than built now, because it is additive: the outbox lives
inside the same internal state envelope the CAS write already owns, so it can be added later without changing the
descriptor or the public execution contract.

**`adapt` ships in v1; `compose` is deferred.** `Saga.adapt` widens a feature saga to a broader event and command
type, mirroring `Decider.adapt` (ADR 15). `compose`, combining several sagas into one, is deferred: unlike a decider,
saga composition does not commute. Two child sagas may correlate the same event under different keys, may use
timer names from the same namespace and collide, and a "cancel everything" terminal transition in one child has no
well-defined meaning for a sibling that isn't finished. The actual place several sagas compose is the executor,
which registers many independent `Saga` descriptors side by side against one subscription; a `compose` combinator
would try to collapse that at the descriptor level and would immediately hit the three conflicts above. Recording
this here is meant to keep it from being re-proposed without addressing them.

**Blocking execution ships first; a reactor runner is additive later.** The pure execution support (evolve/react
folding, effect application, timer-due queries) lives in a stack-agnostic common module, the same layering
`projection-dsl` uses (ADR 58): a reactor `saga-dsl` runner is a facade over the same descriptor and support code,
not a rewrite. Timers are polled rather than pushed, so there is no reactive "wait for a deadline" story to design
in v1; the poller is a plain blocking loop today, and a reactive scheduler for it is deferred along with the reactor
runner itself.

**Two authoring surfaces compile onto one descriptor type.** The machine-core `Saga<E, S, C>` above is the ADR's
subject and the only shape the executor needs to know about. On top of it, a flow/step sugar layer offers linear,
declarative authoring for the common case:

```
saga {
  step("await-payment") {
    on<PaymentReserved>(then = ...) { ... }
    timeout(after = Duration.ofMinutes(30), then = ...) { ... }
  }
}
```

This compiles onto the same `Saga<E, FlowState<E>, C>` machine-core type, so the executor only ever runs one kind of
descriptor regardless of which surface authored it.

A flow saga's `FlowState` remembers the domain events it has received (joins, guards, and not-fulfilled branches all
read them), so those events are persisted. They are serialized as CloudEvents through the application's
`CloudEventConverter`, which means they persist by their stable `CloudEventTypeMapper` type, the same representation the
event store uses, rather than by a Java class name. A domain event can therefore move to a different package without
breaking in-flight flow-saga state, exactly as it can for events in the event store. A machine-core saga's state is the
user's own model and is serialized like the snapshot store; a user who embeds events in it and needs the same package
independence supplies a store that does the CloudEvent conversion. The flow layer's non-goals are stated up front so they are not
mistaken for missing features: no dynamic N-of-M joins, no real accumulators across steps, no "this event is valid in
every step" wildcard matching. A process that needs any of those drops down to the machine core directly, where
`evolve`/`react` can express them without fighting the sugar layer's linear-step model.

## Consequences

- A new, genuinely time-involving or cross-boundary process gets a first-class, pure, unit-testable descriptor
  instead of being forced into a DCB boundary it doesn't fit, or hand-rolled against a subscription with ad hoc
  timer bookkeeping.
- Replay is safe by construction: rehydrating a saga instance runs only `evolve`, so no amount of replaying history
  can re-dispatch a command.
- `react`'s effects are deterministic data (no captured clock), so saga logic is testable with plain equality
  assertions on the returned `SagaEffect` list, the same way `Decider` and `Projection` logic already is.
- Timer bookkeeping is exactly-once and needs no second store or scheduler; the cost is poll-interval-bounded firing
  precision and no JobRunr dashboard/retry surface for saga timers specifically. The deadline module is unaffected
  and `saga-dsl` has no dependency on it.
- Command dispatch is at-least-once in v1, not exactly-once. This is safe with idempotent, decider-backed receivers
  and is explicitly documented, including the one residual cross-node race a CAS retry can produce. An additive
  document-local outbox is the designed v2 fix if exactly-once dispatch becomes necessary.
- `compose` is not available. Multiple sagas run side by side by being registered independently with the executor;
  combining several into a single descriptor is left undesigned until correlation, timer namespacing, and terminal
  semantics across children have an answer.
- Only the blocking execution stack ships initially. A reactor runner is expected to be additive on top of the same
  common module, mirroring how `projection-dsl` split blocking and reactor over one frozen common API (ADR 58).
- The flow/step DSL and the machine-core descriptor are two authoring surfaces over one runtime type, so the
  executor, persistence, and timer polling are implemented once regardless of which surface a feature uses.
- A flow saga's received-event log is bounded to a configurable window, not retained in full. `FlowState` keeps the
  initiating event, the current step's events (a join must count over them), and a configurable carry-over of earlier
  events (the flow builder's `historyWindow`, default 100); older events are dropped. This removes the per-save
  O(N²) re-serialization, the per-append O(N²) in-memory copy, and the 16 MB document ceiling that unbounded history
  hit. The trade-off is a behaviour contract: a guard, a join reaction, or a timeout reaction sees only the retained
  window (the initiating event is always available), so a guard that must count history beyond the window needs a
  wider `historyWindow`. An append-only side-collection for unbounded history remains a possible future path if a
  real workload ever needs it, but no current one does, so it is not built. To keep the join-matching window
  reconstructable after the prefix is dropped, `FlowState` carries an absolute `windowStart` offset and keeps
  `stepEntryIndex` absolute.
- `FlowState` is a narrow public interface exposing only the user-meaningful surface (`currentStep`, `received`,
  `completed`, and the `receivedEvents` view). The concrete state is the record `FlowStateImpl` in the
  `org.occurrent.dsl.saga.flow.internal` package; it carries the flow lowering's transition bookkeeping and is `public`
  only so a `SagaStateStore` in another module can construct and read it. This keeps the bookkeeping off the type a user
  holds while still letting the store round-trip it. The executor and the store cast `FlowState` to `FlowStateImpl` at
  their boundaries, which is safe because the executor is the only producer of the state.
- `FlowStateImpl`'s bookkeeping fields (`stepEntryIndex`, `windowStart`, `previousStep`, `lastAction`,
  `matchedBranchIndex`) are an implementation detail of the flow lowering, not a stable wire format: their meaning can
  change between versions. A store that persists a flow saga's state (the `instanceof FlowStateImpl` serialization branch
  in a `SagaStateStore`) must round-trip whatever it wrote without interpreting them. Only `currentStep`, `received`, and
  `completed` carry user-meaningful semantics.
