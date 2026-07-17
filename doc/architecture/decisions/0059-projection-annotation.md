# 59. The @Projection annotation

Date: 2026-07-15

## Status

Accepted

## Context

ADR 58 gave the read side a descriptor, `Projection`/`DcbProjection`, and runners that turn one into a subscription
or a query: `ProjectionRunner`, `DcbProjectionRunner`, and their reactor counterparts. That ADR deliberately deferred
an annotation, since a convenience layer over a programmatic API is a later step, not the API itself.

The write side already has that convenience layer, and so does the raw subscription mechanism underneath the
Projection DSL. `@DcbSubscription` and `@StreamSubscription` declare a persistent, framework-managed subscription in
one line, and the framework wires it into the Spring composite subscription model, so it catches up from history and
resumes from a durable checkpoint without the caller assembling that model by hand. The Projection DSL had no
equivalent. A persistent, catch-up read model still meant either writing a `@DcbSubscription`/`@StreamSubscription`
method by hand and materializing the view inside it, duplicating the wiring the annotation already does for a plain
subscription, or driving `DcbProjectionRunner`/`ProjectionRunner` programmatically and assembling the catch-up and
checkpoint infrastructure yourself.

The gap was one level of convenience, not a missing capability. `DcbProjectionRunner` and `ProjectionRunner` already
catch up from history and resume durably whenever the `SubscriptionModel` they are given supports it. What was
missing was a one-line declarative path from a `Projection`/`DcbProjection` factory method to a persistent, managed
subscription, the read-model equivalent of `@DcbSubscription`.

## Decision

**A `@Projection` method annotation marks a factory method that returns a `Projection` or `DcbProjection`, and
registers it as a persistent read model on both the blocking and reactor stacks.** The method's return type selects
the path: a `DcbProjection` always goes through the DCB subscription path, a `Projection` goes through the
capability-agnostic or stream-only path depending on the annotation's `capability` attribute. Under the hood this is
`DcbProjectionRunner`/`ProjectionRunner` (or their reactor counterparts) wired to the same subscription
infrastructure `@DcbSubscription`/`@StreamSubscription` already use, not a new mechanism.

**Catch-up and durable resume come from the subscription model `@Projection` subscribes through, not from the DSL
classes.** In the Spring starter that model is a composite: a competing-consumer layer for single-active-instance
delivery, wrapping a catch-up layer that replays history by position (in DCB mode for a `DcbProjection`), wrapping a
durable layer that checkpoints progress, wrapping the native Mongo subscription model. `@Projection` gets catch-up
and durable resume by subscribing through that composite, exactly the way `@DcbSubscription` and `@StreamSubscription`
already do. `Projection`/`DcbProjection` and their runners stay exactly what ADR 58 described, a descriptor and a
plain subscription-to-materialization bridge with no opinion on delivery infrastructure.

**Materialization is store-agnostic through `ViewStateRepository`.** The `store` attribute names a Spring bean
implementing `ViewStateRepository`, or `MaterializedView`, or a Spring Data `CrudRepository` adapted to one of those,
and an empty name resolves the store by convention, typically the Mongo default. A non-Mongo read-model store is a
first-class target, not an afterthought bolted onto a Mongo-shaped default, since `ViewStateRepository` is the same
seam `DcbProjectionRunner`/`ProjectionRunner` already materialize into.

**`@Projection` supports two delivery modes, async and synchronous, and the two are mutually exclusive with the
catch-up start knobs.** The default, `Mode.ASYNC`, is the catch-up-then-live subscription described above. `Mode.SYNCHRONOUS`
instead reuses the synchronous subscription model from ADR 57, dispatching on the writer thread before `execute`
returns for read-your-writes. A synchronous projection has no history to catch up on and no checkpoint to resume
from, so `startAt`, `startAtPosition`, and `resumeBehavior` have no meaning there, and the annotation processor
rejects setting any of them together with `mode = SYNCHRONOUS`.

**This `mode` attribute is not the per-`execute` flag ADR 57 argued against, and `@Snapshot` follows the same choice for
the same reason.** ADR 57 rejected a `mode` attribute on `@Subscription` because "subscription" already names Occurrent's
asynchronous delivery mechanism specifically, so async-only knobs such as `startAt` and `resumeBehavior` would be
meaningless noise on a synchronous variant, and it introduced `@SynchronousSubscription` as the separate, narrower
annotation instead. A projection is a different shape of problem: it is one concept, a read model kept current from a
stream of events, with two delivery timings, not two different mechanisms. `@Projection` and `@Snapshot` therefore give
the read model one annotation with a `mode` attribute, and the processor still makes the illegal combination
unrepresentable by rejecting `startAt`, `startAtPosition`, and `resumeBehavior` together with `mode = SYNCHRONOUS`, which
is the same safety ADR 57 gets from a separate annotation, reached by validation instead of by type.

**The same catch-up and durable resume are reachable programmatically, without the Spring starter.** A caller who
wires their own catch-up-capable subscription model, for example a hand-wired `CatchupSubscriptionModel` over a
native driver, gets the replay-first-boot-then-resume-from-checkpoint behavior through the new
`ResumeStartPositions.replayThenResume(...)`/`replayThenResumeDcb(...)` helpers (blocking and reactor), passed as the
`StartAt`/`DcbStartAt` to `ProjectionRunner`/`DcbProjectionRunner`. `@Projection`'s `resumeBehavior = DEFAULT` handling
is built on the same rule, so the annotated and the programmatic path agree on what "resume" means.

**The prefactor kept in scope was adding `ResumeStartPositions`, not refactoring the existing bean post-processors.**
`OccurrentBlockingAnnotationBeanPostProcessor` and `OccurrentReactiveAnnotationBeanPostProcessor` already implement
this replay-then-resume gate privately, once per stack, for `@StreamSubscription` and `@DcbSubscription`. This
decision adds `ResumeStartPositions` as a new, additive, Spring-free public helper so a non-Spring caller and
`@Projection` can reuse the same rule, and leaves those existing bean post-processors as they are rather than
rewriting them to call it. The two stacks' `SAME_AS_START_AT` handling also stays duplicated in-module per stack
rather than pulled into one shared implementation, because the blocking and reactor stacks legitimately differ here,
for example in how they disable the competing-consumer layer, and forcing a shared abstraction over that difference
would cost more than the duplication does.

## Consequences

- The fold stays metadata-free, the same way `View.evolve(state, event)` already is under ADR 58. A `Projection`'s
  fold does not receive the stream id or position, only the event, so a handler that needs that context reads it from
  the event's own fields, not from delivery metadata.
- `resumeBehavior` must match how durable the target store actually is. A projection materialized into a persistent
  store should use `ResumeBehavior.DEFAULT`, so a restart resumes from the checkpoint instead of reprocessing
  history. A projection materialized into an in-memory store that does not survive a restart should use
  `ResumeBehavior.SAME_AS_START_AT` paired with an early `startAt`, so it rebuilds from history every time, the same
  trade-off `@StreamSubscription`/`@DcbSubscription` already document.
- Delivery is at-least-once, not exactly-once. A crash between processing an event and the checkpoint advancing
  redelivers that event on restart, so a `Projection`'s fold should tolerate reprocessing the same event, typically by
  making the state update idempotent per event id or by folding on a natural key that overwrite-updates cleanly.
- On the reactor stack, materializing into a blocking `ViewStateRepository`/`MaterializedView` still schedules that
  blocking call on `boundedElastic`, the same bridge `ReactiveDcbProjectionRunner`/`ReactiveProjectionRunner` already
  use. A fully reactive store removes that bridge, but no reactive `ViewStateRepository` exists yet, so this is a
  follow-up, not part of this decision.
