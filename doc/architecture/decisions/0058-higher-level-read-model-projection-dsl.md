# 58. Higher-level read model (Projection) DSL

Date: 2026-07-15

## Status

Accepted

## Context

The write side already has a self-describing, composable model. `Decider` is the pure decision and fold. `DcbDecider`
couples that `Decider` with the two things a DCB store needs but a plain decider does not carry: the `DcbCriteria` read
boundary for a command, and the tags to stamp on the events it writes. `DeciderApplicationService` and
`DcbDeciderApplicationService` run them. A feature owns its decision, its consistency boundary, and its tags in one
place.

The read side was asymmetric. There is a pure fold, `View`, with `initialState()` and `evolve(state, event)`, plus
`MaterializedView`, `ViewStateRepository`, and the Spring/Mongo `View.materialized(...)` helper. But nothing coupled a
`View` with the three things a read model needs beyond the fold: which events feed it, which view instance an event
updates, and where the state is stored. The only "subscribe and materialize in one call" bridge,
`StreamSubscriptions.updateView(...)`, existed for the blocking stream `Subscriptions` in Kotlin alone, with no
equivalent for the agnostic `Subscriptions`, DCB, the reactor stack, on-demand queries, or Java.

The concrete request is issue #194: a single `createProjection({ initialState, handlers, tagFilter })` that also
creates the subscription under the hood, with handlers keyed by event type rather than one large `when` or `switch`,
and a tag filter that scopes which events the projection reads. A real read model in the wild (a Parkster search view)
shows the boilerplate this removes: the set of event types is written twice, once as transport routing keys and once
as the branches of the fold, and the view instance id is dug out of the payload by reflection.

## Decision

**Introduce a read-side descriptor symmetric to `DcbDecider`, and reuse `View` rather than replace it.**
`Projection<S, E, ID>` is a final class, built through a builder, that couples a `View` (the pure fold) with the `id` function deriving the view
instance an event updates, the set of event types the fold handles, and an optional explicit `Filter`.
`DcbProjection<S, E, ID>` adds the `DcbCriteria` read boundary, the same way `DcbDecider` adds a criteria to a plain
`Decider`. `View` stays the pure, dependency-light fold it already is, so a caller who only wants to fold events keeps
using it directly, and the descriptor is an additive layer, not a rewrite.

**Define the fold with a type-safe, per-event-type handler builder that also records the handled types.**
`Projection.builder(initialState).on(AccountRegistered.class, (state, event) -> ...)` (and the Kotlin
`projection { on<AccountRegistered> { ... } }` block) registers a handler per event type. The builder assembles the
`View` and records exactly the registered types as the projection's `eventTypes`, so the subscription filter is
derived from the events the fold actually handles. That kills the duplication in the reference view, where the type
list lived in two places that had to be kept in sync by hand. The generated fold dispatches on the event's runtime
class, falling back through superclasses and interfaces so a handler keyed on a sealed parent or an interface still
matches, and returns the state unchanged for an event type with no handler. The no-op fallback makes it always safe to
feed a projection a broader stream than it handles.

**Keep the selector on the descriptor, and let it be more than a type list.** `eventTypes` is the default selector,
and an empty set means "all types". An explicit `Filter` overrides it, so a projection can select on subject, source,
data, or time, exactly as a hand-written subscription can, and the selector still travels with the descriptor rather
than being passed separately at every call site. On the DCB side the richer `DcbCriteria` already subsumes this
(types, tags, and their combinations), so `DcbProjection` carries a `DcbCriteria` and needs no separate `Filter`. A
single-instance projection parameterized by a key, such as issue #194's `isUsernameClaimedProjection(username)`, is
expressed by closing over the key in the factory that builds both the fold and the criteria, so no per-command
boundary function is needed on the read side.

**Leave delivery mode out of the descriptor.** Whether a projection is fed asynchronously (an ordinary subscription
model, eventually consistent), synchronously in the write transaction (the synchronous subscription model from
ADR 57, read-your-writes), or on demand (folding a query result), is chosen by the runner and the `Subscribable` or
query it is given, not baked into the `Projection`. The descriptor stays the pure "what", the runners own the "how".
The runners themselves, across stream, agnostic, and DCB, blocking and reactor, live in sibling modules
(`projection-dsl/blocking`, `projection-dsl/reactor`) built on this frozen `projection-dsl/common` API.

**Defer the annotation, but not catch-up or durable resume.** A `@Projection` annotation would be a convenience over a
complete programmatic API and carries heavy bean-post-processor wiring, so it is a later step, not part of this
decision (see ADR 59). That deferral is about the annotation only. The runners already catch up from history and
resume durably whenever they are given a catch-up-capable subscription model, such as the Spring composite model or a
hand-wired `CatchupSubscriptionModel`, on both stream and DCB, blocking and reactor. `Projection.adapt` is included
because it is a cheap mirror of `Decider.adapt` and widens a feature projection to a broader event type, but `compose`
is deferred, since read models compose less naturally than deciders, as two projections can disagree on the id and
the store.

## Consequences

- The read side now mirrors the write side. A feature describes its read model (fold, id, selector, and for DCB its
  read boundary) in one place, the way `DcbDecider` describes a write model.
- `View` is untouched and stays usable on its own. The descriptor and builder are additive.
- The subscription filter can no longer drift from the fold, because both come from the same registered handlers.
- Feeding a projection events it does not handle is safe by construction, so the same descriptor works behind a
  narrow or a wide subscription.
- The store stays out of the descriptor, so one projection can be materialized into different stores, or folded
  on demand, without changing its definition.
- Delivery mode is a runner concern, so async, synchronous (ADR 57), and pull all reuse one descriptor.
- The on-demand `project` now takes an optional instance id: a keyed projection folded without one throws, since
  blending every instance into a single state on demand would silently produce nonsense, and passing the id folds
  just that instance.
- `compose` is not available yet, and read models compose less naturally than deciders to begin with. The
  `@Projection` annotation followed in ADR 59, reusing these same runners rather than a parallel mechanism, so
  catch-up and durable resume were never blocked on the annotation existing.
