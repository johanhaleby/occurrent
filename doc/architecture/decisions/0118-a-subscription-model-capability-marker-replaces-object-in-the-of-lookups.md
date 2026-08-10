# 118. A subscription model capability marker replaces `Object` in the `of` lookups

Date: 2026-08-10

## Status

Accepted

## Context

`RepositionableSubscriptions`, `ReplayAwareSubscriptions` and `IntrospectableSubscriptions` (blocking) each expose a
static `of(Object subscriptionModel)` that unwraps a `SubscriptionModelWrapper` chain and returns the facet if
something in it implements the interface. The parameter is `Object` because three different shapes of caller reach
it. `SagaAnnotationRegistrar.timersEnabledFor` holds a bare `Subscribable`, `OccurrentSubscriptionsExtension.modelSubscriptionIds`
holds a `SubscriptionModelLifeCycle`, and several subscription-model implementations (`DurableSubscriptionModel`,
`CompetingConsumerSubscriptionModel`, `CatchupSubscriptionModel`, `ManualStartSubscriptionModel`) hold a whole
`SubscriptionModel`.

`SubscriptionModel extends Subscribable, SubscriptionModelLifeCycle`, so it is the intersection of the two halves, not
their union. A parameter typed as the union of "a `Subscribable`, or a `SubscriptionModelLifeCycle`, or a
`SubscriptionModel`, or a `SubscriptionModelWrapper` around any of these" has no name in Java, which has no union
types. Overloading `of` once per accepted type does not work either. A `SubscriptionModel` argument implements both
`Subscribable` and `SubscriptionModelLifeCycle` at once, so two overloads taking those types would both apply to it
and the call would not compile. `Object` was the only type left that accepted every caller, at the cost of accepting
everything else too and pushing the real check into an `instanceof` cascade inside the method body, invisible at the
call site.

## Decision

Introduce a marker interface per stack, `SubscriptionModelCapability`, declaring no methods. Every capability facet
extends it:

* Blocking: `Subscribable`, `CancellableSubscriptions`, `Pushable`, `RepositionableSubscriptions`,
  `ReplayAwareSubscriptions`, `IntrospectableSubscriptions`, `SubscriptionModelWrapper`.
* Reactor: `Subscribable`, `CancellableSubscriptions`, `Pushable`, `IntrospectableSubscriptions`,
  `ReplayAwareSubscriptions`. The reactor stack has no `SubscriptionModelWrapper` and no `RepositionableSubscriptions`.

`SubscriptionModelLifeCycle` and `SubscriptionModel` (both stacks), and the blocking `DcbSubscriptionModel` and
`StreamSubscriptionModel`, do not extend `SubscriptionModelCapability` directly. They inherit it transitively, either
through `Subscribable` or through `CancellableSubscriptions` (which `SubscriptionModelLifeCycle` extends). Adding a
supertype to `Subscribable` and `CancellableSubscriptions` is enough to reach every type built on them without editing
each one.

The three `of(Object)` methods narrow to `findIn(SubscriptionModelCapability)`, renamed from `of` at the same time.
`of` is the Java convention for constructing a value, and `Optional.of` in particular never returns empty, but this
method searches a wrapper chain and can come back empty, so the old name inverted the strongest precedent a reader
has for it. `findIn` says it searches, says it can fail, and names the containment the chain walk actually performs.
A `Subscribable`, a `SubscriptionModelLifeCycle`, a `SubscriptionModel`, and a `SubscriptionModelWrapper` all satisfy
the new parameter type, so every existing call compiles unchanged under the new name. What no longer compiles is
passing something that is none of these, which used to type-check against `Object` and fail only if the wrapper
chain never produced the facet at runtime. The javadoc on each renamed method now names the accepted shapes instead
of the previous "any subscription model, wrapped or not", which was inaccurate. A bare `Subscribable` is not a
subscription model, and neither is a `SubscriptionModelLifeCycle` on its own.

The reactor `FluxSubscriptionModel` and the reactor `DcbSubscriptionModel` are unrelated to this hierarchy. Neither
extends `Subscribable` or `SubscriptionModelLifeCycle` on that stack, so neither is a `SubscriptionModelCapability`,
and this change does not touch them.

Adding a method-less supertype to an existing interface is both source and binary compatible. Every current
implementer keeps compiling and keeps working without recompilation against the old class file. No migration recipe
is needed for the `extends` additions. Renaming `of` to `findIn` and narrowing its parameter from `Object` to
`SubscriptionModelCapability` are both signature changes on `ReplayAwareSubscriptions` and `IntrospectableSubscriptions`,
which shipped under their previous type names in 0.32.0 and were renamed to their current names in PR #705 just
ahead of this change. `UpgradeToOccurrent_0_33` renames the method for those two alongside the type, and it is
recorded as a breaking change in the changelog and the 0.33.0 upgrade guide. `RepositionableSubscriptions.findIn`
never shipped under the `of` name, so it carries no migration note.

### Why this differs from `EventStoreCapability`

`EventStoreCapability` is an enum with two constants, `STREAM` and `DCB`. It is a runtime configuration value, passed into
`EventStoreConfig.eventStoreCapabilities(...)` to say which reads, writes and support structures an event store
should enable, and read back out of a stored event through `Filter.capability(...)` to say which of those paths wrote
it. Its job is to be a value that is compared, stored, and switched on.

`SubscriptionModelCapability` is an interface. Its job is to be extended, so a facet becomes a member of the family
by declaring `extends`, and to appear in a method signature as the type a caller's object is checked against. Neither
mechanic transfers to the other's job. An enum cannot be extended by an arbitrary interface a caller already
implements, and an interface has no fixed, enumerable set of constants to switch over or persist as a stored value.

Neither could adopt the other's shape without losing what it is for. The JDK carries the same asymmetry in the same
domain for the same reason. `java.time.temporal.TemporalUnit` is an interface, because a unit of time is something a
type like `ChronoUnit` or a custom calendar system implements, while `ChronoUnit` itself is an enum, because its
seven-ish standard units are a closed, comparable, storable set. Nobody asks why time units are not one mechanism.

If the subscription side ever needs a declared, runtime-queryable set of capabilities, the enum's shape rather than
the marker interface's, the name reserved for it is `SubscriptionModelFeature`, distinct from
`SubscriptionModelCapability` on purpose, so the two do not collide when that need arrives.

## Consequences

* `findIn(SubscriptionModelCapability)` documents at the call site what it actually accepts and does, instead of
  accepting anything under a name that promised construction rather than a search.
* A future capability facet only needs `extends SubscriptionModelCapability` to become usable everywhere the marker
  is accepted, and nothing else in the hierarchy needs to change.
* Anything outside this library that calls one of the three `of` methods, by name or with a type that is not a
  `Subscribable`, a `SubscriptionModelLifeCycle`, a `SubscriptionModelWrapper`, or a whole `SubscriptionModel`, no
  longer compiles. This is limited to code implementing one of the two renamed interfaces directly and calling `of`,
  which `UpgradeToOccurrent_0_33` fixes for the rename, though not for a caller passing something the type system did
  not previously catch.
* The reactor stack gains the same marker and the same `extends` additions on its five facets, even though nothing on
  that stack currently declares a parameter of this type. That stack has no `SubscriptionModelWrapper` to recurse
  through, so its facets are reached with a direct `instanceof` check instead. The marker is added for symmetry with
  the blocking stack and so a future reactor lookup with the same shape has a supertype ready to declare, rather than
  facing the same `Object` problem this ADR fixes on the blocking stack.
