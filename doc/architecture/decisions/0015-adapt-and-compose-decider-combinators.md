# 15. Adapt and compose decider combinators

Date: 2026-06-24

## Status

Accepted

## Context

The `dsl/decider` module provides a minimal `Decider<C, S, E>` (initialState, decide, evolve, isTerminal) used by both the stream `ApplicationService` and the DCB `DcbApplicationService` decider DSLs. The service is parameterized by the event type. For DCB it is `DcbApplicationService<DomainEvent>`, invariant in its event type, so a feature decider whose event type is a subtype, for example `Decider<CourseCommand, CourseState, CourseEvent>`, cannot be passed to a service over the common `DomainEvent`.

The course-enrollment example worked around this with a local `forDomainEvents()` extension that widened the event type and ignored foreign events. That adapter is generally useful, and a project that splits a domain into several feature deciders also needs a way to combine them. Both belong in the library rather than copied into each application.

Two questions had to be settled before promoting them.

First, how to unify the command and event types of different deciders. A decider that runs against a shared service, or alongside other deciders, must speak the shared command and event types, not its own narrow ones.

Second, how to represent the state of several deciders combined into one. The combined decider folds a single event stream, and each constituent decider keeps its own state.

## Decision

Add two combinators to `dsl/decider`, following Jeremie Chassaing's decider vocabulary: `adapt` retargets a decider's types, and `compose` combines deciders. Fraktalio's Fmodel is the Kotlin precedent and confirmed the shape, and we keep Chassaing's names. Both are pure decider algebra. The module gains no new dependency, and in particular no dependency on any DCB module.

### `adapt` widens to supertypes, no `Either`

`adapt` lifts a `Decider<SubC, S, SubE>` to a `Decider<C, S, E>` where `SubC` is a subtype of `C` and `SubE` a subtype of `E`. A command that is not a `SubC` produces no events, an event that is not a `SubE` leaves the state unchanged, and initialState and isTerminal delegate unchanged.

Commands and events unify through a common supertype rather than an `Either`. At any instant there is exactly one command to decide on and exactly one event to evolve from. That is a sum, and a sum of which only one variant is ever inhabited at a time is exactly what a supertype expresses. The example already roots its types at `DomainCommand` and `DomainEvent`, so an `Either` would add a wrapper for a choice that the type hierarchy already encodes. Kotlin lacks ad hoc union types, which is the usual reason a library reaches for `Either`, but a declared supertype is the cleaner fit here and avoids the wrapping and unwrapping at every call site.

The runtime checks need the `SubC` and `SubE` classes. We follow the module's established dual-API pattern: a Java static taking `Class` tokens, paired with a Kotlin `inline reified` extension that supplies the tokens and reads `decider.adapt()` at the call site. Delegating the Kotlin extension to the Java core avoids the non-null command constraint a hand-rolled Kotlin version needs, because no Kotlin lambda re-wraps the Java command parameter.

### `compose` holds a product state

`compose` combines feature deciders into one decider. The two and three decider overloads `adapt` each constituent to the shared `C` and `E` for you, so a caller passes feature deciders over their own narrow command and event types directly, for example `compose(courseDecider, studentDecider, enrollmentDecider)`. Each command is offered to every constituent, and only the one that recognizes it emits events. Each event evolves every constituent's own state slice independently, skipping a slice whose decider is already terminal, so each slice settles at the same state it would reach folding its own events alone (matching `Decider.fold`, which stops folding once a decider reaches its terminal state). The composed decider is terminal once every constituent is. This relies on `isTerminal` being absorbing, meaning once it holds for a state it holds for every state reachable from it. That is the same assumption `Decider.fold` already makes, and it is documented on `isTerminal`.

State cannot be modeled as a supertype the way commands and events are. The composed decider must hold the state of every constituent at the same time and for the whole fold, because the constituents advance their states independently. That is a product (an AND), not a sum. A single supertype reference holds one value, so it can represent one constituent's state or another's, never both at once. The only value that carries all of them is a holder that has one slot per constituent, which is the product. Collapsing instead to one shared state was rejected, because it would force every decider to read and write the full union state and would undo the per-feature states that make the deciders independently testable.

Two fixed-arity Kotlin overloads cover the common cases and are the ergonomic surface: they return a `Pair` for two deciders and a `Triple` for three, and because they are `inline` with reified sub-types they `adapt` each constituent themselves. The two decider case also has an infix form, `first compose second`, for readability. It is deliberately two only, because infix is left associative and `a compose b compose c` would nest to `Pair<Pair<S1, S2>, S3>` rather than a `Triple`, so three deciders use the prefix form. For four or more deciders the result is a `CompositeState` that holds each state positionally with a typed `slice(index)` accessor, available in two forms: a `compose(deciders)` that takes a `List`, and a vararg `compose(first, second, third, fourth, vararg rest)`. Neither adapts, because the elements of a list or vararg cannot carry the per element types `adapt` needs, so their deciders must already share `C` and `E`. The vararg requires four leading deciders on purpose: that is what keeps it from overlapping the typed two and three decider overloads, which a `vararg` with two leading deciders would make ambiguous. The net effect for a caller is seamless, `compose(d1, d2, ...)` resolves to `Pair` at two, `Triple` at three, and `CompositeState` at four or more. This asymmetry, fixed arity adapts and the list and vararg forms do not, is the price of the cleaner common-case call site.

### Deciders stay store-agnostic

`adapt` and `compose` are store-agnostic decider algebra and must stay that way. A decider never depends on a DCB module. The layering only ever points the other way: a higher layer may depend on `dsl/decider`, never the reverse.

## Consequences

Positive:

- A feature decider over its own narrow command and event types runs against a shared service through `adapt()`, with no hand-rolled widening per application.
- Several feature deciders combine into one with `compose`, each keeping its own focused state and remaining independently testable.
- No `Either` or other wrapper appears at call sites. Commands and events stay their plain domain types.
- The module gains no dependency. The combinators are usable from both the stream and DCB decider DSLs.

Negative:

- The vararg `CompositeState` is positional and not statically typed in its slices, so a `slice(index)` read carries an unchecked cast that is safe by construction (slice i always holds decider i's state). The `Pair` and `Triple` overloads avoid this for two and three deciders, which is the common case.
- `compose` combines deciders at the decider level. It does not, on its own, route each command to its own consistency boundary in a single DCB `execute`. That is a separate higher layer (see future direction).

### Future direction

A DCB-specific decider that carries its own consistency boundary (a `DcbQuery`), so that a single `execute` routes every command to the right boundary, is a natural next layer, in the style of Fmodel's `DcbDecider`. It would live in the DCB DSL and depend on `dsl/decider`, never the reverse, and is deliberately out of scope here. `adapt` and `compose` are the store-agnostic foundation it would build on.
