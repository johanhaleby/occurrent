# 129. A flow saga's stepWindow caps only its own declared events

Date: 2026-08-16

## Status

Accepted. Fixes #773, #764.

## Context

`FlowSagaImpl.evolveOnEvent` appends every correlated event it receives to an instance's retained
history before it checks which branch handles it (`FlowSagaImpl.java`, `append(state.received(),
event)`). The core `Saga.Builder` does not. Its `evolve` resolves a handler through `TypeDispatch`
and returns the state untouched when none is registered.

That inconsistency has a cost once `stepWindow(N)` is set (shipped in 0.33.0, ADR 123). An event of
a type no step in the flow declares still takes one of the step's N slots, and can evict one of the
step's own events to make room for itself. Two ways reach this in production. A `CloudEventTypeMapper`
that collapses a whole domain hierarchy onto one CloudEvent type string does it without any filter
override. A flow declaring only `PaymentReserved` derives the filter `eq("order-event")`, which
matches every event in the hierarchy. An explicit `replacementFilter()` or `narrowingFilter()` wider
than the flow's declared types does it deliberately.

`stepWindow`'s own javadoc says it caps "the current step's own received events." The implementation
did not agree with that claim for an event outside the flow's declared types, which is #773.

#764 asks a separate but related question. Should a later release require an explicit retention
choice for a flow that can park (a step with a window condition or a timeout), instead of the
current opt-in `stepWindow` plus the edge-triggered warning `SpringMongoSagaStateStore` already logs
when an instance's retained event count crosses 1,000? That warning, shipped in 0.33.0, is untyped
and counts every retained event, declared or not. A maintainer comment on #764 couples the two issues
explicitly. Whatever #773's fix does to what a parked instance retains changes the answer to #764.
This ADR decides both together because #773's fix determines exactly what #764 is choosing between.

## Decision

### #773: the cap counts only the flow's own declared-type events

`stepWindow` now counts and evicts only events whose type is a member of the flow's own
`eventTypes()`, the union of every step's declared branches and window-condition leaves, already
computed at build time (ADR 124 covers how a declared sealed type expands into it). A correlated
event of any other type is still appended to `received()`, never silently dropped, but it neither
counts toward the N-event budget nor evicts one of the step's own events by itself. It is swept up
only as a byproduct of the window advancing past it while dropping enough declared events ahead of
it to satisfy the cap.

The alternative the issue also named, discarding the foreign event before it is ever appended, was
rejected. Nothing else in the codebase silently drops an event that genuinely arrived and correlated
to an instance, and doing so here would be a new precedent rather than a neutral simplification. It
also runs against the isolation rule in `AGENTS.md`: no design here may lose events. The event is
not "the step's own," but it happened to this instance, and a caller reading `ReceivedEvents` later
may reasonably ask about it.

A third option, a total-size ceiling on the step's retained tail regardless of type (evict foreign
events first once some larger bound is crossed), was considered and rejected. There is no principled
ceiling to pick that is not itself arbitrary, and enforcing one would reintroduce #773's own bug in a
different guise, evicting real content to make room for volume the flow never declared any interest
in. The case that motivates it, a widened selector or a collapsing type mapper, is a deliberate,
uncommon caller customization already covered by the store's warning.

### The consequence this has for #764

Once a foreign-typed event no longer counts toward `stepWindow`, a step fed only foreign-typed
events is not bounded by `stepWindow` at all, because nothing evicts an event that never counts.
This is the cost the issue itself named for this fix ("a retention rule with two kinds of entry in
it"), not a new problem this ADR introduces.

It reaches only flows that already opted into a wider surface than their own declared types. The
default path, no `replacementFilter`, no `narrowingFilter`, no collapsing type mapper, can never
deliver a foreign-typed event to `evolve` in the first place, because the subscription filter itself
is derived from `eventTypes()`. And the 0.33.0 warning is untyped. It already counts every retained
event, declared or foreign, so this exact growth is visible in production today regardless of what
`stepWindow` bounds.

### #764: no forced retention choice

The opt-in `stepWindow` default plus the existing warning stays the whole story. No release will
require a flow to declare a retention policy before it can park.

ADR 123's rejected alternatives already argued against a smaller version of this same shape:
redefining `historyWindow` to also cover the current step's events, instead of adding `stepWindow` as
a second setting, was rejected because every existing flow would silently narrow what its guards,
reactions, and timeout callbacks can read, at the shipped default, on an upgrade. Occurrent's callers
are unknown, so a silent narrowing is exactly the footgun the conventions refuse.

A required retention choice for every park-capable flow is that same argument at a larger scale, a
construction-time refusal for the majority of existing flows, which have no reason to set
`stepWindow` today, in exchange for closing a risk the warning already surfaces. #764 itself
considered three shapes for this. Keep the opt-in default and treat the warning as the answer.
Require a choice only for flows that declare a window condition or a timeout. Or add a configurable
hard cap that refuses the save past a size. The second is the construction-time refusal above,
scoped narrower but paying the same cost for the same reason. The third moves the failure earlier
without changing the default, picks another arbitrary size, and is redundant with the warning. The
first is what already ships, and is what this ADR keeps.

### Javadoc corrected to state the scope

Three places described the old behavior, or did not say enough to rule it out, and each needed a
correction in the same change:

- `Saga.replacementFilter()` stated the bug as the shipped cost of a wide selector. It now says a
  foreign event is retained but neither counts against `stepWindow` nor evicts a declared event, and
  that nothing evicts it either, so it can grow what a step stores without limit.
- `FlowSaga.Builder.stepWindow(int)` did not previously say what "the current step's own received
  events" meant for a type the flow does not declare. It now states the scope directly and points at
  the store's warning as the residual signal.
- The design comment above `SpringMongoSagaStateStore.RETAINED_EVENT_WARNING_THRESHOLD` listed the
  known growth causes (an unbounded `stepWindow`, a step that never transitions). It now lists this
  one too.

## Consequences

The vast majority of existing flows, everything without a `replacementFilter`, a `narrowingFilter`,
or a collapsing type mapper, see no behavior change. Their subscription can never deliver a
foreign-typed event, so `isDeclared` is true for everything they ever receive and the cap behaves
exactly as before.

A flow that already widened its selector gets a genuine bug fix. `stepWindow` now keeps exactly N of
its own declared-type events, instead of a mix that a foreign-type flood could crowd out. What it
does not get is a size guarantee on the foreign-typed portion. A caller relying on a wide selector for
a genuine reason should watch the store's warning rather than assume `stepWindow` alone bounds
document size, and a caller who wants that bound back should narrow the selector to the flow's own
declared types instead.

This ADR supersedes the "deferred to 0.34" routing recorded on both #773 and #764 with a decided
answer now, landing in the same change that fixes the eviction defect.
