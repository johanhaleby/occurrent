# 93. A missing capability is refused, and a reactive publisher is cold

Date: 2026-08-04

## Status

Accepted. Closes #394, TCK phase 5.

## Context

Phases 1 to 4 covered what a store does when asked something it can answer. Two things it can be asked are left, and
neither is about reading or writing events.

**A capability it was not built with.** `EventStoreCapability` is a construction-time argument to a store's config, and
a store implements every interface either way, so there is always an object to call and nothing on it says the call will
not work. All three MongoDB stores refuse with an `UnsupportedOperationException` naming the capability, and two of them
had their own test asserting so. The reactive store had none, so the store whose refusals arrive as `Mono.error` rather
than a throw was the one nothing checked. Nothing recorded that refusing is the contract rather than a MongoDB habit.

**Anything a blocking bridge destroys.** The reactive store is covered by running the blocking suites over
`BlockingEventStoreOverReactive`, which is why the behavioural contract is written once. What blocking on a result
cannot see is the shape of the publisher: whether the work waited for a subscriber, whether a failure travelled through
the publisher or was thrown while assembling it, whether a `Mono` documented to always emit ever completes empty, and
what cancelling a read does. A store can get all four wrong and pass every blocking suite. The bridge's own javadoc
already said those belonged to a suite named `ReactiveEventStoreConformance`, which did not exist.

## Decision

**Refusing is contract, and it is asserted in both directions.** `CapabilityGuardConformance` holds a store built with
one capability to refusing the other, across all 11 stream entry points and all 6 DCB ones. The type is pinned and the
message is only required to name the capability, because the stores word it as "DCB capability is not enabled for this
MongoEventStore", which carries the implementing class and so cannot be cross-store law. That is the same line
`DuplicateCloudEventException.getDetails()` sits on.

Refusing rather than answering emptily is the part worth stating. A DCB read on a store that never enabled DCB, answered
with no events, is indistinguishable from an empty store, so a decider would append against a boundary it never checked.
An unsound answer is worse than no answer, which is the reasoning ADR 79 already applied to a truncated timestamp.

**Each group also asserts the restricted store still serves the capability it does have.** Without that, a fixture
handing back a dead store, or one closed early, passes every refusal assertion. The fixture accessors therefore hand
over the working view alongside the refusing ones: `EventStoreWithoutDcb` carries the stream view, `EventStoreWithoutStream`
carries the DCB view.

**The both-capabilities case is not here.** Nothing refuses in that configuration, so what is worth asserting is that
the two halves coexist without seeing each other's events, which `DcbStreamInteropConformance` already does. #394 lists
three cases and this covers two, deliberately, rather than restating the third.

**A reactive publisher is cold, and a failure travels through it.** `ReactiveEventStoreConformance` pins four things,
each of which costs a caller something concrete:

- Nothing happens until something subscribes, so a `Mono` built inside a `switchIfEmpty` and never subscribed has not
  already written.
- A failure reaches the subscriber rather than the assembling call, so error handling written the reactive way runs at
  all. A `WriteConditionNotFulfilledException` thrown out of assembly never reaches `onErrorResume`.
- A `Mono` documented to always emit does emit. An empty completion becomes whatever default the caller's operator
  supplies, so `count()` silently reads zero instead of failing. `read(..)` of a stream that does not exist emits one
  empty `EventStream` for the same reason.
- Cancelling a read completes and leaves the store readable, since taking the first match is ordinary use.

**It is STREAM only and takes no capability declaration.** These are properties of how a publisher is built rather than
of a capability, so asserting them once on the stream side says what there is to say, and every reactive store shipping
with Occurrent supports STREAM. `ReactiveEventStoreFixture` is therefore its own small interface rather than a subtype
of `EventStoreFixture`: an implementation supplies the blocking fixture for the whole behavioural contract and this one
only for the publishers.

**Every wait in it is bounded**, at 20 seconds. The event-store CI shards carry no rerun backstop, so a store that never
completes has to fail rather than hang the build, which is the rule #475 established.

## Consequences

The four anti-silent-skip rules extend to both new suites. Declining is still the visible absence of a subclass, the
restricted-store accessors default to empty and fail the suite rather than skipping it when they are, and
`SuiteNeverSkipsTest` gained a case for the guard suite. The reactive leaf needed its own `ReactiveSuiteNeverSkipsTest`,
because the blocking leaf's version cannot see a suite in another module, which would have left the reactive suite as
the one place an `Assumption` could be added unnoticed.

**This constrains #392.** A SQL store that answers a DCB read on a STREAM-only configuration with an empty result set,
rather than refusing, now fails a suite rather than being discovered later. So does one whose JDBC-backed publishers do
their work while being assembled, which is the easy mistake when wrapping a blocking driver in `Mono.just(..)` instead
of `Mono.fromCallable(..)`.

The in-memory store runs neither suite, and that is a real gap rather than a choice. It has no capability concept at all:
it serves STREAM and DCB unconditionally, so there is no way to build a restricted one to hold to a refusal. Recorded
here rather than worked around, because giving the in-memory store capabilities to satisfy a test would be the tail
wagging the dog. It also has no reactive form, so the reactive suite does not apply to it.

Nothing about the four shipping stores changed. Both suites passed on first run against all of them, which is the
outcome to expect from a phase that writes down what the stores already agreed on, and it means the 98 lines deleted
from the two per-store capability tests were duplication rather than coverage. What remains in those two files is
Mongo-specific: index and support-collection assertions, the config builder's own validation, and the operator-created
index that must fail startup.
