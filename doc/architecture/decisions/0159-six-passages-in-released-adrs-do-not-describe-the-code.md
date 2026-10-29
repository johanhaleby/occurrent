# 159. Six passages in released ADRs do not describe the code

Date: 2026-10-29

## Status

Accepted. Corrects [ADR 89](0089-manual-subscription-mode-on-the-reactive-stack.md),
[ADR 126](0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md),
[ADR 127](0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md),
[ADR 132](0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md) and
[ADR 133](0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md). All five have shipped in
a release, so they are corrected here rather than edited. Fixes [#1009](https://github.com/johanhaleby/occurrent/issues/1009)
and [#1010](https://github.com/johanhaleby/occurrent/issues/1010).

This ADR decides nothing new. Every correction below states what the code on `main` already does.

## Context

Each of the five ADRs has a passage that a reader can take as a statement about the code, and that the code does
not match. Some were never true of the code, and the others became false with a later change.

## Decision

### 1. ADR 132: the blocking handover filled the cache from its replay

ADR 132's Consequences section says ADR 135 closed #891 by having the reactor history windows fill no cache, "which
is what the blocking stacks already did". That is true of the three blocking position catch-up stacks and was never
true of `BlockingHandover`, the catch-up-then-push engine. Its replay filled the same cache its live delivery
filtered on. ADR 135's amendment for [#963](https://github.com/johanhaleby/occurrent/issues/963) records this.

The sentence is still false of the handover today.
[ADR 137](0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md), which shipped in 0.34.0,
repaired the handover without removing that cache. The replay still fills one, `replayedIds`, and a live payload it
suppresses still reaches its source, so the append is recorded. Read the sentence as covering the three position
catch-up stacks only.

### 2. ADR 132 decision 5: the reactive store refuses a wait on a non-blocking thread

Decision 5 lists what `waitUntilApplied` can return, and says a wait always reads at least once before it honours
its timeout. `ReactiveMongoAppliedAppendStore` has one more outcome. On a thread `Schedulers.isInNonBlockingThread()`
marks, a Netty event loop for example, `waitUntilApplied` throws `IllegalStateException` before its first read, so
that wait reads nothing at all.

The store's other three operations, `hasApplied`, `recordApplied` and `clear`, call `block()` and catch nothing
around it. On such a thread they throw the `IllegalStateException` that Reactor's `block()` throws there. The
store's javadoc already says both.

### 3. ADR 126: #753 closed in 0.34.0 with a code change

ADR 126's section "#753 needs no code change of its own" decides to leave #753 open with no further action, and its
Consequences section says "#753 stays open". ADR 126's own Status section supersedes both. #753 closed in 0.34.0, and
declaring an event type whose concrete subtypes cannot be found is now refused. Read both passages as the decision
that the Status amendment replaced.

### 4. ADR 133: a MongoDB filter specification also gets an empty result

ADR 133 says the default `destinationsFor(SubscriptionFilter)` answers a `DcbSubscriptionFilter` and a custom
`SubscriptionFilter` with an empty result, and calls those "the two it cannot unwrap". `MongoFilterSpecification`,
with its nested `MongoJsonFilterSpecification` and `MongoBsonFilterSpecification`, is a third. It holds a MongoDB
query rather than a `Filter`, so the default answers it with an empty result too, and a consumer binds the
catch-all destination.

The rule the default follows is shorter than any list. It unwraps the `Filter` an `AgnosticSubscriptionFilter` or a
`StreamSubscriptionFilter` holds, and answers every other `SubscriptionFilter` with an empty result.

### 5. ADR 89: the blocking durable model writes a first position with `ifAbsent()`

ADR 89's amendment for #738 says that on the blocking stack only `ManualStartSubscriptionModel` writes a first
position conditionally, and that `DurableSubscriptionModel` "still records a first position with `any()`". Since
[#975](https://github.com/johanhaleby/occurrent/pull/975), which shipped in 0.34.0,
`DurableSubscriptionModel.saveFirstPosition` writes the first position with `ifAbsent()`. When that write loses, it settles the race through
`CheckpointStorage.resolveFirstCheckpointRace`, or refuses unless the position it reads back is the one it computed.

A storage that answers `false` from `evaluatesWriteConditionsFor` still gets an unconditional write, and the model
logs a warning saying so. That is the one case where the old sentence still describes what happens.

### 6. ADR 127: a handler a proxy cannot invoke does not always fail at startup

ADR 127's amendment for #965 says a handler method the bean's Spring proxy cannot invoke "now fails fast at startup".
That holds for a bean created during startup, since every proxy such a bean gets exists before its handlers are
checked. A bean built after startup, a `@Lazy` bean for example, fails later in one of two ways.

- Its handlers are checked when the bean is created, so the exception comes from creating the bean.
- A `BeanPostProcessor` that runs after Occurrent's can wrap the bean in a proxy that cannot invoke the handler.
  That proxy does not exist yet when the handler is checked, so the handler fails with
  `SubscriptionHandlerNotInvocableException` the first time an event is delivered to it.

Section 11 of [the 0.34.0 upgrade guide](../../migration/upgrading-to-0.34.0.md) describes both.

## Consequences

- ADR 89, 126, 127, 132 and 133 each gain a pointer to this ADR in their Status section, and nothing else in them changes.
- ADR 135 gets no pointer. It already says what correction 1 says, and this ADR changes nothing it decides.
