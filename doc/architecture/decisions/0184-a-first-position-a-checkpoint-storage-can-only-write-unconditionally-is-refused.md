# 184. A first position a checkpoint storage can only write unconditionally is refused

Date: 2026-11-20

## Status

Accepted. Amends [ADR 89](0089-manual-subscription-mode-on-the-reactive-stack.md), whose point "A storage that cannot
evaluate the condition keeps the write it had" gave such a storage an unconditional first write with a warning, and
the passages in [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md),
[ADR 143](0143-a-subscribe-stores-its-start-position-only-after-the-wrapped-model-accepted-it.md) and
[ADR 159](0159-six-passages-in-released-adrs-do-not-describe-the-code.md) that describe that write. Applies to
`DurableSubscriptionModel`, `ReactorDurableSubscriptionModel` and the first checkpoint both Spring Boot starters store for a live start,
which [ADR 172](0172-how-the-spring-boot-starters-find-and-start-annotated-handlers.md) decides.

## Context

A durable subscription that asks for the model default and has no checkpoint stored records its first position before
anything is delivered. `ReactorDurableSubscriptionModel` writes it with `CheckpointWriteCondition.ifAbsent()`, and so
does `DurableSubscriptionModel` since 0.34.0, so of two nodes recording one at the same moment, only one write is
stored. The reactor model also records one for a `StartAt.dynamic(..)` that answers `StartAt.now()`, and from 0.35.0
both starters store the live start as the first checkpoint the same way.

`CheckpointStorage.evaluatesWriteConditions()` defaults to `false`. A storage that answers `false` from
`evaluatesWriteConditionsFor(String)` can't evaluate `ifAbsent()`, and every one of these sites then wrote the first
position unconditionally and logged a warning. Two nodes recording a first position at the same moment could overwrite
each other's and lose the events between the two positions for good.

ADR 89 kept that write because refusing would fail every application whose own storage never overrode the method.
`AGENTS.md` counts a loss window that is narrow, documented and warn-logged as a loss all the same, and
`ManualStartSubscriptionModel` already refuses such a storage when it is built.

## Decision

A first position that would be written to a storage answering `false` from `evaluatesWriteConditionsFor(String)` is
refused with `IllegalStateException` before anything is written for the subscription:

* `DurableSubscriptionModel` refuses it where it refuses a subscription with no start position to record, from
  `subscribe(..)` or from the evaluation of the start position that would have recorded it.
* `ReactorDurableSubscriptionModel` refuses it on `Subscription.waitUntilStarted()`, the way it refuses a first position
  that lost a race it can't order.
* Both Spring Boot starters refuse it when they would store the live start as the first checkpoint, so the application fails to start.

`startWhenFirstPositionCanBeOverwritten(boolean)` on `DurableSubscriptionModelConfig` and
`ReactorDurableSubscriptionModelConfig`, and the property `occurrent.subscription.start-when-first-position-can-be-overwritten`,
keep the unconditional write and the warning. The name says what the application accepts, following
`startWhenNoStartPositionCanBeRecorded`. Both default to `false`.

A subscription that records no first position isn't refused, whatever the storage answers. That covers a checkpoint
already stored, a `StartAt` of the application's own other than a dynamic one answering `now()` on the reactor stack,
and a starter subscription at `NOW` with `resumeBehavior = SAME_AS_START_AT`.

## Consequences

Every storage Occurrent ships answers `true` apart from `SpringRedisCheckpointStorage` for two kinds of id. Its
`save(..)` refuses an id in the reserved namespace of its version key whatever the condition, so such a subscription
never got a first position written and is now refused before the write instead. In Cluster-safe mode it also answers
`false` for an id Redis Cluster can't put in one slot with its version key, such as `{}orders`, which got the
unconditional write and is now refused unless the opt-in is set. Its javadoc says no id an application would choose is
built like that by accident.

An application with a storage of its own that keeps the interface default fails at its first subscribe of a new
subscription, or at startup with a starter. The way out is to evaluate `ifAbsent()` and answer `true`, or to set the
opt-in and accept the loss. The `AddCheckpointStorageConditionalWriteStubs` recipe adds stubs that keep
`evaluatesWriteConditions()` at `false`, so a storage it migrated is refused too until it implements the condition.

## Alternatives considered

Keeping the warning as the default, with an opt-in to the refusal. That keeps the loss as the default for every
application that never reads its logs, which is the case `AGENTS.md` rules out.

An opt-in named after the mechanism, such as `allowUnconditionalFirstPosition`. It says what the code does and not
what the application loses, where `startWhenNoStartPositionCanBeRecorded` names the outcome.
