# 125. A lowered join's reaction reads its own window, not the whole retained history

Date: 2026-08-12

## Status

Accepted. Reverses part of [ADR 120](0120-a-step-condition-is-a-monotone-matcher-tree.md)'s 2026-08-11 amendment,
which exempted a lowered `join` from the window narrowing `on(StepCondition, ...)` got. Ships in 0.33.0, which is
held for it.

## Context

ADR 120's amendment narrowed a window-condition reaction to the window its condition fired on, then carved a lowered
`join` out of that narrowing with a `loweredFromJoin` flag. The reasoning was that `join` shipped in 0.31.0 reading
the whole retained history, and lowering it to a condition tree as sugar must not silently change what its callback
sees. PR 742's review had offered two resolutions for that tension, preserving the full-history view through an
explicit discriminator, or treating the divergence as a breaking change with a required migration. The discriminator
is what the amendment recorded.

That resolution left three surfaces disagreeing about the same API. `FlowSagaImpl.reactionWindow` excluded a lowered
`join` from windowing, so its reaction kept reading the whole retained history. `ReceivedEvents`'s interface javadoc
already documented the narrowed behaviour for every window-condition reaction, `join` included, unedited since
before the amendment, so it stated a contract the code did not implement. `StepBuilder.join`'s own deprecation
javadoc told callers the opposite of both. It said switching to `on(allOf(...))` changed nothing about what the
callback sees, "Behavior is unchanged, only the way you write it." Three answers to one question is what makes this
a bug rather than a design preference, and it is why Occurrent was telling callers to make a silent window change
while its own javadoc said they would not.

## Decision

`WindowCondition` drops `loweredFromJoin`. Every `WindowCondition` trigger, whether built by
`on(StepCondition, ...)` directly or by lowering a `join`, now narrows the same way. `FlowSagaImpl.reactionWindow`
windows it whenever `state.previousStepEntryIndex() >= 0`, with no exception for how the trigger was constructed.
That window always starts after index 0, the pinned initiating event, in every step including the first, so a
`join`'s reaction loses the initiating event from `count`, `all`, `first`, `any`, `none` and `asList` wherever it
previously reached it through the whole retained history, and a `join` past the first step loses whatever an
earlier step left behind on top of that. The window is the retained suffix of the events received since the step
it fired from was entered, which is all of them unless a `stepWindow` cap has already evicted the step's own
oldest ones, in which case the callback sees only what survived, same as `on(StepCondition, ...)` already did
before this record. `received.initiating()` is the one accessor built to reach past the window, and it still
returns the start event at any step, cap or no cap. Condition evaluation is unaffected either way, at any cap. A
first-step `join` already counted only post-start arrivals before this record, since it counts since the step's
entry the same way `on(StepCondition, ...)` always has, and
[ADR 123](0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md) already covers why a
`stepWindow` cap changes what the callback sees but not what the condition counted.

`StepBuilder.join`'s deprecation javadoc is corrected to state the shared window plainly and to drop the false
"Behavior is unchanged, only the way you write it" claim. `ReceivedEvents`'s javadoc needed no change, since it
already stated the narrowed contract this record now makes true. `FlowSagaTest.LoweredJoin`'s cross-step test is
inverted from asserting the exemption to asserting the narrowing, keeping the fixture that proves `initiating()`
still reaches past the window. Two more tests cover the boundaries that inversion alone does not, an earlier step's
event of the same type the `join` is waiting for, and a first-step `join` reading its own start type through a
generic accessor.

The changelog entry moves to Breaking changes, and `doc/migration/upgrading-to-0.33.0.md` gets a new section 11 as
the migration path, since no `UpgradeToOccurrent_0_33` recipe touches `StepBuilder` and a behavioural window change
is not something a rewrite can detect.

## Rejected alternatives

**Removing `join` instead of narrowing it.** No recipe covers `join` at all, so removal would break every caller
with no automated fix, and it would force each of them through exactly the silent window change this record
removes, just without `join` left to fall back to. Narrowing keeps `join` usable and deprecated, with the same
migration story `on(StepCondition, ...)` already carries.

**Keeping the discriminator and adding a second one for whichever gap surfaces next.** A per-trigger flag on
`WindowCondition` is how the previous amendment preserved the exemption, and the model that flag was hiding, one
reaction shape with a silent bit deciding which contract applies, is itself the defect, not the flag's absence.
Reversing the decision resolves the three-surface disagreement. Patching the discriminator again would only add a
fourth answer to it.

## Consequences

- A `join`'s reaction reads fewer events than it did before this release, at any step. A first-step `join` loses the
  initiating event from its generic accessors, and a `join` past the first step loses that plus whatever an earlier
  step left behind. A caller relying on either reach has to hand-count against a guard's full `ReceivedEvents`
  instead, the same workaround `join` itself was built to remove. `received.initiating()` is unaffected.
- ADR 120's amendment argued this narrowing could never reach a lowered `join`, because that would silently break a
  shipped API. This record makes exactly that change, deliberately, with the changelog entry and migration section
  the amendment said a dropped discriminator would need.
- The sibling paragraph in the same amendment, exempting a lowered `join`'s collapsed same-type expectations from
  the `allOf` duplicate-matcher rejection, is unrelated to what fires a branch's reaction and stays as written.
- No wire or persistence format changes. `loweredFromJoin` lived on the `WindowCondition` trigger record, rebuilt
  from a saga's declaration every time one is constructed, never on `FlowStateImpl` or in a stored document, so
  there is nothing for a store to migrate and no reconstructed-state case to add.
