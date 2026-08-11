# 123. A step condition's counts are carried, so the step's events can be dropped

Date: 2026-08-11

## Status

Accepted. Resolves #741, the half of the 0.32.0 design review's finding 3 that [ADR 120](0120-a-step-condition-is-a-monotone-matcher-tree.md)'s
amendment deferred. 0122 is the current maximum, re-audited across every remote branch at write time per the max-plus-one rule.

## Context

`historyWindow` limits how many received events a flow saga keeps behind the current step's entry, and only that. The trim
runs when a step is left, and the step being left keeps every one of its own events, because a window condition counts over
them. So an instance parked in one step while a large number of correlated events arrive keeps all of them, and
`historyWindow(0)` is no exception. The 0.31.0 changelog entry that introduced `historyWindow` said the retained state "does
not grow without bound", which was true only for a flow whose steps turn over.

Nothing here is a correctness bug. It is an operational limit, and closing it means the current step's events have to become
droppable, which is what ADR 120's amendment recorded as the reason the two halves are one change seen twice.

## Decision

**`stepWindow(int)` limits how many of the current step's own events are kept, and it is applied on every delivery.** It is
opt-in with no cap by default, at least 1, on `FlowSaga.Builder` and in the Kotlin `saga { }` block. Retention is a
contiguous tail, so capping the current step at `n` means keeping the last `n` events of all once the step has more than
`n`, with the initiating event always kept as the first element. Together with `historyWindow` the total kept is at most
`historyWindow + stepWindow + 1`.

The minimum of 1 is a guarantee rather than an arbitrary floor. The arriving event is always the newest, and the drop takes
from the oldest end, so ADR 120's promise that the event which tipped a condition over is
`received.asList().getLast()` survives the cap, and `initiating()` reaches the start event as it always did.

**A window condition's counts are carried in the instance's state instead of being re-derived from the events.** That is
what makes a step complete on exactly the event it would have completed on without the cap. Dropping the events without
carrying the counts would leave a condition permanently short, which is the failure the cap exists to avoid rather than to
create. `FlowStateImpl` has a `stepConditionProgress` component holding one count per leaf plus a fingerprint of the
declaration they were counted for.

**A carried count is the raw number of events that matched a leaf's matcher, saturating at `Integer.MAX_VALUE`, and the
count a leaf asks for is not part of the fingerprint.** An earlier design capped each counter at the highest threshold asked
for and left the threshold out of the key, which meant raising a threshold on a redeploy silently kept a count that could
never reach it, so the step never completed. Keeping raw totals removes that whole class of failure instead of detecting
it, since a total means the same thing whatever threshold a later declaration compares it against. Saturating is safe
because a leaf's truth is never undone by a later event, so a count that high already exceeds every threshold a leaf can
ask for.

**Counts are only carried for a step whose leaves can be told apart, proved when the saga is built.** A predicate has no
identity a persisted fingerprint can capture, so a leaf's entry is its event type plus whether it has a predicate, and two
leaves that agree on both produce the same entry. `FlowSagaImpl.StepLeaves` groups a step's leaves by that entry and
requires every group's `EventMatcher`s to be equal, which is what makes two leaves sharing an entry interchangeable and a
crossing of their counts a no-op. A step that fails the check counts its window on every delivery, exactly as it did before
this change, and `build()` refuses `stepWindow` for a flow containing one, naming the step, the event type and the
remedies.

That check is the reason this design differs from the one #741 proposed. That one keyed a leaf on its event type, whether it
has a predicate, and the count it asks for, and relied on a full recount when the fingerprint did not match. Two leaves that
agree on all three produce identical entries, so swapping them leaves the fingerprint matching and moves each count onto the
other's predicate. When the two sit in different branches of one step, that fires the wrong branch and runs the wrong
reaction with nothing to notice it, which is the same silent failure two earlier designs were rejected for.

```java
step("decide", step -> step
    .on(event(Payment.class, 1, p -> p.isBig()), transitionTo("escalate"))
    .on(event(Payment.class, 1, p -> p.isSuspicious()), transitionTo("review")))
```

Swap those two lines on a redeploy and a fingerprint built that way is byte identical.

**Carrying counts means giving up tolerance for a mid flight change to a capped step's declaration, and that is stated here
rather than left to be discovered.** A leaf whose count is reached needs no further events, and a leaf still short needs
future events rather than past ones, so the only thing past events are needed for is recounting from scratch after a
declaration changed under a parked instance. Once the cap has dropped them there is nothing to recount from. So the trade is
exact, and it is the whole of what the cap costs.

Such an instance refuses the delivery with an `IllegalStateException` naming the step, saying that its condition declaration
changed while the instance was parked in it, that the events its counts would be rebuilt from are gone, and that retrying
cannot help. The two remedies are to put the previous declaration back until the parked instances have moved on, or to
delete the instance. `IllegalStateException` rather than a new exception type, because an operator's remedy is a deploy or a database
action rather than a `catch`, and this is the same shape of state refusal ADR 104 and ADR 105 settled on. Resetting the
counts to zero was rejected for the same reason the capped-counter design was, a step that waits forever with no signal.

**Absent counts always mean "count the window", and a document written before the field existed relies on it.** `null` is
the "not known" value, since `0` is a real count and `-1` keeps meaning only what it means on `previousStepEntryIndex`. A
count list whose length disagrees with the step's declaration, or which contains a negative, is treated the same way as absent,
because a store defaults each field on its own and can hand back a combination no `evolve` ever wrote.

**The signal that events were dropped needs the step's entry position, not just the tail's start.** Reading the tail as
starting past the step's entry is what says the step's own events are gone. The obvious form of that test, comparing the two
numbers alone, reads a document whose tail start was defaulted to 1 and whose entry position was defaulted to 0 as a real
drop, and then refuses a delivery where the previous behaviour was to degrade. `FlowSagaTest.ReconstructedState.a_window_start_past_the_step_entry_does_not_pull_the_initiating_event_into_the_window`
is the test that caught it, which is the clamping exposure ADR 120's amendment warned the next change would inherit,
arriving exactly as warned. An instance that has entered a step was entered at position 1 or later, so the test also
requires that, and a defaulted tail start of 1 can then never pass a real entry position.

**The determinism contract is unchanged, and this is why it had to be stated first.** A predicate is still re-run whenever a
count has to be derived from the window, and a replay still runs it from the start, so a predicate that consults the clock,
a random source, mutable state, or a remote service can still answer differently for the same event later. Carrying counts
does not relax that. It does change how a violation shows up, since a carried count freezes an answer a rescan would have
re-asked, and that is a permitted consequence of a contract that ADR 120's amendment already states rather than a new
hazard. The order the two halves landed in was deliberate for this reason.

## What this buys, measured against what it does not

Per delivery a flow saga copies the whole retained list in `append`, counts the window once per leaf, and re-serializes
every retained event as CloudEvent JSON when the state is saved. Carrying counts removes the second of those and adds a
fingerprint and a small list of numbers to every save.

So carrying counts is not a performance change worth making on its own, and it is not presented as one. The list copy and
the per-save re-serialization both stay, and the re-serialization is the expensive one by a wide margin. What `stepWindow`
does is put a ceiling on the number those two terms and the stored document size all scale with, which is a different kind
of change from a percentage.

The coupling is also narrower than #741's wording suggests, and the accurate version is worth keeping. A cap is possible
without carried counts, since a step with only classic branches and guards needs nothing new to drop its oldest events.
What the counts buy is that a window condition stays exactly correct under the cap. They make it safe rather than possible.

## Rejected alternatives

**Redefining `historyWindow` to cover the current step's events too, instead of adding a second setting.** One number for
total retention is the tidier API, and it was rejected because every existing flow would silently narrow what its guards,
its window-condition reactions and its `timeout` reactions can read, at the shipped default of 100, on an upgrade. Occurrent
is a published library whose callers are unknown, so a silent narrowing is exactly the footgun the conventions refuse. Two
settings that each say what they limit cost a reader one extra sentence and cost nobody a behaviour change.

**A positional count list with a size check, and no equality proof.** Rejected because position mis-associates on a reorder
in precisely the way a signature does, which is the same objection that sank the earlier signature design.

**Refusing `stepWindow` for any flow that has a window condition at all, and shipping the cap alone.** This is a materially
smaller change that changes nothing about the persisted state and leaves nothing new to keep compatible, and it does cap a noisy step whose branches are
all classic. It was rejected because a step that idles while a large number of correlated events arrive is usually one
waiting on a count, so it excludes the case the work exists for.

**Requiring a caller-supplied name on a predicated leaf, so two of them can always be told apart.** This turns the
build-time refusal above into something with a remedy in every case, and ADR 120's amendment already considered names on
conditions for a different purpose and judged them a materially larger public API. It stays the way out if the refusal
proves too restrictive in practice, and it is not needed to ship the cap.

**A caller-declared version on the flow, bumped by hand when a declaration changes, instead of a computed fingerprint.**
Rejected because forgetting to bump it produces exactly the silently crossed counts the fingerprint exists to prevent, and
nothing can tell a forgotten bump from an unchanged declaration.

## Consequences

- A flow that can idle in a step a large number of correlated events arrive in has a way to cap what it stores, and the
  0.31.0 claim that the retained state does not grow without bound becomes true for it once `stepWindow` is set. The
  changelog corrects that claim forward, in the entry for this change, rather than by editing the dated 0.31.0 section.
- Under `stepWindow`, a guard, a window-condition reaction and a `timeout`'s `onExpiry` read only the events still kept. A
  retry guard counting across a self-looping step needs its threshold to fit inside the cap, the same requirement
  `historyWindow` already has. The migration guide's section 9 states it, and it is not a forced migration, since
  nothing changes unless the cap is set.
- A capped step whose condition declaration changes while instances are parked in it refuses those deliveries loudly. This
  is the first place the flow layer says anything about a declaration changing under a parked instance. A renamed or removed
  step still throws a bare `NullPointerException` from inside `evolve`, which is filed separately as #748 and deliberately
  not fixed here, because deciding what an instance parked in a step that no longer exists should do is a lifecycle
  decision rather than a message.
- The persisted shape gains one nullable component and the Mongo store one nested sub-document. Compatibility is kept in both
  directions, and the pre-0.33.0 eight-component secondary constructor on `FlowStateImpl` still keeps an out-of-repo
  `SagaStateStore` compiling, now supplying `null` counts alongside the `-1` entry position it already supplied. No
  nine-component constructor is added, because that component list has never been released and nothing outside this
  repository can be compiled against it. Three hand-seeded documents are the evidence, one written by 0.32.0, one by an
  earlier 0.33.0 build, and one holding counts that cannot describe any declaration.
- Nothing changes for `join`. A lowered `join` declares leaves with no predicates over types collapsed to one each, so its
  leaves are always distinguishable and a `join` step can be capped like any other.
- No OpenRewrite recipe entry. Nothing changes signature, the new setting is additive and opt-in, and a recipe can neither
  turn a runtime setting on nor rewrite what a callback expects to be able to read.
