# 138. A refused saga event does not come back on its own

Date: 2026-09-18

## Status

Accepted. Amends [ADR 109](0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md), which shipped in
0.32.0 and is therefore corrected by reference rather than edited. Resolves
[#1079](https://github.com/johanhaleby/occurrent/issues/1079). Nothing here changes behaviour.

## Context

ADR 109 decided that a saga refuses an event carrying no redelivery key, and that refusing is the default. It says
twice that the refused event is then offered again. The Decision says "A push feed offers it again, the saga refuses
it again, and the application stays stuck on it". The Consequences say "A refused event is not acknowledged, so a
broker will keep offering it", hedged only by a queue with a dead-letter policy, meaning a separate destination for
messages that keep failing.

The two are wrong for different reasons, which is why they are corrected here together rather than told as one
story. An ADR records what was decided and when, so an amendment that flattens a claim that was false from the start
into a claim that a later release falsified would itself be inaccurate.

**The Decision's sentence was wrong on the day it was written.** `PushSubscriptionModel`'s javadoc has said since
`2df6988a5` of 2026-07-19 that a handler exception propagates to the caller so the listener can decide whether to
acknowledge or redeliver. That is nearly three weeks before this decision was taken in `3a2cca842` on 2026-08-07, the
javadoc is the one the code implements, and the model still behaved that way at `occurrent-0.33.0`, so nothing
released since falsified the sentence. It was never true. A test that asks
whether later work touched a claim can only find claims later work falsified, which is why three passes over this
family of surfaces left the sentence alone.

**The Consequence's sentence was true when it was written.** No consume-side bridge existed on 2026-08-07.
`43af19f9f` added the transport-neutral broker API on 2026-08-18, `DeliveryFailurePolicy` among it, and the first
bridge that applies the policy is the RabbitMQ one in `bf72e48d0` a day after that. A bridge configured with
`DeliveryFailurePolicy.PARK`
republishes a refused event to the parking destination and acknowledges it out of the source queue once that
republish is confirmed, so on such a bridge the event is normally gone from the source on the first refusal. A park
publish that fails redelivers the original instead, which is one way `PARK` can still end in a redelivery. The
dead-letter hedge does not cover parking, because dead-lettering is the broker's own policy and parking is the
bridge's.

## Decision

**The saga declines to acknowledge a refused event, and what happens to it afterwards belongs to the subscription
model.** A `PushSubscriptionModel` hands the acknowledge-or-redeliver decision to the listener that called `accept`,
and on a consume-side broker bridge `DeliveryFailurePolicy` is where the choice is configured. For as long as the
model keeps offering the event, the saga refuses it again and the application stays stuck on it, which is what
ADR 109 described.
Where it is not, the saga still issues no duplicate commands, and that is the part the decision owns either way.

This is the same rule [#1076](https://github.com/johanhaleby/occurrent/issues/1076) settled on for the 0.34.0 saga
surfaces, applied to the one record those passes ruled out of scope. No surface says a refused or failing delivery
comes back, or blocks what is behind it, without that depending on the subscription model offering it again.

## Consequences

**The argument ADR 109 gave for `REQUIRED` being the default is weaker than it read.** That argument is the trade
ADR 104 made, that an event a saga cannot handle correctly is better stuck and visible than consumed and wrong. It
holds wherever the model offers the event again. On a bridge configured with `PARK` the event is neither stuck nor
visible where a reader of ADR 109 would look for it, so someone choosing `REQUIRED` there is promised something they
do not get. `BEST_EFFORT` and the reasoning for the opt-out are unaffected, and so is everything else ADR 109
decided.

**ADR 109 itself keeps its released text.** Its Status section points here, per the rule in `AGENTS.md` that an ADR
which has shipped is immutable and is corrected by a successor rather than in place. A reader arriving from
`changelog.md` or from section 15 of the 0.32.0 upgrade guide meets that pointer before the two sentences.

The 0.32.0 upgrade guide's own version of the claim, "the event is not acknowledged and your broker offers it
again", is corrected in place rather than by reference, because a migration guide is instructions a reader acts on
today rather than a record of what was decided. The two released `changelog.md` entries are left alone, since that
section records what a release changed and the reader's path out of it reaches the corrected upgrade-guide section.
