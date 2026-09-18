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

Both skip the same step, that whatever fed the event decides whether to acknowledge it, and they overstate by
different amounts. They are corrected here together rather than as one story, because an ADR records what was
decided and when, and running the two into a single explanation would itself be inaccurate.

**The Decision's sentence contradicted a documented contract.** It names a push feed and says that feed offers the
event again. `PushSubscriptionModel`'s javadoc has said since `2df6988a5` of 2026-07-19 that a handler exception
propagates to the caller so the listener can decide whether to acknowledge or redeliver, and it still said so at
`occurrent-0.32.0`, the release this decision shipped in. So the sentence asserted an outcome about the one model it
names, against that model's own contract, nineteen days after the contract was written. A test that asks whether
later work touched a claim can only find claims later work falsified, which is why three passes over this family of
surfaces left it alone.

**The Consequence's sentence assumed a listener that does not acknowledge, and did not say so.** It says a refused
event is not acknowledged, which is true of the saga, and infers that a broker will keep offering it, which needs
whatever fed the event to leave it unacknowledged as well. The sentence needed a condition it did not state, since
the same javadoc gave the listener that choice.

0.34.0 makes it wrong in a stronger way. Occurrent now ships a consume-side bridge, `43af19f9f` adding the
transport-neutral API on 2026-08-18 and `bf72e48d0` the first RabbitMQ bridge a day later, and under
`DeliveryFailurePolicy.PARK` that bridge republishes a refused event to the parking destination and acknowledges it
out of the source queue once the republish is confirmed. The acknowledging side is now a configuration Occurrent
supplies and documents rather than something a listener might happen to do. A park publish that fails redelivers the
original instead, which is one way `PARK` can still end in a redelivery. The dead-letter hedge does not cover
parking, because dead-lettering is the broker's own policy and parking is the bridge's.

## Decision

**The saga declines to acknowledge a refused event, and what happens to it afterwards belongs to the subscription
model.** A `PushSubscriptionModel` hands the acknowledge-or-redeliver decision to the listener that called `accept`,
and on a consume-side broker bridge `DeliveryFailurePolicy` is where the choice is configured. For as long as the
model keeps offering the event, the saga refuses it again, which is the outcome ADR 109 described. What that costs
the events behind it is the feed's, and this record says nothing about it. Where the event is not offered again, the
saga still issues no duplicate commands, and that is the part this decision owns either way.

This is the same rule [#1076](https://github.com/johanhaleby/occurrent/issues/1076) settled on for the 0.34.0 saga
surfaces, applied to the one record those passes ruled out of scope. No public saga API and no saga documentation
says a refused or failing delivery comes back, or blocks what is behind it, without that depending on the
subscription model offering it again. Internal comments, test comments and `PushSubscriptionModel`'s own fan-out
justification still do, and [#1080](https://github.com/johanhaleby/occurrent/issues/1080) on milestone 0.35.0 holds
them.

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
