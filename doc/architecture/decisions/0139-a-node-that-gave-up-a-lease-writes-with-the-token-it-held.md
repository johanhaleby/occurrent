# 139. A node that gave up a lease writes with the token it held

Date: 2026-09-29

## Status

Accepted. Amends [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md), which shipped in
0.33.0 and is therefore corrected by reference rather than edited. Resolves part of
[#1166](https://github.com/johanhaleby/occurrent/issues/1166).

## Context

ADR 116 has `fencingToken(subscriptionId)` answer with a token only while the one consumer registered for that
subscription holds the lock, and empty otherwise. An empty answer makes `DurableSubscriptionModel` write the checkpoint
with `CheckpointWriteCondition.any()`, which MongoDB accepts whatever version the stored checkpoint has.

That rule misses the one write the fence exists for. A handler that started while this node held the lease can still be
running after the node gave the lease up, through a pause, a release after a lost refresh, or an unregister. By the
time it finishes, `fencingToken` answers empty, its checkpoint write goes out as `any()`, and it overwrites whatever the
new holder has written since. The next resume then starts from the older position and delivers again what the new
holder already handled.

ADR 116 saw half of this. Its Status record says the stale token is what the fence refuses, and it counts
`LOCK_RELEASED` as not holding the lock because the token belongs to the lease just given up. Both are true, and
together they mean that the one write that needs the stale token is the one that never gets it.

Reading the token after the handler returns has a second gap. A node that loses the lease and wins it back while one
of its handlers is still running answers with the new, higher token, and the old handler's write is accepted even
though another node may have moved the checkpoint in between.

## Decision

**`fencingToken` answers with the token of the last lease this node held for the subscription once it no longer holds
one, and `DurableSubscriptionModel` reads the token before it calls the handler, not after.**

The strategy keeps the last token it was granted per subscription id. Losing the lease, releasing it or unregistering
does not remove that token. A node that never held a lease for a subscription still answers empty, so a subscription
that does not compete at all, whose checkpoint writes the Spring Boot starter also asks the same strategy about, keeps writing with
`any()` exactly as before. The rule for two consumers registered for one subscription in the same instance is
unchanged, and it still answers empty for as long as both are registered.

A write made with the stale token is refused once another node has written with a higher one. On
`NativeMongoSubscriptionModel` that refusal ends delivery on this node (ADR 116). On `SpringMongoSubscriptionModel` it
does not. Spring's `CursorReadingTask` hands the exception to the error handler and goes on reading the change stream,
and the error handler only keeps the subscription from being restarted, by ending a restart loop that runs for it or
never starting one. A write made with the stale token before anyone else has written is accepted, which is correct,
since nothing newer exists to be overwritten.

Reading the token before the handler runs ties the write to the lease that was held when the event was handed over. A
node that won the lease back in the meantime cannot lend its new token to a handler that started under the old one.

## Consequences

`CompetingConsumerStrategy.fencingToken`'s javadoc says empty means the node never held the lock for that subscription
or has no token to give, not that it does not hold the lock right now. The token map holds one entry per
subscription id this node has ever held a lease for.

`StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` and `CatchupThenPushSubscriptionModel` still read the
token after the handler returns. The stale token closes the lease-lost case for them too, but the case where the same
node wins the lease back while an old handler is still running is only closed for `DurableSubscriptionModel`, which is
the model a competing consumer subscription checkpoints through.
