# 156. A refresh round tells the listeners what they have not heard

Date: 2026-10-29

## Status

Accepted. Fixes [#1349](https://github.com/johanhaleby/occurrent/issues/1349). Amends
[ADR 113](0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md) and
[ADR 140](0140-a-refresh-round-tells-its-listeners-on-a-thread-of-its-own.md). Applies to both MongoDB lease
strategies, which share `MongoLeaseCompetingConsumerStrategySupport`.

## Context

Under ADR 113 the locked step that changes a consumer's lease status also works out which callback that change owes,
and the listeners are told once the lock is released. Under ADR 140 a refresh round hands each such callback to a
notifier for the subscription, and `ScheduledRefresh` kept a queue per subscription id for the callbacks a notifier
had not taken yet.

So a decided grant or prohibition lived in one place only, a local variable or a queue entry, until every listener had
been called. A heap `OutOfMemoryError` can be thrown at any call in between. It can come after the locked step and
before the hand-over, while the queue entry is built or taken, or inside the strategy's own loop over the listeners.
Each of those lost the callback, and no later round decided it again, since the status was already written. A lost
prohibition keeps a subscription running on a node that no longer holds its lease. A lost grant keeps a
subscription paused on the node that holds it. An unregister whose lease removal threw also forgot the consumer
without its listeners ever hearing the prohibition.

## Decision

**Each consumer keeps a record of what its listeners were last told, and every refresh round compares that record
with the lease status and tells the listeners the difference.** A change is owed until a tell of it has finished.
A tell marks the record as unfinished before it calls the first listener, so a tell that a throwable cuts short is
told again by the next round.

The record says either "not granted" or "granted with token T". Two counts on the consumer show what the status alone
hides:

- `lapses` goes up when `hasLock` answers no for an acquired status, and when a locked step replaces an acquired
  status with one that is not acquired or has another token. A grant told before the latest lapse is told again,
  since a listener may have acted on that no.
- `losses` goes up when a locked step replaces an acquired status with one that is not acquired or has another token.
  A record that has not seen the latest loss owes a prohibition, also for a lease whose grant was never told, since a
  caller may have learned of that lease from `registerCompetingConsumer` or `hasLock`.

A commit that extends a lease, and a register that gets the same token back, change neither count.

A round never calls a listener. When the comparison finds something owed, it hands a tell to the notifier for the
subscription, as ADR 140 decided. `ScheduledRefresh` keeps no queue any more. A hand-over the notifier throws on is
dropped and logged, and the next round compares again and hands it over again. Once a hand-over has failed in a
round, the rest of that round's hand-overs are dropped without asking the notifier, so a notifier that can take
nothing costs one log line per round.

A tell says the current difference, not each change in the order a round decided it. A grant that was undone before
it was told may never be told, but the prohibition after it is.

What a listener throws during a tell on a notifier thread is logged, a `RuntimeException` at WARN and an `Error` at
ERROR. During the tell a register, release or unregister makes on its own thread, the call throws it instead. After a
`RuntimeException` the record moves on. After an `Error` the record stays unfinished, so the next round tells every
listener the current answer again. One case stays open. When the `Error` came on a prohibition and the same tell then granted a new token, the next
round repeats only the grant.

### Register, release and unregister

Only one tell for a consumer calls its listeners at a time. A register, release or unregister tells while no other
tell for that consumer is running, and then at most the change its own locked step made. Anything else that is owed,
it hands to the notifier. When another tell is running, the call doesn't wait for it, since that tell may be inside a
listener that waits for a lock the caller holds, the deadlock in ADR 113. It
returns, and the running tell tells the change after its own, or hands it to the notifier. When a tell on a notifier
thread throws from the strategy's own code instead, or one of its listeners throws an `Error`, the next round tells
that change.

That replaces the promise in the last Decision paragraph of ADR 140, that registering, unregistering and releasing
call the listeners on the caller's thread before they return, with the following sentence:

> A register, release or unregister may call the listeners before it returns, and throw what they throw, but only for
> the change it made itself and only while nothing else is calling them about that consumer, and anything else they
> are told, which can include that change, comes on a notifier thread, before or after the call returns, where what
> they throw is logged.

Every call site in `CompetingConsumerSubscriptionModel` handles a callback that arrives later on another thread. Each
one records its own state before the call, so the callback finds nothing left to do, or reads the state again after
the call. The calls made while the model shuts down need neither, since it ignores every callback once shutdown has
begun. A callback on a notifier thread takes the same lock and reconcile path that every callback a
round decided already takes.

An unregister keeps the consumer until its lease is removed and its listeners have heard the prohibition. Meanwhile
`hasLock` answers no for it and no round extends its lease. Each round removes the lease again and tells what is
owed.

### Rejected: telling while another tell runs

A register, release or unregister could tell its own change at once, also while another tell for the same consumer
runs, which keeps ADR 140's promise in full. The record then has two writers. In that design a register that ran
during a round's grant tell turned the grant into a prohibition, and `CompetingConsumerSubscriptionModel` acts on a
prohibition by pausing the subscription and releasing its lease.

## Consequences

A listener can be told the same thing twice. That happens after an `OutOfMemoryError` cut a tell short, and after an
`Error`, once per round for as long as a listener keeps throwing it. It also happens when a lease is lost between a
tell's read of the status and its read of the counts. The tell then tells the grant once more, and the prohibition
after it. A listener can also be told a prohibition with no
grant before it. The `CompetingConsumerListener` javadoc says both, and `CompetingConsumerSubscriptionModel` handles
both. ADR 113's "one change of status is reported once" now holds except in those three cases.

An `Error` that a listener throws during a tell on a notifier thread reaches the log at ERROR and not the caller. In
0.34.0 a listener's `Error` from a register, release or unregister came out of `subscribe`, `resume` or `cancel`.

Two bounds are known:

- While the notifier hasn't started a consumer's tell, each round can hand it one more. Each of those finds another
  tell running or nothing owed, and returns. The notifiers that `ScheduledRefresh.auto()` and `every(..)` build start
  a thread per tell. A flag against it, set by the round and cleared by the tell, can be lost to an `OutOfMemoryError`
  together with the next hand-over, so the bound costs less than that fix.
- An unregistered consumer whose listener keeps throwing an `Error` on the prohibition is never forgotten while that
  lasts. Every round removes its lease again and tells every listener the prohibition again, up to the life of the
  process. Nothing is lost, and it ends the first round after the listener stops throwing. Bounding it means
  forgetting a consumer whose prohibition was never heard, which is the loss this ADR fixes, or keeping state per
  listener.

The one-tell-at-a-time rule rests on an assumption the stripe lock already makes. Its `unlock()` in a `finally` is
trusted not to throw at the depth where its `lock()` just succeeded. That is not verified.

[ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and
[ADR 139](0139-a-node-that-gave-up-a-lease-writes-with-the-token-it-held.md) are affected in one place only. The
strategy now records the last fencing token it was granted before it writes the new status, so an `OutOfMemoryError`
between the two can't leave `fencingToken` answering with an older token once that lease is lost.
