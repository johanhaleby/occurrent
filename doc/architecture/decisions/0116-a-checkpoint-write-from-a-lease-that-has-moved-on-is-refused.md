# 116. A checkpoint write from a lease that has moved on is refused

Date: 2026-08-09

## Status

Accepted. Designs the fix for #665, which stays open for the implementation.
[ADR 115](0115-a-lease-fencing-token-is-computed-but-not-yet-checked.md) corrected the javadoc that
promised this and deferred the design here. Builds on
[ADR 113](0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md) and
[ADR 114](0114-a-lease-expires-on-the-database-clock-not-the-asking-nodes.md), and applies
[ADR 106](0106-a-refused-subscription-call-says-which-condition-it-hit.md)'s rule for which exception
a refusal gets.

## Context

`MongoListenerLockService.acquireOrRefreshFor` computes a `version` that increments whenever the lease
changes hands and stays put on a refresh. It comes back as `ListenerLock.version()` and nothing reads
it. A subscriber whose lease has moved to another node can still write a checkpoint, and if the new
holder has already written a later one, that write moves the checkpoint backward and the new holder
redelivers work already done.

Three things stood between the version and a real check, and #665 names all three.
`CompetingConsumerStrategy` answers in booleans and offers no way to read the current value. Four
`CheckpointStorage` implementations would each need the comparison. And `CompetingConsumerSubscriptionModel`
sits between them without ever seeing an event, since it only starts and stops the model it wraps, so
there was no path from the one to the other.

A fencing token is the standard answer to this. It is a number that increases every time a lock
changes owner, and the thing being written to remembers the highest one it has seen and refuses
anything lower. What follows is what it takes to make Occurrent's version into one, and the answer
turned out to be less about the two public interfaces than about the lease itself.

## Decision

### The lock document survives a release, and this ships one release before the fence

The version is not a fencing token yet, because it restarts. `MongoListenerLockService.remove` deletes
the lock document, and the acquisition pipeline seeds a fresh one to 0 through
`$ifNull($add($version, 1), 0)`. Every ordinary way of giving a lease up goes through `remove`:
unregistering, a user pause, handing a granted lock back, `stop()`, and the `@PreDestroy` on
`shutdown()`.

Build a fence on that and an ordinary deployment stops the subscription for good. A node holding
version 5 writes checkpoints stamped 5, its context closes, the document is deleted, and the next node
acquires a new document at version 0. Every write it makes offers 0 against a stored 5, is refused,
and a refresh never raises the version.

**So `remove` unsets `subscriberId` and `expiresAt` instead of deleting.** The filter stays `{_id,
subscriberId}`, so only the holder can release. `isAllowedFor` already treats a missing `subscriberId`
as a free lock, so a released lock is taken exactly as a deleted one was, except that the version
increments instead of resetting. Unsetting `expiresAt` is not needed for that, since `lockIsExpiredExpr`
already reads a missing one as expired, and it is worth doing so somebody reading the collection can
tell a held lease from an unheld one. The return type goes from `DeleteResult` to `UpdateResult`, which
the only call site discards, and `remove` is package private in an `internal` package.

The release keeps the collection's default write acknowledgement rather than moving to majority like
acquiring and refreshing. It runs on the `@PreDestroy` path, once per subscription and serially, before
the strategy's own shutdown clears the flag that lets its retries run, and majority acknowledgement
carries no timeout, so a replica set that has lost a majority would hold the context open until the
process is killed. A release that a failover rolls back leaves the lease looking held until it expires,
which is what expiry is for. An acquisition rolled back would be a different matter, since two nodes
would then both believe they hold the lease, and that write already asks for majority.

**This ships alone, one release ahead of everything else here, and that is a decision rather than a
preference.** During the upgrade that installs it, old and new nodes run the same subscription. An old
node's `remove` still deletes the document, the new node's next refresh matches nothing and re-acquires
a fresh document at version 0, and if the fence were already on, every write from that node would be
refused for good. That is the failure this decision exists to prevent, caused by the deploy that
installs it. A lock document that persists with an increasing version changes nothing anybody can
observe, so shipping it by itself costs a release boundary and no design.

Rejected: a setting that turns the fence off for one release. It keeps both in one release and leaves
behind a switch whose only purpose is to be deleted.

One invariant goes with this. The lock collection must not be dropped independently of the checkpoint
store, since that resets the tokens under checkpoints that remember higher ones. Recovery needs no new
API, because `CheckpointStorage.delete(subscriptionId)` clears the checkpoint and its token together.

### The strategy answers with a token, through a default method

```java
default OptionalLong fencingToken(String subscriptionId) {
    return OptionalLong.empty();
}
```

The javadoc states four things. The value increases on every genuine change of owner and is unchanged
when the same holder refreshes. Empty means this node does not believe it holds the lock, or this
strategy has no token to give. The call must not block and must not reach a database, because it runs
on a per-event write path. And an implementation that does not override it is not broken, it simply
has no fence.

`MongoLeaseCompetingConsumerStrategySupport` stops discarding the version. `acquireLease` keeps
`ListenerLock.version()` and the `Status` enum becomes a small carrier holding it for the acquired
case. The cached value stays honest for as long as the node believes it holds the lease, because
`commit` leaves the version alone and a lost commit moves the consumer to `LOCK_NOT_ACQUIRED`. That
staleness is the whole point, since the stale token is what the fence refuses.

**The answer is a token only when exactly one consumer is registered for that subscription, whatever
its status, and that one consumer holds the lock. Otherwise empty.** The obvious rules are both wrong
in the same direction. Answering with the highest token among the holders lets a consumer sitting at
version 5 with an expired lease it has not noticed be handed the winner's version 6, and its stale
write accepted. Answering only when exactly one consumer holds the lock does the same thing one step
later, because a consumer is demoted by its own refresh before the subscription is paused, so there is
a window where the winner is the only holder and the loser is still delivering. Counting every
registered consumer closes it, since the loser stays registered throughout. The lowest token is no
better than the highest, because it refuses the legitimate writer. `LOCK_RELEASED` counts as not
holding the lock, since its token belongs to the lease it just gave up.

Standing down is not free and the cost is worth stating. An unconditional write leaves the stored
version alone, so the fence re-arms as soon as the ambiguity clears, but the write itself still lands and can
still move the checkpoint back while it lasts.

One strategy instance serving two consumers of the same subscription id is competing consumers inside
a single node, which is not a configuration Occurrent expects, and the fence stands down for as long
as both are registered. Where that ends is worth writing down. Pausing one of them unregisters it after
asking the model to stop delivering, and the native model waits a second for a delivery thread before
carrying on, so a handler slower than that is still running when the count drops back to 1 and its
write can be handed the other consumer's token.

Rejected: a throwing default, on the `EventStoreFixture.notOverridden` precedent
[ADR 107](0107-what-a-tck-version-promises.md) cites. That precedent fits a TCK fixture member, where
not overriding means the suite cannot run and there is no correct fallback. Here "I have no token" is
a well-defined answer that reduces to today's behaviour.

Rejected: widening `registerCompetingConsumer` or `hasLock`. Breaking for the third-party
implementations the interface invites, and it answers at the wrong moment, since the token is needed
at every checkpoint write rather than once at registration.

### The checkpoint store learns about versions, and never about leases

A `CheckpointStorage` records where a subscription has read to. Nothing in that job involves a lease,
and a method parameter called a fencing token would put competing consumers into the vocabulary of
every checkpoint store anybody writes, including the SQL one filed as #403. An implementer would have
to understand distributed leasing to implement a checkpoint store correctly, which is the wrong thing
to ask of them.

So the store gets a concept of its own, and it is the one Occurrent already uses for the event store,
which is a write that states what must be true of the stored version before it is allowed.

```java
public sealed interface CheckpointWriteCondition {

    static CheckpointWriteCondition any();

    static CheckpointWriteCondition notOlderThan(long writeVersion);

    record Any() implements CheckpointWriteCondition {}

    record NotOlderThan(long writeVersion) implements CheckpointWriteCondition {}
}

public interface VersionedCheckpointStorage extends CheckpointStorage {

    Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

    OptionalLong writeVersion(String subscriptionId);
}
```

`any()` writes the checkpoint and leaves the stored version untouched, which is what
`CheckpointStorage.save` has always meant. `notOlderThan(v)` refuses when the stored version is greater
than `v`, and otherwise writes the checkpoint and records `v`. Versions come from the caller rather
than from the store, and the store never learns where they come from.

Naming the condition rather than passing a bare number is what makes the two write modes one operation
instead of two with different guarantees, and it is what lets the refusal say which condition was not
met. It also puts the rule that an unconditional write must leave the stored version alone into the
type, where every implementation has to face it, rather than into a paragraph of this document that
two of them might read differently.

`writeVersion` is not there for the fence, which never reads it. A store that records something its
caller cannot read back tells half the truth about its own state, and the failure this ADR guards
against ends with somebody asking which version is stored and why their writes are refused. Answering
that by reading a MongoDB document by hand is not an answer a library should leave people with.

The fencing token is what the competing consumer layer supplies as the version, and it stays entirely
on that side, in one small type that knows both halves.

```java
public final class FencedCheckpointStorage {

    public static CheckpointStorage fencedBy(VersionedCheckpointStorage storage, CompetingConsumerStrategy strategy);

    public static CheckpointStorage fencedBy(VersionedCheckpointStorage storage, Supplier<CompetingConsumerStrategy> strategy);
}
```

It asks the strategy, and writes with `notOlderThan(token)` when it gets one and `any()` when it does
not. Standing down is a call rather than an absence, which is the whole of the rule the earlier
sections describe.

All of it lives in `subscription/api/blocking` and `subscription/core`, next to the types it needs, so
there is no new module and no new dependency. `CheckpointStorage` itself does not change.

A separate storage interface rather than a default method on `CheckpointStorage`, because a store that
cannot write conditionally has to be caught where somebody wires it rather than at the first checkpoint
write. Both places that wire it name a concrete type, so that is a compile error and nobody pays a
runtime check for it. It also gives the conformance suite something to attach to, so a future store
inherits a contract instead of an implementer deciding whether they have opted into one.

**A condition of its own rather than the event store's `WriteCondition`, and this is the one place the
two deliberately diverge.** `streamVersionEq` means the version I expect equals the stored one, and the
event store assigns the next version itself. Here the version is assigned outside the store and the
rule is not older than what is stored, so sharing the type would hand checkpoint stores conditions like
`lt(5)` that mean nothing to them and would have to be refused at runtime. Same idea, stated for this
store, which is worth more than a shared class name.

`CheckpointWriteCondition` is sealed and has exactly the two cases anything needs today. A third case
would be a considered change to a contract every store implements, which is the right weight for it,
and the alternative of an open predicate would put a condition interpreter written in Lua into the
Redis storage for cases nobody has asked for.

### Nothing carries the token through the subscription models

The wrapper asks the strategy at write time, and the `subscriptionId` the save call already has is the
only key it needs. No subscription model changes, and every checkpoint write is covered rather than
only the durable one, including both catch-up models, the durable model's seed write, the catch-up
handover writes, the start position `ManualStartSubscriptionModel` records and
`CatchupThenPushSubscriptionModel`'s catch-up marker.

This is what dissolves the third problem in #665. The token never travels from the strategy through
`CompetingConsumerSubscriptionModel` to the write, because the strategy and the storage meet directly
at a subscription id.

Rejected: having `CompetingConsumerSubscriptionModel` install a token source on the model it wraps. It
needs a second capability interface, a mutable setter on a public subscription model, and a walk down
the delegate chain, and it still cannot reach the storage the catch-up models hold.

Rejected: a thread-local set around the delivery callback. Hidden coupling, and it cannot work for any
reactive path.

Rejected: `DurableSubscriptionModel` taking the strategy as a constructor argument. It covers one write
site out of seven and adds constructor overloads to several models.

The cost is that a hand-wired user has to wrap the storage, and forgetting leaves today's behaviour.
The version nobody can forget needs `CompetingConsumerSubscriptionModel` to own the storage, which
inverts every shipped wiring and breaks `DelegatingSubscriptionModel`. The Spring Boot starter below
removes most of the exposure.

### The comparison rule, and what it does not promise

`notOlderThan(v)` is accepted when no version is stored, or when the stored version is not greater than
`v`. Nothing stored means a checkpoint written before any of this existed, so every shipped deployment
stays readable and there is nothing to migrate.

`any()` leaves the stored version alone, and that is why it is a case of the condition rather than a
missing argument. Unconditional writes stay alive in three places, which are a hand-wired user who did
not wrap the storage, a node still on the previous release during a deploy, and the moment where the
strategy stands down. Both Mongo storages write a full replacement document today, so an unconditional
save would take the version with it, and a stale writer offering an old one would then find nothing
stored and be accepted. So `CheckpointStorage.save` carries the stored version forward. Redis gets this
for free.

**The guarantee is about ownership rather than position, and it starts once every node runs the release
that carries the fence.** From then on a write carrying a token lower than the stored one is refused,
permanently, and the stored version never decreases and is never erased. During the deploy that installs
the fence it does not hold, because a node on the previous release still writes a full replacement and
takes the token with it, so the fence is off until the deploy finishes and re-arms on the first write
after it.

There is a window this does not cover, and it is smaller than it looks but real. Between a takeover and
the new holder's first write, the stored version is still the old holder's, so an old holder that has not
noticed can write. The new holder read the checkpoint when it subscribed and writes unconditionally
afterwards, so if the old holder wrote a later position inside that window, the new holder's first
write moves the stored position back. The design accepts this. It stays inside at-least-once, which is
the contract, and the events in between are redelivered rather than lost.

Rejected: arming the fence on takeover by writing the current token during `read`, which is called at
exactly that moment. It closes the window, and it turns a read into a write and adds a round trip to
every read including a user's own, to buy one window against redelivery that the contract already
allows. Worth reaching for if the window ever proves to matter.

Rejected, and this is the one worth recording because it needs no design at all. Refusing any
checkpoint that moves backward. `Checkpoint` exposes only `asString()` and a MongoDB resume token is opaque, so
checkpoints are not ordered and backward is not computable. It would also refuse a deliberate rewind,
where a user restarts a subscription from an earlier position. The token tells apart the right thing,
which is that a newer owner exists rather than that a position is older.

### A refused write throws, and it must never be retried

`CheckpointWriteConditionNotFulfilledException extends IllegalStateException`, in `subscription/core`,
naming the subscription id, the version stored and the condition that was not met. Those are the three
things `WriteConditionNotFulfilledException` names for the event store, which is the point.

ADR 106's question decides the root. Can the caller fix it by passing something else? No. The node lost
its lease, which is the state of another machine, and that is the wording ADR 106 used to keep a
competing consumer's lock refusal out of the `IllegalArgumentException` family. It does not join that
sealed family, which is rooted in `IllegalArgumentException`.

Not a returned outcome, because `save` returns `Checkpoint` for chaining and no caller inspects it, so
a quietly refused save would look exactly like a successful one, which is this issue's bug in different
clothes.

**Excluding the refusal from every retry on the path is part of the decision.** Both Mongo subscription
models retry a throwing delivery action forever with the model-wide shutdown flag as the only stop
condition, and the retried action is the durable model's own lambda, so the user's `action.accept` runs
again on every attempt. Both Mongo storages retry `save` on their own terms as well. Two things go
wrong, and the second is a correctness bug rather than noise.

1. The node re-runs the user's side effects every couple of seconds for as long as the process lives.
2. The retry loop still holds the position from the delivery that was refused, while the wrapper asks
   the strategy for a token again on each attempt. If this node later takes the lease back at a higher
   token, an attempt from that loop is accepted and writes the old position over the newer one. The
   fence would be beaten by waiting.

Excluding it costs no API change, because the call sites already pass their own predicate and
`RetryExecution` combines it with the strategy's own.

The per-event retry is not the whole path in either model. The native model wraps the entire internal
subscription in a second unbounded retry, and an exception escaping the inner one is caught, falls into
the restart branch, and is rethrown into the outer one, which restarts from the same position forever.
The Spring model reaches the same place by a different route, through an error handler that does not
recognise a fence refusal and reports a restart that runs its own unbounded loop from a position
deliberately not advanced past the failed event.

Two requirements go with the exclusion, because "the subscription stops" can be read a way that makes
things worse. **The subscription must stay known and pausable on that node.** The native model's
history-lost branch forgets the subscription id, and copying that here breaks the recovery path, since
the strategy then pauses a subscription the delegate no longer knows, and that refusal is thrown on the
lease refresh thread inside a listener loop with nothing catching it, which abandons the refresh for
every other consumer on the node and is retried forever. One subscription's refused write would stop
lease refresh for all of them. **And the stop is logged at error level before the exception is
rethrown**, because otherwise it reaches an executor's uncaught handler and nothing says why the node
went quiet.

What an operator sees is that the event is not acknowledged, the strategy's next refresh finds the
lease gone and pauses the consumer within one lease period, and the new holder redelivers the event.
**At-least-once is the contract and it is unchanged. The fence removes checkpoint regression by a node
that lost its lease, and it does not promise exactly-once delivery.**

One limit is named rather than fixed. A node that lost the lease and later takes it back resumes its own
subscription from the position it had, not from the checkpoint the other node advanced, so it can write
an older position under a legitimately higher token. That is a different defect, it corrects itself
through redelivery, and it is filed separately.

### Storage mechanics

**Both Mongo storages.** One round trip. `findOneAndUpdate`, filter on `_id` alone, upsert,
`returnDocument AFTER`, and an update pipeline whose `$cond` writes the new document only when the
stored version is missing or not greater than the one being written, and yields `$$ROOT` otherwise. The caller
tells the outcomes apart by comparing the token on the returned document to the one it offered, which is
how ADR 114 made `acquireOrRefreshFor` tell its own two outcomes apart, including its reason for
matching on `_id` alone against the unique index rather than leaning on a duplicate-key error. What gets
written is `$mergeObjects` of the new document and the stored version, in that order, so no stale
`resumeToken` survives beside a new `operationTime` and the legacy `subscriptionPosition` field still
disappears on first write.

The new document is wrapped in `$literal`. An update pipeline evaluates what it writes, unlike a
replace, so any string starting with `$` is read as a field path instead of a value, and both the
subscription id and the string form of a `Checkpoint` come from the caller. Without it a subscription
named `$foo` writes something other than its own name. That, and whether the pipeline behaves as
described at all, is checked against a live replica set before anything is built on it, the way ADR 114
checked its own pipeline and found the naive form wrong.

**Redis.** The existing key and its plain string value stay exactly as they are. The token goes in a
separate prefixed key, one Lua script does the comparison and both writes atomically, and `delete`
removes both. This keeps a rolling deploy safe, which matters more here than anywhere else, because a
competing-consumer cluster runs mixed versions by definition during one. An old node still does a plain
`GET` against an unchanged key holding an unchanged value.

That claim only holds if the script and the ordinary writes agree on how values become bytes.
`SpringRedisCheckpointStorage` holds a `RedisOperations<String, String>` and writes through
`opsForValue().set`, which uses whatever value serializer the template was built with, and nothing requires
that to be a string serializer. So the script uses the `execute` overload taking explicit serializers and
a `RedisScript<Long>` result rather than inheriting the template's.

Rejected: moving the value into a hash holding both fields. It works on Redis Cluster, and an old node's
`GET` against a hash fails with `WRONGTYPE` for the whole length of a rolling deploy.

The cost of two keys is Redis Cluster, where a script over two differently-named keys is refused for
crossing slots, and the two names cannot be brought into one slot without renaming the existing
checkpoint key. That failure is immediate and reaches only a cluster user who turned the fence on, while
an unfenced cluster user is untouched.

The blocking in-memory storage implements the capability too, which is a few lines and gives the new
conformance suite something to run without a container.

### The Spring Boot starter fences the storage it creates

The starter hands its `CheckpointStorage` to the durable model, the catch-up config and
`ManualStartSubscriptionModel.stoppedByDefault`, and that is not the whole list. The projection and saga
registrars each pull `CheckpointStorage` straight out of the application context, and both feed
`CatchupThenPushSubscriptionModel`, which writes checkpoints.

**So `occurrentCheckpointStorage` returns the fenced storage itself.** Every consumer of the bean gets
it, however it resolves the bean, and there is one wrapping site rather than a list to keep in step.

The strategy has to be reached without the storage bean depending on it, which is why `fencedBy` takes a
`Supplier<CompetingConsumerStrategy>` as well as a strategy. The strategy bean depends on
`List<CompetingConsumerListener>`, an open extension point, so a user listener that injects
`CheckpointStorage` would close a cycle if the storage bean asked for the strategy while it was being
built. Resolving it on first use through an `ObjectProvider` removes the dependency. The provider is
asked with `getIfUnique` rather than `getIfAvailable`, because an application with two strategy beans
starts today and must keep starting, which also means two strategy beans mean no fence.

Three rules on that lazy resolution, each of them a way the fence could otherwise be turned off
quietly. Only a strategy that was actually found is remembered, so a first attempt that finds nothing is
tried again rather than disabling the fence for the life of the process. The field holding it is
`volatile`, and resolving twice is harmless. And a resolution that fails rather than returns nothing
counts as no token, which keeps a checkpoint write from a registrar-driven subscription working while
the strategy bean is still being built.

`occurrentCheckpointStorage` is `@ConditionalOnMissingBean(CheckpointStorage.class)`, so a user-declared
bean replaces it and is never wrapped. **The starter fences the storage it creates, and a storage you
supply is yours to wrap**, which is one line. No runtime type check, no warning, and no application
breaks on upgrade because it declared its own storage bean.

### The reactor checkpoint storage stays as it is

#665 names four storages and `ReactorCheckpointStorage` should not be one of them yet. There is no
reactor competing consumer model and no reactor `CompetingConsumerStrategy`, and the only bridge from a
reactor model into a blocking one lives in the TCK, so no reactor checkpoint write is ever made under a
lease. Fencing it now builds for a caller that does not exist.

What it would take, so nobody designs it again, is a matching capability interface in
`subscription/api/reactor`, a wrapper that signals `Mono.error(CheckpointWriteConditionNotFulfilledException)` instead of
throwing, and the no-database contract on `fencingToken`, which is what makes calling the blocking
strategy from a reactive pipeline legitimate. Same pipeline as the blocking Mongo storages.

One thing follows from leaving it alone. `ReactorCheckpointStorage` keeps writing a full replacement and
would take a stored version with it, and both starters default their checkpoint collection to the same
property, so a checkpoint collection the blocking storage fences must not also be written by the reactor
one.

### None of this breaks a caller, so there is no recipe

A default method on an interface, a new interface, new implementations of it, a new exception, a new
Redis key, and an internal method that stops deleting a document are all additive. No call site changes
shape, which is the test ADR 106 applied before concluding it owed no recipe either. So a changelog
entry under Changes rather than Breaking changes, a new-capability section in the migration guide, and
no `org.occurrent.UpgradeToOccurrent_*` recipe.

## Consequences

The lock collection now keeps one small document per subscription id ever used, where it used to delete
them. That document is the subscription's lease record, and its `version` is the fencing token.

The upgrade has an order and the migration guide states it as a requirement. Deploy the release that
carries the lease change before the release that carries the fence. An operator who skips it gets a
cluster where an old node still deletes the lock document while a new node writes fenced checkpoints,
so a subscription is refused, stops, releases, is taken over at a token still below the stored one, and
repeats, once per unit of the stored version, each cycle costing a lease period and one re-run of the
user's action. It recovers on its own and `CheckpointStorage.delete(subscriptionId)` ends it
immediately, at the price of a replay.

A user who wires their own subscription models gets the fence by wrapping the storage and gets today's
behaviour by not wrapping it. A user on the Spring Boot starter gets it without doing anything, unless
they declare their own `CheckpointStorage` bean, which is theirs to wrap.

A `CheckpointStorage` implementation outside this repository keeps working untouched and has no fence.
Implementing `VersionedCheckpointStorage` is what opts in, and the new conformance suite is what says
whether it did it correctly, including the case where `any()` must leave a stored version alone.

Redis Cluster is not supported for fenced checkpoints, and turning the fence on there fails at the first
write rather than quietly. An unfenced cluster user is unaffected.

This is the first place in Occurrent where a subscription refuses a checkpoint write made on behalf of
application code. The exception is deliberately not retried, which is the opposite of how every other
failure on the delivery path is treated, and the reason is written into the retry predicates rather than
left to a reader of the stack trace.

The implementation registers as its own epic against the two release boundaries above, split into the
lease change, the strategy's token, the checkpoint store's write condition, the Mongo storages, the retry exclusion in
both subscription models, the Redis storage, the starter wiring, an end-to-end proof over a real MongoDB
covering both an expired lease and a graceful handover, and the documentation.
