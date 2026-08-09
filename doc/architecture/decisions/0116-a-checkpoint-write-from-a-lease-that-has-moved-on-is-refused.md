# 116. A checkpoint write from a lease that has moved on is refused

Date: 2026-08-09

## Status

Accepted. Designs the fix for #665, which stays open for the implementation.
[ADR 115](0115-a-lease-fencing-token-is-computed-but-not-yet-checked.md) corrected the javadoc that
promised this and deferred the design here. Builds on
[ADR 113](0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md) and
[ADR 114](0114-a-lease-expires-on-the-database-clock-not-the-asking-nodes.md), and applies
[ADR 106](0106-a-refused-subscription-call-says-which-condition-it-hit.md)'s rule for which exception
a refusal gets and [ADR 93](0093-a-missing-capability-is-refused-and-a-reactive-publisher-is-cold.md)'s
rule for a store that cannot do what it is asked.

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

### The lock document survives a release

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

**Both ship together, in 0.33.0, which supersedes the two-release order this ADR first recorded.**
During the one deploy that installs them, the cluster runs mixed versions for a while, and that window
is accepted rather than avoided. A 0.32.0 node's `remove` still deletes the lock document. A 0.33.0 node
that takes the lease over from it re-acquires at version 0, and its checkpoint writes are refused,
because the fence is already live on that node. Each further takeover during the deploy raises the
version by one, so the cycle resolves on its own once every node runs 0.33.0. An operator who wants it
to stop sooner can call `CheckpointStorage.delete(subscriptionId)`, which ends the loop immediately at
the price of a replay. One release boundary was not worth its ceremony for a window this bounded and
self-correcting, and the maintainer made that call with this failure mode stated to him.

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

### A checkpoint write states its condition, on `CheckpointStorage` itself

A `CheckpointStorage` records where a subscription has read to. Nothing in that job involves a lease,
and a parameter called a fencing token would put competing consumers into the vocabulary of every
checkpoint store anybody writes, including the SQL one filed as #403. So the store gets a concept of
its own, and it is the one Occurrent already uses for the event store, which is a write that says what
must be true of the stored version before it is allowed.

```java
public sealed interface CheckpointWriteCondition {

    static CheckpointWriteCondition any();

    static CheckpointWriteCondition notOlderThan(long writeVersion);

    record Any() implements CheckpointWriteCondition {}

    record NotOlderThan(long writeVersion) implements CheckpointWriteCondition {}
}
```

`any()` writes the checkpoint and leaves the stored version untouched, which is what
`CheckpointStorage.save` has always meant. `notOlderThan(v)` refuses when the stored version is greater
than `v`, and otherwise writes the checkpoint and records `v`. Versions come from the caller rather
than from the store, and the store never learns where they come from.

**This goes on `CheckpointStorage` and on its reactor twin, not on a second interface beside them.**

```java
Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

default Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
    return save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
}

OptionalLong writeVersion(String subscriptionId);
```

An unconditional checkpoint write is unsafe the moment more than one node can write, and competing
consumers is a first-class feature of this library, so writing conditionally is part of what a
checkpoint store is rather than an extra somebody opts into. That is what puts it on the interface every
checkpoint store already implements.

Rejected: **a second interface beside `CheckpointStorage` carrying the conditional write**, so that
nothing already implementing the latter would break. Not breaking anything is the wrong reason to shape
an interface. AGENTS.md says to avoid breakage where avoiding it costs nothing and not to preserve a
mistake just to avoid it, and avoiding it here would cost a second public interface and a capability
question wherever a store is wired, for a guarantee that is not optional.

**A store that cannot evaluate anything but `any()` refuses the others with
`UnsupportedOperationException`.** That is [ADR 93](0093-a-missing-capability-is-refused-and-a-reactive-publisher-is-cold.md)'s
rule, and the question ADR 106 restated. No different argument helps, and the caller needs a
differently built store. Redis on a cluster is exactly that case, and so is any store that has not caught up yet.

The two-argument `save` stays as a default, so **no calling code changes at all**. What breaks is
implementations, which now implement the three-argument method and answer `writeVersion`, and that gets
an `org.occurrent.UpgradeToOccurrent_*` recipe and a migration-guide section.

`writeVersion` is not there for the fence, which never reads it. A store that records something its
caller cannot read back tells half the truth about its own state, and the failure this ADR guards
against ends with somebody asking which version is stored and why their writes are refused. Answering
that by reading a MongoDB document by hand is not an answer a library should leave people with.

Naming the condition rather than passing a bare number is what makes the two write modes one operation
instead of two with different guarantees, and it is what lets the refusal say which condition was not
met. It also puts the rule that an unconditional write must leave the stored version alone into the
type, where every implementation has to face it, rather than into a paragraph of this document that two
of them might read differently.

**A condition of its own rather than the event store's `WriteCondition`.** `streamVersionEq` means the
version I expect equals the stored one, and the event store assigns the next version itself. Here the
version is assigned outside the store and the rule is not older than what is stored, so sharing the type
would hand checkpoint stores conditions like `lt(5)` that mean nothing to them. Same idea, stated for
this store, which is worth more than a shared class name.

`CheckpointWriteCondition` is sealed and has exactly the three cases anything needs today, `any()`,
`notOlderThan(v)` and `ifAbsent()`. A fourth case would be a considered change to a contract every store
implements, which is the right weight for it, and an open predicate would put a condition interpreter
written in Lua into the Redis storage for cases nobody has asked for.

**`ifAbsent()` is the case this section originally left out.** `notOlderThan(v)` accepts a write when
nothing is stored yet, so it cannot tell a subscription's first checkpoint apart from a later one.
Pinning a subscription's very first checkpoint, what `ManualStartSubscriptionModel.stoppedByDefault`
does, needs exactly that distinction. Two nodes racing to start the same subscription both see nothing
stored, and under `notOlderThan` both writes could succeed, with whichever writes second silently
winning and losing the events between the two positions (#669). `ifAbsent()` succeeds only while nothing
is stored, so the first of two racing writes wins and the second is refused instead of silently
overwritten. That need surfaced after this section was written, which is why the starter section further
down already assumes `ifAbsent()` exists.

### A subscription model stamps its own checkpoint writes

The models that write checkpoints are the ones that should know what they are writing them as, so they
take a source of write versions and use it.

```java
@FunctionalInterface
public interface CheckpointWriteVersionSource {
    OptionalLong writeVersion(String subscriptionId);
}
```

`DurableSubscriptionModel`, `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` and
`CatchupThenPushSubscriptionModel` each take one beside their storage, and every checkpoint they write
becomes `notOlderThan` the version they get, or `any()` when they get none. A model with no source
writes `any()`, which is one code path rather than two.

**A model does not learn what a lease is.** It learns that it has a source of write versions and that
it stamps its writes with one. No competing-consumer type is imported or depended on by a model that
writes checkpoints. `DurableSubscriptionModel`'s own javadoc does name `CompetingConsumerSubscriptionModel`
and a lease handover, but only in prose describing what a caller above it may be doing, never as a type
its code references. A model with no source behaves exactly as it does today.

**Neither interface names the other, and the two are joined by a method reference at the wiring site.**

```java
new DurableSubscriptionModel(inner, storage, strategy::fencingToken)
```

`CompetingConsumerStrategy` does not implement `CheckpointWriteVersionSource`, because a name with
Checkpoint in it has no business on a distributed lock. The lock offers a fencing token, the model
wants a write version, the shapes agree, and the wiring is where they meet. That is the whole of the
coupling between competing consumers and checkpointing.

Rejected: **a `CheckpointStorage` that wraps another one, asks the strategy itself and rewrites the save on the way past.** It touches no
subscription model and covers every write site from one wiring point, which is why it was attractive.
It is also not a checkpoint storage. It is a competing consumer object wearing the storage interface,
and it strengthens `CheckpointStorage.save` from behind that interface, so four models and every user
holding the injected bean have a reference that can now refuse a write with nothing in the type saying
so. Touching nothing else was its only real advantage, and that is not an advantage.

Rejected: **making the fence internal to `CompetingConsumerStrategy`.** A fence is a condition
evaluated at the write, by whatever performs the write, and the strategy never performs one. Keeping
the writer ignorant needs either the wrapper above or a checkpoint store that knows what a lease is.
Asking the strategy before each write instead is a check rather than a condition, so it is stale by the
time the write lands, which is the reason fencing exists at all, and it costs a round trip per event.
The one arrangement that would work is the lease and the checkpoint living in one store with the write
as a transaction over both, which rules out a MongoDB lease beside a Redis checkpoint and puts lease
knowledge in the storage regardless.

Rejected: **`CompetingConsumerSubscriptionModel` installing a source on the model it wraps.** It saves
the user one constructor argument and pays with a mutable setter on a public subscription model, a walk
down the delegate chain, and no way to reach the storage the catch-up models hold.

The cost is real and worth naming. Four models gain a constructor argument, a hand-wiring user passes
the source in two places rather than wrapping a storage in one, and the starter has to reach every
place that builds a checkpoint-writing model rather than one bean. That is more code than the wrapper.
What it buys is that no type pretends to be another one and every object states its own truth.

### The comparison rule, and what it does not promise

`notOlderThan(v)` is accepted when no version is stored, or when the stored version is not greater than
`v`. Nothing stored means a checkpoint written before any of this existed, so every shipped deployment
stays readable and there is nothing to migrate.

`any()` leaves the stored version alone, and that is why it is a case of the condition rather than a
missing argument. Unconditional writes stay alive in three places, which are a hand-wired user who did
pass a source, a node still on the previous release during a deploy, and the moment where the strategy
stands down. Both Mongo storages write a full replacement document today, so an unconditional
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
2. The retry loop still holds the position from the delivery that was refused, while the model asks
   its source for a version again on each attempt. If this node later takes the lease back at a higher
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
`returnDocument AFTER`, and an update pipeline whose `$cond` decides whether to write the new document.
`notOlderThan(v)` gates on the stored version, missing or not greater than `v`, and stamps `v` into the
returned document when it fires. `ifAbsent()` gates on whether a checkpoint is stored at all, not on a
version, so a successful write carries the previous version forward unchanged instead of stamping a new
one. Either condition failing yields `$$ROOT`, the document untouched.

For `notOlderThan`, the caller tells the outcomes apart by comparing the version on the returned document
to the one it offered, which is how ADR 114 made `acquireOrRefreshFor` tell its own two outcomes apart,
including its reason for matching on `_id` alone against the unique index rather than leaning on a
duplicate-key error. `ifAbsent` has no version to compare, since a successful write leaves the previous
one in place, so the caller instead compares the checkpoint value on the returned document to the one it
offered.

What gets written is `$mergeObjects` of the new document and the stored version, in that order, so no stale
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

**The same-value edge is storage-dependent.** Mongo's `ifAbsent` gates on presence, not value, so a
second write offering the exact same checkpoint value as the one already stored is indistinguishable from
a first write and is reported as success rather than refused, though nothing is stored twice
(`CheckpointWriteCondition.ifAbsent()` documents the edge). Redis is strict. The Lua script checks
`EXISTS` on the checkpoint key, so a second write to an existing key is refused whatever value it offers.
The one shipped caller, `ManualStartSubscriptionModel.pinStartPosition`, swallows the refusal either way
and is unaffected by which family it runs against.

The blocking in-memory storage implements the capability too, which is a few lines and gives the new
conformance suite something to run without a container.

### The Spring Boot starter supplies the source wherever it builds a model

The starter builds checkpoint-writing models in more places than the one that takes the storage bean.
`occurrentCompetingDurableSubscriptionModel` builds the durable model and the catch-up config, and the
projection and saga registrars each pull `CheckpointStorage` out of the application context and build
a `CatchupThenPushSubscriptionModel`. Each of those is a place the source has to reach, and there is no
single bean to wrap that reaches them all. `ManualStartSubscriptionModel.stoppedByDefault` writes a
checkpoint too, the start position it records on a subscription's first run, but that write only ever
uses `ifAbsent()` against a key with nothing stored yet, and `notOlderThan()` would accept the same
write there. A fence condition has nothing to add to that write, so the source does not need to reach
it.

**So the starter passes `strategy::fencingToken` at each of them, and the checkpoint storage bean stays
an ordinary checkpoint storage.** That is more sites than a wrapper would have needed and it is the
honest count, because those are the sites that write checkpoints. Missing one leaves that path writing
with `any()`, which is today's behaviour rather than a broken fence, and the end-to-end proof covers
the registrar-driven paths for that reason.

The strategy has to be reached without those beans depending on it eagerly, because the strategy
depends on `List<CompetingConsumerListener>`, which is an open extension point, so a user listener that
injects a subscription model would close a cycle. Each site takes an `ObjectProvider` and resolves on
first use, asked with `getIfUnique` rather than `getIfAvailable`, because an application with two
strategy beans starts today and must keep starting, which also means two strategy beans mean no fence.

Three rules on that lazy resolution, each of them a way the fence could otherwise be turned off
quietly. Only a strategy that was actually found is remembered, so a first attempt that finds nothing
is tried again rather than disabling the fence for the life of the process. The field holding it is
`volatile`, and resolving twice is harmless. And a resolution that fails rather than returns nothing
counts as no version, which keeps a checkpoint write from a registrar-driven subscription working while
the strategy bean is still being built.

A user who declares their own `CheckpointStorage` bean is unaffected, since that bean no longer carries
the fence. A user who hand-wires their own models passes the source themselves, which is one argument
in two places.

### The reactor stack gets the condition now, and a source when it has one to give

#665 names four storages. `ReactorCheckpointStorage` is one of them and it does get the conditional
write, on the same interface change as the blocking twin, but nothing supplies it a real condition yet.
There is no reactor competing consumer model and no reactor `CompetingConsumerStrategy`, and the only
bridge from a reactor model into a blocking one lives in the TCK, so no reactor checkpoint write is
made under a lease and `ReactorDurableSubscriptionModel` gets no source.

The interface changes on both stacks together because they are being broken once. Leaving the reactor
`CheckpointStorage` alone would mean a second breaking change, a second recipe and a second migration
guide section the day a reactor competing consumer model arrives, and it would leave the two stacks
saying different things about the same operation, which
[ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md) exists to
prevent. A refusal on that stack signals `Mono.error` rather than throwing.

### This breaks implementations, and that is the right price

Everything on the competing consumer side is additive. A default method on `CompetingConsumerStrategy`, a
new source interface, the Spring Boot starter's `CompetingConsumerCheckpointWriteVersionSource` that
adapts a strategy bean into that interface, a new exception, a new Redis key, and an internal method that
stops deleting a document all leave every call site as it was.

The checkpoint store is a real break, and deliberately. No calling code changes, since the
two-argument `save` remains as a default, but every implementation of `CheckpointStorage` and of its
reactor twin now implements the three-argument method and answers `writeVersion`. That earns an
`org.occurrent.UpgradeToOccurrent_*` recipe and a migration-guide section, which is what the convention
in AGENTS.md is for, and the changelog entry goes under Breaking changes.

A design that owes no migration is a pleasant result and a poor requirement. Any shape here can be made
to break nothing, by adding an interface beside the one that should have changed, and the cost lands on
everybody who later has to work out which of the two they need. The break is taken because the
alternative is a checkpoint store whose safety under concurrent writers is optional.

## Consequences

Checked against the hard rule first, the way AGENTS.md asks. **No events are lost**, because a refused
write means the delivery is not acknowledged and the new holder redelivers it. **No subscription is
blocked by another one being faulty**, and that is the one place this design could have broken the rule.
A refusal thrown on a delivery thread must not reach the lease refresh thread, which runs every consumer
on the node in one loop, so the requirements in the retry section that the subscription stays known and
pausable are what keeps one subscription's refused write from stopping every other subscription's lease
refresh. The end-to-end proof asserts it rather than trusting it.


The lock collection now keeps one small document per subscription id ever used, where it used to delete
them. That document is the subscription's lease record, and its `version` is the fencing token.

There is no two-release order to get right, since the lease change and the fence ship together in
0.33.0. The requirement the migration guide states instead is completing the 0.32.0-to-0.33.0 deploy.
Until every node runs 0.33.0, a cluster in that window has an old node still deleting the lock document
while a new node writes fenced checkpoints, so a subscription is refused, stops, releases, is taken over
at a token still below the stored one, and repeats, once per unit of the stored version, each cycle
costing a lease period and one re-run of the user's action. It recovers on its own once the deploy
completes, and `CheckpointStorage.delete(subscriptionId)` ends it immediately, at the price of a replay.
The migration guide documents the window.

A user who wires their own subscription models gets the fence by passing the source and gets today's
behaviour by leaving it out. A user on the Spring Boot starter gets it without doing anything, whatever
their checkpoint storage bean is, because the fence no longer travels through that bean.

A `CheckpointStorage` implementation outside this repository has to be updated, which the recipe does
for the signature and the author does for the behaviour. A store that cannot write conditionally stays
usable by refusing anything but `any()`. The conformance suite is what says whether one that claims to
did it correctly, including the case where `any()` must leave a stored version alone.

Redis Cluster is not supported for conditional checkpoint writes, and asking for one there fails at the
first write rather than quietly. A cluster user who supplies no source is unaffected.

This is the first place in Occurrent where a subscription refuses a checkpoint write made on behalf of
application code. The exception is deliberately not retried, which is the opposite of how every other
failure on the delivery path is treated, and the reason is written into the retry predicates rather than
left to a reader of the stack trace.

The implementation registers as its own epic against the 0.33.0 release above, split into the
lease change, the strategy's token, the checkpoint store's write condition, the four models taking a
source, the Mongo storages, the retry exclusion in both subscription models, the Redis storage, the starter wiring, an end-to-end proof over a real MongoDB
covering both an expired lease and a graceful handover, and the documentation.
