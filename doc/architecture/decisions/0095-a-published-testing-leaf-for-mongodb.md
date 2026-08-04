# 95. A published testing leaf for MongoDB, and a flush that deletes rather than drops

Date: 2026-08-04

## Status

Accepted. Resolves #483, which ADR 82 filed when it deferred this.

## Context

An application testing against MongoDB has to empty the database between tests, and getting it wrong fails quietly.
`test-support` has a `FlushMongoDBExtension` that drops the whole database, but it is unpublished, so an application
cannot depend on it and writes its own. The Parkster push-notification service did, and its version differs on exactly
the point that matters.

ADR 82 shipped `occurrent-testing-*` for application authors and left this open, on the grounds that a store-specific
leaf "would pull Testcontainers and the Mongo driver onto a new artifact's compile path, and that trade deserves its own
decision". **Half that cost does not exist.** A flush needs a database handle, not a container, so Testcontainers stays
at test scope. What ships is the driver, and an application testing MongoDB already has it.

## Decision

**A third leaf, `occurrent-testing-mongodb`.** Not inside `occurrent-testing-spring-boot`, which has no MongoDB
dependency today and whose reader may not use Spring. Its published dependencies are `mongodb-driver-sync`,
`junit-jupiter-api`, and `jspecify` as optional, verified in the flattened consumer POM.

**No Occurrent module on its compile path, which is a constraint rather than an accident.** Roughly 30 modules will
depend on this leaf at test scope once the in-repo call sites converge on it, so a compile dependency on an Occurrent
module is a reactor cycle waiting to happen. The composition below is expressed through `Runnable` for that reason.

**Deleting documents is the contract, and there are two reasons, not one.** The known one is that dropping a collection
or a database invalidates a live change stream. Both MongoDB subscription models watch a collection, so the resume token
dies with it, and stopping the model first does not help because it resumes from a position pointing into a collection
that no longer exists.

The second reason is not written down anywhere and is the worse of the two. **An Occurrent MongoDB event store creates
its unique indexes in its constructor**, so they come back only when a new store is constructed
(`SpringMongoEventStore.java:145` calling `initializeEventStore`, which creates them at `:831`, `:862` and `:875`). A
Spring test context is cached across test classes, so nothing constructs one again. After a drop, optimistic concurrency
and duplicate detection have no index behind them and their assertions pass for the rest of the run. A broken change
stream at least fails a test. This does not.

`droppingTheDatabaseIn` is still offered, because a test asserting that a collection or an index is *absent* cannot be
served by deleting documents, and this repository has such tests
(`MongoEventStoreCapabilityTest.java:140,147,148`). It carries the warning rather than being hidden, since hiding the
faster option invites somebody to reimplement it.

**The base case names no collections.** Occurrent writes to more than an application remembers: the events, a stream
position collection, a DCB checkpoint collection, the subscription checkpoint collection, and a competing consumer lock
collection. A hand-written list stops covering one the day a feature is switched on, silently, which is what the
Parkster version demonstrates. `everyCollectionIn(database)` enumerates instead, skipping views, which cannot be written
to, and `system.*`, which is not a test's to empty. Naming collections is the narrowing, not the default.

**It takes a `MongoDatabase`, not a `ConnectionString`.** This one is a lesson rather than a preference.
`AGENTS.md:127` exists because the connection-string form is a trap: 89 call sites in this repository append a
`.collection` suffix believing it selects a database, and it does not, because MongoDB forbids a dot in a database name.
Minting a permanent published API around that parameter would ship the trap to everyone. A `MongoDatabase` cannot be
ambiguous, and the caller supplies the client, so the extension no longer opens and closes one per test.

**No retry loop.** The old one existed because it built a cold client against a just-started container. With a
caller-supplied database the client is already connected and the driver has its own server-selection timeout, so a
ten-attempt loop only delays the report. A failure now throws, naming the database. The version in `test-support` gives
up silently after ten attempts and leaves the previous test's data in place, which is a latent bug in this repository's
own suite. Fixing it belongs to #505, which owns that file.

**Checkpoint clearing went to the store-neutral leaf, not here.** `CheckpointStorage.delete(String)` already exists
(`CheckpointStorage.java:66`) and `occurrent-testing-junit-jupiter` already depends on the module holding it, so
`OccurrentSubscriptionsExtension.clearingCheckpoints(CheckpointStorage)` needs no MongoDB and no collection name. ADR 82
says a capability expressible against the neutral module belongs there. Putting it here would have hardcoded
`"subscriptions"` into a published API, duplicating `OccurrentProperties.java:235` with nothing linking them, and left
an application keeping its checkpoints in Redis unserved for no reason.

**Ordering stops being the user's problem.** `clearingStateWith(Runnable)` runs after every subscription is stopped and
before any is resumed, so a flush composes into one `@RegisterExtension` instead of two ordered with `@Order`:

```java
OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel)
        .clearingStateWith(OccurrentMongoFlush.everyCollectionIn(mongoTemplate.getDb()))
        .clearingCheckpoints(checkpointStorage);
```

Checkpoints are cleared after the flush, so a flush that recreates the checkpoint collection cannot leave one behind.

## Consequences

**Two flush helpers exist until #505 lands**, one deleting and one dropping. Tolerable because no test in this
repository keeps a change stream open across a flush, so none is exposed to the difference, and because converging the
89 call sites means rewriting the same lines #505 is already rewriting: 77 of them carry the connection string and the
flush construction together. Doing it twice would mean 77 conflicts.

**`ReplicaSetReadyMongoDBContainer` does not travel with it**, and that is a decision rather than an omission. It needs
Testcontainers on the compile path, which is the cost this leaf avoids, and it reads a build-filtered
`occurrent-test-support.properties` for its Mongo version, so publishing it would ship this repository's test version as
a consumer-facing default. If it graduates it belongs in a leaf of its own.

**This module's own tests do not use an Occurrent subscription model.** The change-stream property is MongoDB's, so it is
asserted with the driver's `watch()` directly, which also keeps the compile path clear. Both directions are asserted: the
stream keeps delivering across a flush, and a drop produces `drop` then `invalidate`. The second is what makes the first
mean anything.

**It starts its own container rather than sharing the reused one on port 27017.** The deviation is deliberate: these
tests watch a change stream across a flush, so a container another test class drops out from under them fails for a
reason unrelated to the code. Testcontainers disables reuse in CI, so this costs nothing there. `testing/mongodb`
therefore moved out of the container-free `misc` CI shard into `mongodb-native`, and the `testing` prefix in `misc` had
to be split into its two store-neutral leaves. Leaving the bare prefix would have run the module twice, which
`verify-shard-coverage` cannot detect because it only fails on zero coverage.

**A new published artifact is permanent.** ADR 82 accepted that risk for a ten-line extension on the grounds that the
alternative observed in practice is every application author writing the same thing slightly differently. That is
literally what happened here, so the same reasoning applies without needing to be re-derived.
