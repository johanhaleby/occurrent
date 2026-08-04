# 96. A test container owns its database

Date: 2026-08-04

## Status

Accepted. Closes #505.

## Context

171 test files started a `MongoDBContainer`, and around 110 of them built the connection string the same way:

```java
new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"))
```

`getReplicaSetUrl()` names the database `test`, and a connection string splits its path on the first dot, so that url
means database `test`, collection `events`. All 110 classes shared one database and separated themselves by 26 distinct
collection names. Nothing in the repository ever named a database.

The flush between tests emptied that whole database. Within one Maven run that is fine, because the classes take turns. It stops being fine the moment two runs overlap on one machine, which happens whenever a second agent
session or a second terminal builds at the same time: both attach to the same reused container, both flush, and each one
deletes data the other just committed. 69 of the classes also pinned the host port to `27017:27017`, which guaranteed
they would find each other, and one class had already been moved to `27018` to buy itself some parallelism.

The failure does not look like contention. A write returns successfully and the very next read sees nothing, or a stream
holds five of the six events just appended, which reads exactly like the event store losing a committed write. It cost
real debugging time more than once, and `.context/ORCHESTRATOR.md` had to carry a standing warning to reproduce any
suspicious MongoDB failure in isolation before believing it. There is a second symptom that was never named: dropping a
database invalidates every live change stream on it, so one run's flush could silently kill another run's subscription,
which surfaces as a subscription that receives nothing.

The issue proposed a database per test class. On its own that does not fix the reported problem, because two concurrent
runs execute the same classes and would collide on the same per-class name. What separates two runs has to be something
only one of them can hold.

## Decision

`ReplicaSetReadyMongoDBContainer` prefixes every database name with a scope that belongs to one container object in one
JVM, and it is now the only way a test gets a Mongo container.

```java
private static final AtomicInteger CONTAINER_COUNT = new AtomicInteger();
private final String databaseScope = "oc" + ProcessHandle.current().pid() + "_" + CONTAINER_COUNT.incrementAndGet();

@Override
public String getReplicaSetUrl(String databaseName) {
    return super.getReplicaSetUrl(scopedDatabaseName(databaseScope, databaseName));
}
```

The **process id** separates two concurrent runs. They are distinct live processes, so their pids cannot collide. The
**counter** separates container objects inside one JVM, which in practice is one database per test class, since the
dominant shape is a single `@Container static` field per class.

Only the one-argument overload is overridden, because the no-argument one is a virtual self-call to it. That covers all
172 call sites, including the 8 that pass an explicit literal name. Those mattered: two concurrent runs of
`ProjectionAnnotationDurableResumeMongoTest` both used the database `projection-durable-resume`, so naming a database by
hand had exactly the same collision as leaving it as `test`.

Overriding at that seam is what keeps the change small. `mongodb://host:port/oc4711_3_test.events` still parses as
database `oc4711_3_test` and collection `events`, so every hand-built url keeps working and the 17 files that read
`connectionString.getCollection()` back out are untouched. Spring Boot's `MongoDbContainerConnectionDetailsFactory` is
constructed with `super(MongoDBContainer::getReplicaSetUrl)`, so every `@ServiceConnection` container bean is scoped
without an edit. The scoped name rejects a dot and asserts MongoDB's 63 byte limit, because a dot would quietly move
part of the name into the collection and the length has no check in the driver.

### The fixed host ports are gone

All 69 bindings are deleted, including the `27018` workaround. Four comments in the tree justified pinning by claiming
`getReplicaSetUrl()` "reports the replica set member's own localhost:27017 address". That is not what it does: it goes
through `getConnectionString()`, which is `String.format("mongodb://%s:%d", getHost(), getMappedPort(27017))`. The 55
`@ServiceConnection` classes that already ran on dynamic ports were the standing proof. Those comments are corrected
rather than deleted, because left alone they would talk the next person out of this.

One class keeps a MongoDB to itself, by a means other than a port. `SpringMongoEventStoreDcbConcurrencyTest` drives 50
concurrent transactions per iteration and spends most of its class-level `@Timeout(180)` doing it, and the `27018`
binding was what kept a neighbouring suite's load off its server. Deleting the binding made it fail, so it now carries a
label nothing else sets. Testcontainers derives its reuse key from the whole container definition, so a unique label is a
server of its own. A database of its own, which every class now has, keeps the data apart but not the CPU.

Five example tests read their url from configuration rather than from the container, so they genuinely needed the fixed
port. They get `@DynamicPropertySource` instead of `@ServiceConnection`: none of the five poms has
`spring-boot-testcontainers`, and the forwarder's application reads the url as a property and appends `.events` itself,
which a `MongoConnectionDetails` bean never populates.

Removing the bindings also exposed a property name that had quietly stopped working. Spring Boot 4.1 deprecates
`spring.data.mongodb.uri` at *error* level in favour of `spring.mongodb.uri`, so the old name is not bound at all. Four
example test `application.yaml` files and the nine restart-pattern tests' `--spring.data.mongodb.uri=` boot arguments
were therefore all ignored, and every one of those applications was reaching MongoDB through Boot's default
`mongodb://localhost:27017/test` instead. The fixed port made that indistinguishable from working. Every test-side use is
renamed to `spring.mongodb.uri`; the same stale name in four example *main* configs is left for its own change, since a
`@ServiceConnection` test never reads it and only someone running those examples by hand is affected.

### Reuse stays at the call site

`withReuse(true)` is deliberately not centralised in the helper. The ~55 `@ServiceConnection` classes in the two
`spring-boot-starter-mongodb` modules boot a fresh container per Spring context and have no flush extension at all,
since neither pom depends on `test-support`. Turning reuse on for them would change a container lifecycle this change
has no reason to touch.

Removing the port bindings does not cost container sharing, because there was none to lose. Measured on
`eventstore/mongodb/spring/blocking`, 19 test classes: before this change, 19 containers created and 0 reuse hits; after,
19 created and 3 hits. `GenericContainer.stop()` in Testcontainers 2.0.5 hands the container to `ResourceReaper`
whatever `withReuse` says, so a `@Container static` field is removed when its class finishes either way. The fixed port
worked not because classes shared a server but because they take turns. So this trades one container per class for one
container per class, and the ~400MB-per-worktree objection to a reuse label was about a cost the tree was already
paying.

### The flush comes from the published module

`test-support`'s `FlushMongoDBExtension` is deleted and all 90 call sites move to `OccurrentMongoFlush` from
`occurrent-testing-mongodb` (ADR 95). Doing it in this change rather than after it is the cheaper order: 77 of those 90
lines carried the connection string and the flush construction on the same statement, so a separate pass would have
conflicted with this one on every single one of them.

It is not a like-for-like replacement, in two ways that matter.

It empties collections instead of dropping the database, which is the point of publishing it: dropping invalidates a live
change stream and destroys the event store's unique indexes, and a Spring context cached across test classes never
rebuilds them. Four tests genuinely need the dropping form, because they assert that a collection or an index does *not*
exist and emptying cannot express absence: `MongoEventStoreCapabilityTest`, `MongoEventStorePositionTest`, the two Spring
twins of those, and `ReactorMongoEventStorePositionTest`. The last one is worth naming because it phrases the assertion
as "no index satisfies this" rather than `doesNotContain`, so a grep for the obvious shape misses it and only running the
suite finds it.

It takes a `MongoDatabase` rather than a `ConnectionString`, deliberately, since a connection string is what let a test
name a collection where it meant to name a database. `MongoTestDatabase` in `test-support` is the bridge, and it keeps one
client per server for the JVM instead of the one per `beforeEach` that the deleted extension opened and closed.

The deleted extension also caught `Throwable`, retried ten times, and then returned as though it had worked, printing a
line to stdout. A flush that failed left the previous test's data in place and failed nothing, which is the same symptom
as the store losing a write. The published one throws, so that hazard leaves with the class rather than needing its own
fix.

### Cleanup does not ask whether another run is alive

At the first container start in a JVM, every database whose name begins with `oc<ourPid>_` is dropped. Nothing inspects
another process. `ProcessHandle.of(pid).isPresent()` lies when two runs sit in different pid namespaces, for instance a
build inside a devcontainer beside one on the host, and the direction it lies in is reporting a live run as absent and
dropping its data. Restricting the sweep to our own pid is self-limiting instead: a recycled pid cleans up whatever the
previous owner left.

That start-time sweep is the whole of the cleanup. A shutdown hook that dropped the same databases on the way out was
tried and removed: `stop()` already hands each container to `ResourceReaper` when its class finishes, so the databases
go with it, and a hook that opened a client per container seen delayed JVM exit enough for Surefire to report "going to
kill self fork JVM. The exit has elapsed 30 seconds" on the modules with the most test classes.

The readiness probe keeps a database of its own. `containerIsStarted` fires on a reuse hit as well as a fresh start, so a
probe pointed at the scoped database would drop the run's data from the second container onwards.

## Consequences

- Two Maven runs on one machine can overlap. That includes two runs in the same checkout, which is the case a container
  per checkout, or a Testcontainers reuse label, would not have fixed: two runs of the same class would land on the same
  labelled container. A label is still the right tool for keeping one heavy suite off a shared server, which is what
  `SpringMongoEventStoreDcbConcurrencyTest` uses it for.
- A local `mongod` on 27017 no longer blocks the suite, and a contributor without Testcontainers reuse enabled can run
  two builds at once.
- The flush reaches a database only this container owns, which is the precondition #483 named before a published flush
  could be considered. It is also narrower in a second way now, since the published one empties rather than drops, so it
  can no longer invalidate a change stream at all, concurrent or not.
- An IDE run no longer builds the image `mongo:null`. Surefire is what supplies `test.mongo.version`, and
  `withDefaultVersion()` falls back to the version the build filters into `occurrent-test-support.properties`.
- The design assumes test classes do not run concurrently inside one JVM. Nothing enables that today: there is no
  `junit-platform.properties`, no `forkCount`, `reuseForks` or `parallel` setting in any pom, and no `@Execution` or
  `@Isolated` annotation. Modules within a CI shard also stay sequential. Enabling JUnit parallelism later would need a
  per-class axis on top of the per-container one, and the comment in `maven.yml` that used to cite the fixed ports now
  cites this instead.
- A run creates one database per test class rather than reusing one. They normally go when the container does, and a
  leftover on a container that outlives the run is dropped by the next JVM that holds this process id.
- Two assertions had to stop hardcoding a namespace. `MongoEventStoreTest` asserted on a duplicate-key message quoting
  `test.events`, which now derives the namespace from the container. Assertions on synthetic exceptions built in unit
  tests keep their literals, since no server is involved.
- `.github/scripts/check-mongo-test-containers.sh` fails a build that reintroduces a `27017:27017` binding or an image
  name built from `test.mongo.version` under `src/test`. The pattern reached 171 files by being copied from the
  neighbouring test, so the guard is the part that keeps it from growing back.
