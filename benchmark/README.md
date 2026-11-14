# occurrent-benchmark

JMH benchmarks that make previously ad-hoc, uncommitted measurements re-runnable:

- `BlockingHandoverThroughputBenchmark`: the throughput comparison behind
  [ADR 108](../doc/architecture/decisions/0108-a-live-push-handler-runs-outside-the-handover-lock.md), the handover
  lock change.
- `PayloadFilterReadBenchmark`: the per-leaf payload read cost behind [PR #615](https://github.com/johanhaleby/occurrent/pull/615)
  and the go/no-go question for [#623](https://github.com/johanhaleby/occurrent/issues/623) (memoizing payload-field
  reads across filter condition leaves).
- `ReactorProjectionHandoffBenchmark`: the per-event `Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())`
  hand-off cost the reactor projection DSL's fold pays at
  [#639](https://github.com/johanhaleby/occurrent/issues/639), wrapped against unwrapped, for both wrapping sites.

- `DcbTypeStripeSettingBenchmark` and `DcbTypeMarkerCostBenchmark`: the native store's DCB appends and reads with the
  type marker stripes of [ADR 162](../doc/architecture/decisions/0162-a-dcb-type-marker-is-split-into-stripes.md). See
  [DCB type marker stripes](#dcb-type-marker-stripes).

The two DCB benchmarks start a MongoDB container, so they need Docker. Every other benchmark here runs in-process and
needs no Docker, no MongoDB and no other container.

## Why this module is not part of a normal build

This module is dev-only: it is never published, and it never even enters a plain `mvn install` or `mvn -Prelease
install` reactor. It is registered under an opt-in `benchmarks` profile in the root `pom.xml` (unlike the
`examples-module` profile, this one is not `activeByDefault`), and it additionally sets `maven.deploy.skip` in its
own `pom.xml` as a second, independent guard in case `-Pbenchmarks` and `-Prelease` are ever combined.

## Building

```
mvn -Pbenchmarks -pl benchmark -am test-compile
```

## Running

Build the self-contained benchmarks jar, then run it directly with the JMH CLI:

```
mvn -Pbenchmarks -pl benchmark -am package -DskipTests
java -jar benchmark/target/benchmarks.jar BlockingHandoverThroughputBenchmark -wi 3 -i 5 -f 1
java -jar benchmark/target/benchmarks.jar PayloadFilterReadBenchmark -wi 3 -i 5 -f 1
java -jar benchmark/target/benchmarks.jar ReactorProjectionHandoffBenchmark -wi 3 -i 5 -f 1
```

`-wi 3 -i 5 -f 1` is the exact protocol ADR 108's table states. It is also a reasonable default for
`PayloadFilterReadBenchmark`, though with 3 leaf counts x 2 payload sizes x 2 field positions x 2 backings x 2
benchmark methods = 48 forked-parameter combinations, a full run takes several minutes.
`ReactorProjectionHandoffBenchmark` has 2 wrapping sites x wrapped/unwrapped x 4 thread counts x 2 work sizes = 32
forked-parameter combinations, so a full run also takes several tens of minutes.

For a fast smoke run that only proves the harness executes end to end, shrink warmup/measurement:

```
java -jar benchmark/target/benchmarks.jar BlockingHandoverThroughputBenchmark -wi 1 -i 1 -f 1 -w 200ms -r 200ms
java -jar benchmark/target/benchmarks.jar PayloadFilterReadBenchmark -wi 1 -i 1 -f 1 -w 200ms -r 200ms
java -jar benchmark/target/benchmarks.jar ReactorProjectionHandoffBenchmark -wi 1 -i 1 -f 1 -w 200ms -r 200ms
```

Numbers from a smoke run are not meaningful for citing anywhere; only numbers from the full protocol above should be
recorded against an ADR, a changelog entry, or an issue.

## DCB type marker stripes

Both classes run the native `MongoEventStore` on a single-node replica set that `ReplicaSetReadyMongoDBContainer`
starts, one container per fork, with the Mongo version the rest of the build tests against. Docker has to be
reachable. On colima, for example:

```
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
unset TESTCONTAINERS_HOST_OVERRIDE
mvn -B -Pbenchmarks -pl benchmark -am package -DskipTests
java -jar benchmark/target/benchmarks.jar DcbTypeStripeSettingBenchmark -wi 3 -i 5 -f 1
java -jar benchmark/target/benchmarks.jar DcbTypeMarkerCostBenchmark -wi 3 -i 5 -f 1
```

Each iteration is JMH's default 10 seconds, so a single iteration holds hundreds to thousands of appends.
`DcbTypeStripeSettingBenchmark` takes about 9 minutes and `DcbTypeMarkerCostBenchmark` about 8. A database
benchmark is sensitive to whatever else the machine runs, so record `uptime` before and after each run and compare
runs made under a similar load.

A tagged conditional append appends an event of type `TicketSold` with a tag no other event has, on the condition
`failIfEventsMatch(tags(tag))`. A type-only conditional append reads `types("TicketSold")` for a consistency token,
newest event only, and appends a `TicketSold` event on `failIfEventsMatch(token)`.

`DcbTypeStripeSettingBenchmark` runs each benchmark with `spreadTypeMarkerWrites` off and on:

| Benchmark | Threads | What it appends |
|---|---|---|
| `oneType` | 16 | Tagged conditional appends of one type. |
| `typePerThread` | 16 | Tagged conditional appends of a type per thread. The setting changes nothing here, so the difference between its two results is noise. |
| `mixed` | 15 + 1 | 15 threads of `oneType` and one thread of type-only conditional appends of the same type. |

`DcbTypeMarkerCostBenchmark` uses no API that the stripes added, so it compiles unchanged on a branch from before
them. To compare a type's one marker with its eight stripes, copy `DcbTypeMarkerCostBenchmark` and the classes it
uses (`MongoDcbStore`, `OwnType`, `TaggedWriter`, `TypeOnlyWriter`) to that branch, add the
`occurrent-eventstore-mongodb-native` and `test-support` dependencies to its `pom.xml`, and run it there too.

| Benchmark | Threads | What it measures |
|---|---|---|
| `typeOnlyRead` | 1 | `read(types("TicketSold"))` over 1 and over 1000 events of the type. |
| `typeOnlyAppendOneType` | 16 | Type-only conditional appends of one type, which refuse each other. |
| `typeOnlyAppendTypePerThread` | 16 | Type-only conditional appends of a type per thread, which don't conflict on a type marker. |
| `taggedAppendOneType` | 16 | Tagged conditional appends of one type with the setting at its default, off. |

The score is attempts per second. The secondary results are rates per second too:

| Counter | Counts |
|---|---|
| `taggedConflicts` | Commands of a tagged append that MongoDB failed with a transient transaction error, in practice a write conflict. The driver or the store runs the transaction again after each one. |
| `typeOnlyAppended` | Type-only conditional appends that committed. |
| `typeOnlyRefused` | Type-only conditional appends refused with `DcbAppendConditionNotFulfilledException`, because an event of the type was appended after the read. |
| `typeOnlyConflicts` | As `taggedConflicts`, for the type-only appends. |

At the end of each fork the output prints how many transient transaction errors the client saw and how many were
counted against a benchmark thread. The two are equal when every error was counted.
