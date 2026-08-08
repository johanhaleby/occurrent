# occurrent-benchmark

JMH benchmarks that make two previously ad-hoc, uncommitted measurements re-runnable:

- `BlockingHandoverThroughputBenchmark`: the throughput comparison behind
  [ADR 108](../doc/architecture/decisions/0108-a-live-push-handler-runs-outside-the-handover-lock.md), the handover
  lock change.
- `PayloadFilterReadBenchmark`: the per-leaf payload read cost behind [PR #615](https://github.com/johanhaleby/occurrent/pull/615)
  and the go/no-go question for [#623](https://github.com/johanhaleby/occurrent/issues/623) (memoizing payload-field
  reads across filter condition leaves).
- `ReactorProjectionHandoffBenchmark`: the per-event `Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())`
  hand-off cost the reactor projection DSL's fold pays at
  [#639](https://github.com/johanhaleby/occurrent/issues/639), wrapped against unwrapped, for both wrapping sites.

Both benchmarks run entirely in-process. No Docker, no MongoDB, no other container is required.

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
