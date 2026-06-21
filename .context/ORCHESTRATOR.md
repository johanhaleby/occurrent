# Occurrent Orchestrator Memory

Last updated: 2026-05-22

## Current State

Phase 1 initial exploration is complete and ready for user confirmation. This file did not exist at session start, so the first repository map was built from code-review-graph, Maven metadata, representative source reads, ADRs, CI config, `.context/lessons.md`, recent Git history, and read-only subagent exploration.

No production or test code has been changed during exploration.

One read-only conventions/history subagent timed out and was closed; its scope was covered by local history, ADRs, tests, and `.context/lessons.md`.

## Architecture Summary

Occurrent is a Maven multi-module JVM event-sourcing library centered on CloudEvents. It is designed as small composable libraries rather than a framework: domain models should stay independent of Occurrent; Occurrent stores CloudEvents and provides application-service, event-store, subscription, DSL, deadline, and Spring Boot wiring helpers.

Root modules from `pom.xml`:

- `test-support`: shared test/domain helpers.
- `eventstore`: event-store APIs and implementations.
- `subscription`: subscription APIs and implementations.
- `cloudevents-extension`: Occurrent CloudEvent stream metadata extensions.
- `common`: shared condition/filter/time/retry/Mongo utility modules.
- `application`: application service, command composition, CloudEvent conversion, type mapping.
- `dsl`: query, subscription, module, decider, Arrow decider, and view DSLs.
- `framework`: Spring Boot MongoDB starter and annotation support.
- `deadline`: deadline scheduling API plus in-memory and JobRunr implementations.
- `library`: higher-level libraries, currently `hederlig`.
- `bom`: published dependency-management BOM.
- `example`: enabled by the active-by-default `examples-module` profile.

Main layering:

1. `common` and `cloudevents-extension` provide reusable primitives: `Condition`, `Filter`, retry/time utilities, Mongo filter/sort conversion, and Occurrent stream metadata (`streamid`, `streamversion`).
2. `eventstore` owns persistence contracts and implementations. Blocking `EventStore` composes `ReadEventStream`, conditional/unconditional writes, and existence checks. Wider capabilities are split into optional interfaces such as `EventStoreQueries`, `EventStoreOperations`, and `ReadEventStreamWithFilter`.
3. `application` adapts domain events to CloudEvents through `CloudEventConverter`/`CloudEventTypeMapper` and orchestrates domain-command execution through `ApplicationService` / `GenericApplicationService`.
4. `subscription` reacts to event streams and builds read models/projections/sagas. It has blocking/reactor APIs, Mongo/native/Spring/Redis/in-memory adapters, and utility wrappers for durable, catchup, and competing-consumer behavior.
5. `dsl` provides convenience layers over the core APIs for Kotlin/Java users.
6. `framework/spring-boot-starter-mongodb` auto-configures the common blocking Mongo stack.

## Module Boundaries

`eventstore`:
- `eventstore/api/common`: shared event-store model: `WriteCondition`, `WriteResult`, `StreamReadFilter`, `SortBy`, validators/mappers.
- `eventstore/api/blocking`: blocking read/write/query/operation interfaces.
- `eventstore/api/reactor`: Reactor equivalents.
- `eventstore/inmemory`: in-memory implementation, mainly tests/demos, also supports operations and filtered reads.
- `eventstore/mongodb/common`: Mongo document mapping, exception translation, shared support.
- `eventstore/mongodb/native`: native MongoDB driver implementation.
- `eventstore/mongodb/spring/blocking`: Spring `MongoTemplate` blocking implementation.
- `eventstore/mongodb/spring/reactor`: reactive Spring Mongo implementation.

`application`:
- `application/service/blocking`: `ApplicationService`, `GenericApplicationService`, `ExecuteOptions`, `ExecuteFilter`.
- `application/command-composition`: function/list/stream/sequence command composition helpers.
- `application/cloudevent-converter`: API plus Jackson 2, Jackson 3, XStream implementations and Kotlin extensions.
- `application/cloudevent-type-mapper`: reflection/custom CloudEvent type mapping.

`subscription`:
- `subscription/api`: blocking/reactor subscription contracts.
- `subscription/core`: shared subscription positions, filters, start positions, timeout helpers.
- `subscription/inmemory`: in-memory subscription model.
- `subscription/mongodb`: common Mongo subscription support plus native/Spring implementations and position storage.
- `subscription/redis`: Redis-backed Spring position storage.
- `subscription/util`: durable, catchup, competing-consumer, predicate, and reactor wrappers.

`framework/spring-boot-starter-mongodb`:
- Wires `MongoTransactionManager`, `SpringMongoEventStore`, Mongo position storage, lease competing-consumer strategy, `SpringMongoSubscriptionModel -> DurableSubscriptionModel -> CatchupSubscriptionModel -> CompetingConsumerSubscriptionModel`, query/subscription DSLs, and `GenericApplicationService`.
- Imports Jackson 3 converter configuration and provides fallback type mapper/converter behavior.

ADRs live in `doc/architecture/decisions`, not `doc/adr`.

## Primary Execution Flows

Application command flow:

1. Caller invokes `ApplicationService.execute(streamId, ExecuteOptions, domainFunction)`.
2. `GenericApplicationService` resolves optional `ExecuteFilter` through the configured `CloudEventConverter`.
3. If filtered reads are requested, the underlying event store must implement `ReadEventStreamWithFilter`; otherwise execution fails fast before invoking the domain function.
4. It reads the stream, converts CloudEvents to domain events, invokes the pure domain function, converts resulting domain events back to CloudEvents, writes with expected stream version, then runs optional side effects after the write.

Event-store Mongo blocking write/read flow:

1. `SpringMongoEventStore.write` validates `WriteCondition`.
2. For `anyStreamVersion`, it materializes the stream so retries can re-use events.
3. Inside a Mongo transaction, it reads current stream version, validates the write condition, maps CloudEvents to Mongo documents with stream metadata, inserts all documents, translates Mongo duplicate/write exceptions, and returns old/new versions.
4. Reads pin to current stream version to avoid read skew, optionally validate/map `StreamReadFilter` to `Filter`, query with configured read options, and map documents back to CloudEvents.

Subscription flow:

1. `Subscribable.subscribe(subscriptionId, filter, startAt, action)` is the blocking entry point.
2. Mongo subscription implementations use change streams and lifecycle-managed listener containers.
3. `DurableSubscriptionModel` persists positions after successful action execution.
4. `CatchupSubscriptionModel` delegates by default unless a stored position exists; time-based starts trigger historical query catchup before switching to the wrapped subscription model.
5. Spring Boot defaults wrap the Mongo subscription in durable, catchup, and competing-consumer layers.

## Conventions And Patterns

General:
- Java 17 baseline; Kotlin JVM target follows Java 17.
- Java and Kotlin coexist in most modules. Root Maven build-helper adds `src/main/kotlin` and `src/test/kotlin`.
- Public APIs are small capability interfaces composed together rather than large monoliths.
- Nullness uses JSpecify in newer APIs (`@NullMarked`, `@Nullable`) but not uniformly across old code.
- Public APIs usually validate nulls/invalid arguments eagerly with `Objects.requireNonNull` or `IllegalArgumentException`.
- Apache 2 license headers are common in source files.
- Static factories/builders are preferred for fluent public APIs.
- Changelog (`changelog.md`): unreleased changes go under the heading `### Changelog next version`, NOT under a versioned `### X.Y.Z (date)` section. A version number and date are assigned only at release time, when the maintainer renames that heading. Never invent a version/date for pending work.

Kotlin API conventions:
- Avoid Kotlin extension names that collide with Java members. ADR 0012 says collection-based Kotlin `ApplicationService` helpers use explicit names like `executeSequence` and `executeList`.
- Typed execute filters for Kotlin are namespaced under `ExecuteFilters` per ADR 0013.
- When changing Kotlin wrappers around Java generics, run at least `test-compile` on affected modules. `.context/lessons.md` notes Kotlin type inference can fail even when Java compiles.
- Prefer keeping Java API contracts honest (`? extends E`) and add localized Kotlin bridge casts/comments only where needed.

Testing:
- JUnit 5 is the main test framework; AssertJ is the dominant assertion style.
- jqwik is used for a small set of property tests.
- Awaitility is widely used for async/subscription/deadline tests.
- Tests include both unit and integration-style tests under Surefire; there is no Failsafe split.
- Docker/Testcontainers-backed tests are common, especially MongoDB and Redis paths.
- Some integration tests bind MongoDB to host port `27017`, which can collide with local services or concurrent runs.

## External Dependencies And Wrapping

Root managed versions include:

- CloudEvents `4.0.1`
- Kotlin `2.3.10`
- Jackson 2 `2.19.2`
- Jackson 3 `3.0.4`
- Reactor BOM `2024.0.10`
- Spring Boot `4.0.4`
- MongoDB driver BOM `5.6.1`
- Testcontainers `2.0.5`
- JUnit `5.11.3`
- JobRunr `8.1.0`
- Spring Retry `2.0.12`
- AssertJ, Mockito, Awaitility, jqwik, Logback, Arrow, Vavr, JSpecify

MongoDB is wrapped through native-driver and Spring `MongoTemplate` implementations. Spring Boot starter provides auto-configuration around the Spring implementation.

CloudEvents are the storage boundary. Domain event serialization/deserialization is explicitly owned by `CloudEventConverter` implementations; callers are generally responsible for CloudEvent data handling.

## Known Fragile Areas And Risks

- Docker/Testcontainers environment is the biggest verification risk. `colima.md` and `nordvpn.md` document Ryuk/Mongo timeout/network issues.
- Mongo tests using fixed `27017:27017` bindings can conflict with local MongoDB or concurrent test runs.
- Async subscription tests are timing-sensitive and use Awaitility/rerunner-style patterns.
- Parallel writes with `anyStreamVersion` are subtle. Mongo implementations materialize streams and retry to avoid false `WriteConditionNotFulfilledException`.
- Filtered stream reads are a recent and sensitive API area. `StreamReadFilter` excludes stream id/version from public convenience methods but still validates string-based `attribute`/`extension` names.
- `GenericApplicationService` must fail clearly when filtered reads are requested on event stores that do not implement `ReadEventStreamWithFilter`.
- Spring Boot/Jackson 3 fallback converter/type mapper behavior was recently changed and fixed (`Make starter auto-configure Jackson 3 only`, `Make default converter beans fall back`, `Fix lazy fallback converter for 0.20.3`).
- Kotlin extension/generic API ergonomics have a history of regressions. Recent fixes include execute extension usage and Kotlin name collision ADRs.
- Historical bug-fix themes from Git history include competing consumer locks/reacquisition, catchup subscription position handling, in-memory concurrent modification during query/write, retry for `any` writes, EventStoreQueries sorting defaults, and annotation processing start behavior.
- Catch-up to live handover invariant (ADR 0014): `CatchupSubscriptionModel` reconciles events written during the bulk replay via the delta query using `SortBy.natural(DESCENDING)` + `limit` (insertion order), NOT the time-based `catchupPhaseSortBy`. Selecting by time is loss-prone under clock skew (a during-replay event with a backdated `time` sorts before the boundary and is missed by both the delta and the live resume). Do not "tidy" the delta sort back to `catchupPhaseSortBy`. The global position is still captured AFTER the bulk replay (fresh token, avoids oplog ageing); the bulk replay keeps `catchupPhaseSortBy` and its time index. `SortBy.natural` in `InMemoryEventStore` now means GLOBAL insertion order (a write-time `insertionSequence`/`insertionOrderByEventKey`), matching MongoDB `$natural`; `query`'s natural handling is unified through the instance `toComparator`. The count-to-read window (an event written between the post-replay `count` and the `$natural` read shifts the newest-N window and pushes the oldest during-replay event out, below the live resume position) is CLOSED by PR #208: the delta re-reads the recent tail until the matching count stops growing, and `runCatchupForStream` filters already-cached ids so the overlapping re-reads stay at-least-once. `runCatchupForStream` now also closes its source stream via try-with-resources so a short-circuited bulk replay does not leak the Mongo cursor. Remaining residuals tracked for the position-based catch-up (PR4 / ADR 0008): the catch-up delta's net-count arithmetic loses events if events are deleted during the replay, and `count(Filter.All)` on the Mongo store returns `estimatedDocumentCount` (the count a beginning-of-time, no-filter rebuild uses), which can undercount after an unclean shutdown and terminate the re-read loop early. Both need a global monotonic position (ADR 0008 for streams, `dcbposition` for DCB) to close fully. The InMemory lazy-stream CME residual was fixed separately (#200).
- code-review-graph exists but is structurally weak for architecture boundaries in this repo: it reports many file-based communities, no cross-community edges, and test-dominated flows. Use it first for inventory/impact as requested, but verify module boundaries from Maven/source.

## Build And Verification

Required shell workflow:
- Use `rtk <command>` whenever `rtk` supports the command.
- Before raw shell commands, explicitly consider whether `rtk` supports the command. Use raw commands only when unsupported or proven unsuitable.

CI:
- `.github/workflows/maven.yml` runs `mvn -B package --file pom.xml` on Temurin Java 17 and 21.
- CI enables Testcontainers reuse by writing `$HOME/.testcontainers.properties`.
- Qodana runs separately with `jetbrains/qodana-jvm-community:2025.2`, project JDK 17.

Local focused Maven patterns:
- Full package: `rtk mvn -B package --file pom.xml`
- Focused module test: `rtk mvn -pl <module-path> -am test`
- Focused module package: `rtk mvn -pl <module-path> -am package`
- Focused test class: `rtk mvn -pl <module-path> -am -Dtest=<TestClass> test`
- Kotlin/generic API changes should usually run at least `rtk mvn -pl <module-path> -am test-compile` plus focused tests.

Release scripts:
- `mvn_local_snapshot.sh` runs release-profile local install with `-Drevision=...` and optional skip tests.
- `mvn_release.sh` requires Java 17, uses `mvn deploy -Prelease`, GPG signing, source/javadoc jars, Sonatype Central publishing, and tags `occurrent-<version>`.

## Orchestrator Operating Notes

- At every session start, read this file first.
- For non-trivial tasks, stay read-only while planning, then route work:
  - Bucket A: delegate substantial work (10+ minutes or 2+ files) to native Codex subagents when available.
  - Bucket B: handle trivial one-line/single-file work inline and note it here.
  - Bucket C: ask the user one strategic question when only they can decide.
- Before delegating, re-read relevant sections here and use code-review-graph first when available (`get_minimal_context_tool`, impact radius, relationship queries).
- Delegated briefs must include goal, owned files, forbidden files, copied conventions, exact `rtk` verification commands, out-of-scope, blocked protocol, and concurrency warning.
- After delegated work completes, review scope and actual verification, update this file, and incrementally update code-review-graph if meaningful code changed.
- After any correction from the user, update `.context/lessons.md` with the pattern to prevent recurrence.

## Decisions Made

- Treat Maven module boundaries and source ownership as definitive architecture boundaries; use code-review-graph as supporting inventory/impact tooling because its current graph is file-community heavy.
- Initial durable memory should be written before user confirmation because future compaction/session recovery depends on it.
- No code changes are allowed during Phase 1 exploration except this durable orchestration memory file.
