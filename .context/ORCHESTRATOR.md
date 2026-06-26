# Occurrent Orchestrator Memory

Last updated: 2026-05-24

## Current State

Phase 1 initial exploration is complete and ready for user confirmation. This file did not exist at session start, so the first repository map was built from code-review-graph, Maven metadata, representative source reads, ADRs, CI config, `.context/lessons.md`, recent Git history, and read-only subagent exploration.

DCB support is currently implemented on branch `johan/dcb-support`.

Spring Mongo event-store capabilities have been added in the current worktree. The default remains stream-only (`{STREAM}`), while DCB is explicitly enabled through composable capabilities.

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
- Update `changelog.md` after every change that affects code behavior, public API, build/runtime behavior, or notable user-facing capability. Small documentation-only edits do not need changelog entries.

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
- DCB support deliberately shares the existing CloudEvent storage boundary instead of creating a parallel event model. DCB metadata is represented as CloudEvent extensions (`dcbtags`, `dcbposition`) so normal subscriptions and CloudEvent consumers can continue to see DCB-written events.
- Mongo DCB position reservation is backed by a separate position collection, but event documents are inserted into the existing event-store collection with normal Occurrent stream metadata. DCB appends are transactional and rely on `MongoTemplate` session synchronization being `ALWAYS`.
- Mongo DCB reads capture a high-watermark before querying and bound matching reads to `dcbposition > afterSequencePosition && dcbposition <= highWatermark`, then return that same high-watermark. This prevents callers from skipping matching events that commit between the query and high-watermark read.
- DCB append-condition concurrency is subtle. The Spring Mongo implementation first checks for actual matching events after the caller's position, then updates conservative checkpoint keys (`all`, `tag:<tag>`, and type-only keys) to detect racing appends.
- Spring Mongo capability mode is now part of `EventStoreConfig`: `STREAM`, `DCB`, or both. Capabilities control index/support-collection creation and runtime API guards, not the CloudEvent document format.
- Occurrent only creates missing Mongo indexes/collections for enabled capabilities; it never removes indexes. Operators should create newly required indexes out-of-band before enabling a capability on large production collections.
- DCB-only Mongo writes must still assign per-storage-stream Occurrent stream versions. Those versions are required if an operator later enables `STREAM` and reads DCB partition streams through the stream API.
- Spring Boot DCB-only auto-configuration must not expose stream `ApplicationService` or wrap subscriptions in `CatchupSubscriptionModel`, because those depend on stream APIs. It should still expose `DomainEventQueries` so opt-in DCB query helpers can reuse the configured converter; normal stream query methods remain guarded by the event-store capability.
- Spring Boot DCB application-service auto-configuration is registered with a `BeanFactoryPostProcessor`, not plain `@ConditionalOnBean(TagGenerator.class)`, because the latter can evaluate before user `@Bean` tag generators are visible in real Boot application contexts.
- In-memory `deleteAll()` must reset DCB sequence state as well as event state, otherwise an empty store can report stale DCB high-watermarks after deletion.

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
- DCB touched-module verification: `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking,application/service/blocking,subscription/mongodb/spring/blocking -am test`

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
- ADR 0014 accepts DCB as an explicit optional capability that shares CloudEvent storage with stream-based Occurrent. Separate `DcbEvent`/`SequencedDcbEvent` wrappers and separate DCB Mongo stores were rejected because they would create a parallel ecosystem for subscriptions and other CloudEvent consumers.
- DCB API surface added as `eventstore-api-dcb`: `DcbEventStore`, `DcbQuery`, `DcbQueryItem`, `DcbReadOptions`, `DcbCloudEvents`, `DcbEventStream`, `DcbAppendCondition`, `DcbAppendResult`, and `DcbAppendConditionNotFulfilledException`.
- DCB query semantics for v1: query items are OR-combined; within one item, type matching is any-of and tag matching is all-of. Read options and append conditions use exclusive `afterSequencePosition` (`sequencePosition > afterSequencePosition`).
- DCB query items now also support per-item excluded CloudEvent types. Within one item, included types are any-of, tags are all-of, and excluded types are none-of. Included/excluded type overlap is rejected, and excluded-only items remain invalid.
- DCB events store explicit tags only in the `dcbtags` CloudEvent extension. Matching must not inspect CloudEvent payload data for tags.
- DCB v1 implementations were added by having `InMemoryEventStore` and `SpringMongoEventStore` implement `DcbEventStore`. Existing stream writes/read APIs remain available side-by-side with DCB append/read.
- Blocking DCB application service added under `application/service/blocking/dcb`: `DcbApplicationService`, `GenericDcbApplicationService`, `TagGenerator`, `DcbStreamIdGenerator`, and `PartitionedDcbStreamIdGenerator`. It reads a DCB query, converts current CloudEvents to domain events, runs the domain function, tags new CloudEvents via `DcbCloudEvents.withTags`, and appends to a generated backing stream id with `failIfEventsMatch(query, lastSequencePosition)`.
- `$learning` was used with strategic checkpoints while implementing the API, in-memory store, Mongo store, and app service. No `TODO(human)` markers remain.
- ADR 0015 accepts composable Spring Mongo event-store capabilities instead of a combined mode enum. `EventStoreConfig` stores a non-empty `Set<SpringMongoEventStoreCapability>`, Spring Boot binds `occurrent.event-store.capabilities`, and the backward-compatible default is `{STREAM}`.

## DCB Work Completed

- ADR: `doc/architecture/decisions/0014-introduce-dcb-as-shared-cloudevent-capability.md`.
- API tests: `DcbApiTest`.
- In-memory tests: `InMemoryEventStoreDcbTest` covers type/tag reads, exclusive after-position semantics, append condition success/failure, empty append rejection, duplicate CloudEvent rejection, and no payload tag inspection.
- Spring Mongo tests: `SpringMongoEventStoreDcbTest` covers type/tag reads, append condition failure, duplicate CloudEvent behavior without position advancement for pre-validation failures, and no payload tag inspection.
- Spring Mongo tests also cover OR query item semantics with tag-all matching and rollback when insertion fails after reserving a position.
- Spring Mongo tests also cover stale same-query append conditions failing without advancing the next committed DCB position.
- Application service tests: `GenericDcbApplicationServiceTest` covers read-decide-append with generated tags, no-op domain functions, and retry from a fresh DCB read when an append condition detects a conflict.
- Subscription compatibility test: `SpringMongoSubscriptionModelTest#blocking_spring_subscription_calls_listener_for_dcb_written_event` confirms DCB appends written to shared Mongo storage are delivered as ordinary CloudEvents to the existing subscription model.
- Simplify pass completed after implementation. Only small cleanup was applied: decoded DCB tags now reuse canonical validation, the partitioned stream-id generator validates inputs, and one application-service collector expression was simplified.
- Public DCB API Javadoc review comments were addressed on 2026-05-23. Newly added DCB API/application-service files now use 2026 copyright headers and brief Javadoc explaining the purpose of DCB queries, tags, append conditions, CloudEvent metadata, storage stream id generation, and the DCB application service.
- Spring Mongo capabilities work completed on 2026-05-23:
  - Added `SpringMongoEventStoreCapability` with `STREAM` and `DCB`.
  - `EventStoreConfig.Builder` accepts a non-empty capability set or varargs; default is `{STREAM}`.
  - `SpringMongoEventStore` always creates the event collection and CloudEvent id/source unique index, creates stream or DCB indexes/support collections only when enabled, and fails fast when callers invoke a disabled API family.
  - DCB-only appends still write normal CloudEvents with Occurrent stream metadata; stream versions are always per-storage-stream so DCB partition streams remain readable if `STREAM` is enabled later.
  - Spring Boot property binding supports omitted/default `stream`, `dcb`, and `stream,dcb`, and auto-configured `EventStoreConfig` propagation is covered by tests.
  - ADR: `doc/architecture/decisions/0015-spring-mongo-event-store-capabilities.md`.
  - Tests added: `SpringMongoEventStoreCapabilityTest` plus Spring Boot auto-configuration characterization tests for capability binding/propagation.
  - Test-automator coverage review initially found gaps in guard coverage, auto-config propagation, and index option assertions; those gaps were fixed before final verification.
  - Branch review found and fixed three follow-up issues: DCB-only Mongo stream versions were made per-stream, DCB-only Spring Boot no longer auto-configures stream application helpers/catchup subscriptions, and in-memory `deleteAll()` now resets DCB sequence state.
  - Review-fix verification passed:
    - `rtk mvn -q -pl eventstore/mongodb/spring/blocking,eventstore/inmemory,framework/spring-boot-starter-mongodb -am -Dtest=SpringMongoEventStoreCapabilityTest,InMemoryEventStoreDcbTest,OccurrentMongoAutoConfigurationCharacterizationTest -Dsurefire.failIfNoSpecifiedTests=false test`
    - `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking,application/service/blocking,subscription/mongodb/spring/blocking -am test`
    - `rtk mvn -q -pl framework/spring-boot-starter-mongodb -am test`
- DCB `excludingTypes` support completed on 2026-05-23:
  - `DcbQueryItem` gained `excludedTypes` while keeping the two-argument constructor for source compatibility.
  - Added minimal factories for tag/tag+type queries with excluded types.
  - In-memory and Spring Mongo DCB matching now apply excluded types to reads and append-condition checks.
  - Spring Mongo checkpoint updates now skip checkpoint advancement when the events being appended do not match the append-condition query after exclusions.
  - ADR 0014 was updated with the refined query semantics.
  - Verification passed:
    - `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking -am -Dtest=DcbApiTest,InMemoryEventStoreDcbTest,SpringMongoEventStoreDcbTest -Dsurefire.failIfNoSpecifiedTests=false test`
    - `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking -am test`
    - `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking,application/service/blocking,subscription/mongodb/spring/blocking -am test`
- DCB DSL module completed on 2026-05-23:
  - Added opt-in blocking module `dsl/dcb-dsl/blocking` with artifact `dcb-dsl-blocking`.
- Added static Java helpers `DcbDomainEventQueries` and result type `DcbDomainEventStream` under `org.occurrent.dsl.dcb.blocking`. The API is intentionally smaller than `DomainEventQueries`: callers pass `DomainEventQueries<E>`, `DcbQuery`, and optional `DcbReadOptions`; the helper reuses the wrapped converter and verifies that the wrapped query implementation supports `DcbEventStore`. `queryWithPosition(...)` exposes the DCB high-watermark/last sequence position.
- Added Kotlin DCB query extensions on `DomainEventQueries<E>`: `queryForSequence`, `queryForList`, and `queryWithPosition`.
  - Added live DCB subscription extension `Subscribable.subscribeDcb(...)`. The helper subscribes broadly to CloudEvents and then requires `dcbposition > 0` plus exact `DcbQuery` matching in process. This avoids subscription-model-specific behavior for missing `dcbposition` and keeps the API honest: it is live CloudEvent delivery of DCB-tagged events, not a DCB-consistent read.
  - Shared DCB query matching was centralized in `DcbCloudEvents.matches(...)`; `DcbCloudEvents.getPosition(...)` now reads numeric/string DCB positions and returns `0` for non-DCB events.
  - In-memory and Spring Mongo DCB append/read internals now reuse the shared matcher, reducing semantic drift for types, tags, OR items, and excluded types.
  - ADR: `doc/architecture/decisions/0016-dcb-dsl-module.md`.
  - Test-automator coverage review found useful gaps around non-DCB event exclusion in DCB subscriptions and typed position overloads; those were fixed before final verification.
  - A follow-up simplify pass removed the stateful `DcbDomainEventQueries` wrapper shape and `DcbSubscriptions` scope object. The resulting public shape is static Java helpers, Kotlin extensions on `DomainEventQueries`/existing subscription types, and no reproduction of the existing stream query/subscription DSLs.
  - Review-agent finding fixed: `DcbCloudEvents.getPosition(...)` now returns `0` only when `dcbposition` is absent and throws when the extension is present with an unsupported type.
  - Verification passed:
    - `rtk mvn -q -pl eventstore/api/dcb,dsl/dcb-dsl/blocking -am test`
    - `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking,application/service/blocking,subscription/mongodb/spring/blocking,dsl/dcb-dsl/blocking -am test`
- DCB metadata integration completed on 2026-05-23:
  - `dcb-dsl-blocking` now reuses the existing subscription DSL `EventMetadata` for `subscribeDcb` metadata callbacks instead of exposing a separate `DcbEventMetadata` type.
  - The DCB DSL module adds Kotlin extension properties `EventMetadata.dcbPosition` and `EventMetadata.dcbTags`; missing DCB position is represented as `null`, while missing tags are an empty set.
  - `EventMetadata` construction is public so opt-in DSL modules can create the shared metadata type without duplicating it.
  - DCB-written events still carry Occurrent `streamid` and `streamversion`, including Spring Mongo DCB-only mode. DCB-only disables stream APIs/indexes but not the CloudEvent storage metadata.
  - ADR 0016 was updated with the metadata reuse decision.
  - Verification passed:
    - `rtk mvn -q -pl dsl/dcb-dsl/blocking -am test`
    - `rtk mvn -q -pl eventstore/mongodb/spring/blocking,dsl/dcb-dsl/blocking -am test`
- DCB word-guessing T13 end-to-end verification completed on 2026-05-23:
  - Added explicit DCB-only stream API rejection checks in both new example modules.
  - Strengthened end-to-end assertions so DCB-written gameplay/points events prove DCB tags, DCB positions, and Occurrent `streamid`/`streamversion` storage metadata exist.
  - Full two-module Spring/Testcontainers verification exposed a manual-module retry gap: live policy appends can race command appends and Mongo translates transient `WriteConflict` to `DataIntegrityViolationException`. Manual `StartGame` and `MakeGuess` now retry that exception, matching the autoconfig module.
  - Ergonomics notes were recorded in `.context/notes/dcb-word-guessing-ergonomics.md`: DCB decider helpers are good after T9, but tag/query/tag-generator boilerplate remains duplicated; live subscription tests need eventual assertions and subscriptions started before commands; annotation metadata is workable, with remaining broad type subscription plus in-handler tag filtering friction.
  - Verification passed: `rtk mvn -q -f example/domain/word-guessing-game/mongodb/spring/pom.xml -pl dcb,dcb-autoconfig -am test`.
- DCB word-guessing T14 final review completed on 2026-05-24:
  - code-review-graph change analysis flagged high branch risk due to the broad DCB/example diff, so final review focused on the required DCB DSL helper, manual example, autoconfig/annotation/decider example, DCB-only stream API rejection, and metadata assertion surfaces.
  - Coverage review found the required tests present: DCB decider helper unit tests, helper/converter tests in both modules, manual command/read integration tests, autoconfig decider/annotation integration tests, DCB-only stream API rejection assertions, and DCB tag/position plus `streamid`/`streamversion` metadata assertions.
  - Simplify pass over touched DCB DSL and example modules found no worthwhile behavior-preserving source simplification. The duplicated manual/autoconfig helper and policy code remains intentional example-local duplication.
  - Reviewer pass found no correctness/API/test/maintainability issues requiring code changes. The only documentation gap was a missing changelog note for the two new word-guessing examples; it was added.
- Spring Boot DCB application-service auto-configuration completed on 2026-05-24:
  - `occurrent.event-store.capabilities` now controls auto-configured application services as well as event-store infrastructure: `{STREAM}` creates the classic stream `ApplicationService`, `{DCB}` with a user `TagGenerator` creates `DcbApplicationService`, and `{STREAM, DCB}` with a user `TagGenerator` creates both.
  - `occurrent.application-service.enabled=false` disables both stream and DCB application services, and `enable-default-retry-strategy=false` switches both to `RetryStrategy.none()`.
  - The starter does not auto-create `TagGenerator`; DCB tags remain domain-specific.
  - The word-guessing DCB autoconfig example now relies on starter-created `occurrentDcbApplicationService` instead of a local manual bean.
  - Verification passed:
    - `rtk mvn -q -pl framework/spring-boot-starter-mongodb -am test`
    - `rtk mvn -q -f example/domain/word-guessing-game/mongodb/spring/pom.xml -pl dcb-autoconfig -am test`
- DCB word-guessing autoconfig converter cleanup completed on 2026-05-24:
  - Removed the example-local `GameCloudEventConverter` and its converter-specific test.
  - `Bootstrap` now exposes a `CloudEventTypeMapper<GameEvent>` using `ReflectionCloudEventTypeMapper.simple(GameEvent::class.java)` and a built-in Jackson 3 `CloudEventConverter` configured with domain source, subject, and millisecond-truncated event timestamps.
  - Verification passed: `rtk mvn -q -f example/domain/word-guessing-game/mongodb/spring/pom.xml -pl dcb-autoconfig -am test`.
- DCB catch-up wiring in progress on branch `johan/dcb-catchup-wiring` (parent `johan/dcb-review-followups-2`, inserted below `johan/dcb-example-course-enrollment`):
  - `OccurrentMongoAutoConfiguration.occurrentCompetingDurableSubscriptionModel` now selects catch-up mode by capability. STREAM present keeps stream catch-up. DCB-only wraps the durable model in the DCB-mode `CatchupSubscriptionModel(durable, dcbEventStore, DcbQuery.all(), config)`, with `DcbEventStore` taken through an `ObjectProvider`. The shared model uses `DcbQuery.all()` and each `DcbSubscriptions` subscription narrows by its own query in the consumer, so the stored position is the global `dcbposition` and stays unambiguous across subscriptions. ADR `0022-wire-dcb-catch-up-in-dcb-only-mode.md`.
  - Callers request replay-from-start with `StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0))`. In-memory always-replay views pass that on every boot.

## Deferred Follow-ups (DCB)

- `@DcbSubscription` annotation: the DCB analog of `@Subscription`, declarative DCB subscriptions with catch-up and resume options, routing through `DcbSubscriptions` and `DcbQuery` instead of the stream `Subscriptions` DSL and `Filter`. The catch-up ergonomics make it clearly warranted (today callers pass `DcbSubscriptionPosition.of(0)` by hand). The user asked for this to be built later. Own stacked PR plus ADR.
- Dual-mode catch-up for STREAM-and-DCB apps: one subscription model serving both stream time-based and DCB position-based catch-up. Today STREAM-and-DCB keeps only stream catch-up. A single `CatchupSubscriptionModel` instance is stream XOR DCB, so this needs a dual-mode model and per-subscription mode selection.
- `DcbSubscriptions` cancel/unsubscribe ergonomics: `DcbSubscriptions` exposes no cancel, so per-connection cancellation (for example the SSE activity feed in the planned web example) goes through `SubscriptionModel.cancelSubscription(id)` directly.
- Web page for the course-enrollment example (Part 2 of the current plan): built on the example branch after the catch-up wiring lands and the stack is rebased. Materialized View dashboard fed by a DCB subscription at `DcbSubscriptionPosition.of(0)`, consistent course-detail read via `DcbDomainEventQueries`, tag-scoped SSE activity feed, Thymeleaf plus HTMX via webjars.
- Split `SubscriptionModel` into separate `StreamSubscriptionModel` and `DcbSubscriptionModel` interfaces to make illegal states unrepresentable (decided with the user on 2026-06-26). Today one `SubscriptionModel`/`CatchupSubscriptionModel` carries both modes (constructor-selected, `isDcbMode()`), and the `StartAt` position types are a shared union, so a DCB subscription can be handed a `TimeBasedSubscriptionPosition` and a stream one a `DcbSubscriptionPosition`, reconciled only at runtime (a time-based start on a DCB instance silently goes live). Caveat to honor in the design: the underlying machinery (Mongo change stream, `DurableSubscriptionModel`, `CompetingConsumerSubscriptionModel`) is genuinely shared, and DCB events flow through the same change stream as stream events (ADR 0017), so the split must live at the API/facade and position/query-type level over a shared core, not as a fork of the durable/competing-consumer/catch-up lifecycle. Options span from a minimal DCB-only `StartAt`/position type on the `DcbSubscriptions` DSL up to a full `DcbSubscriptionModel` interface. Own ADR plus PR.
- Rename `@Subscription` to `@StreamSubscription` and introduce `@DcbSubscription` at the same time so the annotation pair is symmetric and unambiguous. `@Subscription` is released, so deprecate it as an alias rather than hard-renaming. Couple this with the `@DcbSubscription` work above.
