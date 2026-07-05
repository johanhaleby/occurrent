# Occurrent Orchestrator Memory

Last updated: 2026-07-02 (reactive DCB hotel-booking example + reactive DCB API polish merged to main)

## Current State

**Reactive DCB hotel-booking example + reactive API polish merged to `main`** (2026-07-02, commits `aad40a8ec` and `17744c80a`, pushed directly to `main`, no PR, verified locally under Colima but NOT through CI). New reactive DCB example `example/domain/hotel-booking` (Kotlin, WebFlux + Thymeleaf + HTMX), the reactive counterpart to the blocking `course-enrollment` example, wired by `spring-boot-starter-mongodb-reactive` with `@EnableOccurrentReactive`. Three deciders where the cross-boundary `bookingDecider` holds the no-double-booking invariant (room, half-open `[checkIn, checkOut)` stay overlap) and the per-guest active-booking-limit invariant in one conditional append via `DcbQuery.tagsAnyOf(room, guest)`. Live SSE feed is a `DcbSubscriptions` `Flux` returned straight from the controller, so WebFlux auto-cancels on client disconnect with none of the blocking `SseEmitter` id/cancel/`waitUntilStarted` bookkeeping. Building it surfaced reactive API quirks; three were fixed in the polish commit: (1) reactive `DcbApplicationService.execute` return type changed from `Mono<Optional<DcbAppendResult>>` to `Mono<DcbAppendResult>` (empty Mono means no new events), matching the Kotlin `execute` decider extension that already flattened it, with side-effect-once preserved (no-op path emits `Mono.empty()`, append path carries the result); (2) added reactive `queryForListWithPosition` to `dcb-dsl-reactor` for parity with blocking (`queryForList` already existed on both, no reactive `queryForSequence` since `query()` is already the lazy `Flux`); (3) fixed the blocking `course-enrollment` `application.yml` to use `spring.data.mongodb.uri`, the key Spring Boot's `MongoProperties` actually binds (`spring.mongodb.uri` was silently ignored outside the Testcontainers test). Changelog updated, ADR 0035's return-type mention corrected inline (changelog-only, no new ADR for a small unreleased-API refinement).

DCB has shipped on `main` for all four event stores (in-memory, Spring blocking, native driver, Spring reactive). The shared Mongo DCB code lives in `eventstore-mongodb-dcb-common` (`DcbMarkerModel`, `DcbDocumentMapper`), and the event-store capability set is the shared `EventStoreCapability` enum in `eventstore-api-common`. The reactive DCB stack (PRs #242-#247) landed via `av-land` after the prior update.

**Reactive Spring Boot starter shipped and merged** (PRs #256-#261, all on `main`): `spring-boot-starter-mongodb-reactive` gives reactive apps the same one-dependency auto-configuration experience as the blocking `spring-boot-starter-mongodb`, gated by classpath presence (not a runtime flag), mirroring Spring Boot's own `-data-mongodb` vs `-data-mongodb-reactive` split. Stack-neutral pieces (`OccurrentProperties`, the Jackson 3 converter config, the capability `Condition` classes, the annotation-parsing helpers, the DCB `ApplicationService` registrar) were extracted into a new shared module `spring-boot-autoconfigure-mongodb-common` (package `org.occurrent.springboot.mongo.common`) that both starters depend on, avoiding duplicate-bean clashes if both are ever on one classpath. Three reactive substrate gaps had to be built first: `ReactorDurableSubscriptionModel` and `ReactorDcbCatchupSubscriptionModel` were reshaped to implement the reactive `Subscribable`/`PositionAwareSubscriptionModel`/`SubscriptionModelLifeCycle` interfaces (a breaking change to `ReactorDurableSubscriptionModel`'s old cold `Mono<Void> subscribe(id, action)` API, replaced by the hot `Subscription`-returning `Subscribable` API) so they compose into one `Durable(Catchup(mongo))` model; a reactive `StreamSubscriptions` DSL (`subscription-dsl-reactor`) was added; and the reactive DCB DSL gained named, lifecycle-managed subscribe. `@StreamSubscription`/`@DcbSubscription` both work on the reactive stack via `OccurrentReactiveAnnotationBeanPostProcessor`. Known, documented gaps: no reactive competing-consumer model, and no reactive STREAM catch-up (only DCB catch-up exists reactively), so `@StreamSubscription` on the reactive stack fails loud for any history-replay start (`BEGINNING_OF_TIME`, an ISO8601 date, or an epoch millis start), supporting only `NOW`/`DEFAULT`. See ADR 44.

**Annotation processor rename + full behavioral test matrix, PR #264** (open, Copilot review requested): the blocking `OccurrentAnnotationBeanPostProcessor` was renamed to `OccurrentBlockingAnnotationBeanPostProcessor` for symmetry with `OccurrentReactiveAnnotationBeanPostProcessor` (both package-private, internal only). 14 new integration test classes were added across both starters covering the full `startAt` matrix (including previously-untested `startAtISO8601`/`startAtTimeEpochMillis`), durable resume vs. replay across an ACTUAL application restart (close the Spring context, boot a fresh one against the same MongoDB backing data) for every `resumeBehavior`, metadata binding parity, reactive handler return-type adaptation (`void`/`Mono<Void>`/non-`Void` `Mono<T>`), characterized handler-error behavior on both stacks, and `startupMode = WAIT_UNTIL_STARTED`. Writing the reactive DCB restart test found and fixed a real, previously-undiscovered bug in `ReactorDcbCatchupSubscriptionModel`: resuming from an explicit non-zero DCB position (`DcbStartAt.afterPosition(N)`, e.g. `@DcbSubscription(startAtDcbPosition = N)`) could redeliver an event at or before that position through the live handover, because the live stream's id-based dedup cache only covered what the bulk replay itself fetched (which deliberately excludes everything `<= N`). Fixed with a one-line filter change (`getPosition(cloudEvent) > startPosition` instead of `> 0`). `N = 0` (beginning-of-history replay) was never affected. See "Restart-test pattern" and "Testcontainers gotchas" below for reusable patterns this PR established.

The `av-land` helper at `/usr/local/bin/av-land` was rewritten to be prune-safe (retarget each child to trunk before deleting the parent branch) and hang-proof (git rebase instead of in-loop `av sync`, timeout-guarded). Verified on a throwaway stack and again landing the 6-PR reactive-starter stack.

### Restart-test pattern (new, reusable)

Annotation attributes (`startAt`, `resumeBehavior`, `startupMode`) are compile-time constants, and the `DcbStartAt`/`StartAt` dynamic supplier that encodes `resumeBehavior` is evaluated exactly ONCE, inside the subscription model's `subscribe(...)` call at bean-postprocessing time. Pausing/resuming the same running model (`stop()`/`start()`) never re-invokes it, so it cannot demonstrate replay-vs-resume. To actually test `resumeBehavior`, close the Spring context and boot a fresh one (`SpringApplication.run(SecondBootApplication.class, args)`) against the same durable MongoDB backing data (same `spring.data.mongodb.uri`, same database name), so the annotation is reprocessed from scratch, exactly like a real process restart. Pattern used in `DcbSubscriptionResumeBehaviorAnnotationMongoTest`, `StreamSubscriptionResumeBehaviorAnnotationMongoTest`, `ReactiveDcbSubscriptionResumeBehaviorAnnotationMongoTest`: a `FirstBootApplication` (with a `HistoryAppender`) and a `SecondBootApplication` (with an `OfflineAppender`) both registering the SAME subscriber classes, so ctx2's subscriber is a fresh instance whose `received()` list starts empty.

### Testcontainers gotchas (new)

- `MongoDBContainer.getReplicaSetUrl()` (no-arg) always targets the default `"test"` database. Use the single-arg overload `getReplicaSetUrl(String databaseName)` for an isolated per-test database; do NOT string-concatenate a suffix onto the no-arg result (`getReplicaSetUrl() + "." + name`), since MongoDB forbids dots in database names and the concatenation silently still resolves to `"test"`, causing cross-test-class collisions when multiple test classes reuse one container.
- Booting a Spring context directly via `SpringApplication.run(...)` (not `@SpringBootTest`) gets no `@ServiceConnection` auto-wiring; pass `--spring.data.mongodb.uri=...` explicitly in the `args` array.
- For this pattern, `getReplicaSetUrl()`'s internal `getConnectionString()` uses `getMappedPort()`, so a container with a dynamically-assigned host port should resolve correctly on its own, but this repo's established workaround (see `OccurrentReactiveMongoAutoConfigurationWiringTest`) still pins the host port to `27017:27017` with `.withReuse(true)`. Root cause of needing the fixed port was not fully isolated (possibly just Colima/Testcontainers flakiness on this machine, not a deterministic bug), but the fixed-port+reuse pattern is proven reliable and is now used by 3 more test classes in this PR.
- Environment: MongoDB Testcontainers tests on this machine intermittently fail with `MongoSocketOpenException`/`Prematurely reached end of stream` immediately after container start, a known Colima networking flake (see `colima.md`, `colima stop; colima start --vm-type vz --network-address`). Retry once before concluding a test is broken; it is usually not.

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
- Java 21 baseline; Kotlin JVM target follows Java 21.
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
- Spring Boot DCB application-service auto-configuration must not gate the bean's existence with `@ConditionalOnBean(TagGenerator.class)`: that condition evaluates while bean definitions are still being processed, so it can miss a user's `TagGenerator` in a real Boot application context even though the equivalent check passes under `ApplicationContextRunner` slice tests (confirmed empirically, `johan/dcb-application-service-bean`; a prior plan draft assumed `@ConditionalOnBean` was safe based only on slice-test results and had to be corrected after real-context testing). The bean is a normal, generically-typed `@Bean` method (on both starters) that resolves `TagGenerator` through `ObjectProvider.getIfAvailable()` at instantiation time and returns `null` when none exists, logging the same warning as before. This replaced an earlier `BeanFactoryPostProcessor`-based registration, which IntelliJ could not statically resolve, producing a false "Could not autowire" warning at every injection site.
- In-memory `deleteAll()` must reset DCB sequence state as well as event state, otherwise an empty store can report stale DCB high-watermarks after deletion.

## Build And Verification

Required shell workflow:
- Use `rtk <command>` whenever `rtk` supports the command.
- Before raw shell commands, explicitly consider whether `rtk` supports the command. Use raw commands only when unsupported or proven unsuitable.

CI:
- `.github/workflows/maven.yml` runs `mvn -B package --file pom.xml` on Temurin Java 21 and 25.
- CI enables Testcontainers reuse by writing `$HOME/.testcontainers.properties`.

Local focused Maven patterns:
- Full package: `rtk mvn -B package --file pom.xml`
- Focused module test: `rtk mvn -pl <module-path> -am test`
- Focused module package: `rtk mvn -pl <module-path> -am package`
- Focused test class: `rtk mvn -pl <module-path> -am -Dtest=<TestClass> test`
- Kotlin/generic API changes should usually run at least `rtk mvn -pl <module-path> -am test-compile` plus focused tests.
- DCB touched-module verification: `rtk mvn -q -pl eventstore/api/dcb,eventstore/inmemory,eventstore/mongodb/spring/blocking,application/service/blocking,subscription/mongodb/spring/blocking -am test`

Release scripts:
- `mvn_local_snapshot.sh` runs release-profile local install with `-Drevision=...` and optional skip tests.
- `mvn_release.sh` requires Java 21, uses `mvn deploy -Prelease`, GPG signing, source/javadoc jars, Sonatype Central publishing, and tags `occurrent-<version>`.

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

## Deferred Follow-ups (DCB): DONE since last update

The following items in this section as of 2026-06-30 have since shipped: `@DcbSubscription` annotation (both stacks), dual-mode STREAM+DCB catch-up (`CatchupSubscriptionModel` dual-mode constructor, see `DcbDualModeCatchupAutoConfigurationMongoTest`), `DcbSubscriptions` cancel/unsubscribe (`DcbSubscriptionModel.cancelSubscription`, named lifecycle-managed subscribe on both stacks), `DcbSubscriptions` server-side filtering (`DcbSubscriptionFilter`, see `DcbServerSideSubscriptionFilteringMongoTest`), the `SubscriptionModel` split into `Subscribable`/`PositionAwareSubscriptionModel`/`SubscriptionModelLifeCycle` plus a `DcbSubscriptionModel` facade/adapter, and the `@Subscription` to `@StreamSubscription` rename (deprecated alias kept, ADR 0026).

## Deferred Follow-ups (current)

- Reactive competing-consumer subscription model: does not exist. The reactive stack has no equivalent of `SpringMongoLeaseCompetingConsumerStrategy`/`CompetingConsumerSubscriptionModel`, so a reactive subscription model is never competing-consumer wrapped. Documented as a known gap in ADR 44, not started.
- Reactive STREAM catch-up model: BUILT by the in-progress unified-position work (`ReactorStreamCatchupSubscriptionModel`, branch `johan/position-foundation`, ADR 45). On a reactive STREAM-only store with position on, `@StreamSubscription` history replay works; the fail-loud path now fires only when position is off. Supersedes the old "does not exist" note (ADR 44). (Pending merge of the position feature.)
- Two `simpleName!!` non-null-assertion threads on PR #257 (reactive stream subscription DSL) were left as a deliberate open disagreement with a Copilot suggestion and rode along resolved-by-merge. If a safe fallback for anonymous event classes is still wanted in the stream/DCB DSLs, it's a small standalone follow-up, not done.
- `DcbDomainEventQueries` Kotlin Sequence/List extensions are leak-free without explicit close (materializes into a `List` under the hood). Open question, not yet checked: whether the stream-side `DomainEventQueries.queryForSequence` (query-dsl) needs a closing/loan-pattern variant if the underlying stream event store returns a live Mongo-cursor-backed `Stream`.
- **Reactive dual-mode (combined STREAM+DCB) catch-up: DONE (branch `johan/reactive-catchup-decouple`, pending merge).** A new `ReactorCatchupSubscriptionModel` dispatcher routes each subscription to the stream or DCB path by filter type and start position, mirroring the blocking `CatchupSubscriptionModel`. The two reactive models were refactored onto a shared DCB-free pipeline (`CatchupReader`, `HandoverCache`, `PositionCatchupPipeline`) living in the stream module, so reactive stream catch-up no longer depends on the DCB API. A combined reactive store replays both stream and DCB history, and the fail-loud guard now fires only when position is off. A routing bug was found and fixed along the way: because stream and DCB replay both use a `GlobalSubscriptionPosition`, `routesToDcb` must key on filter type first (`DcbSubscriptionFilter` to DCB, `OccurrentSubscriptionFilter` to stream, position heuristic only for a null filter). A `GlobalSubscriptionPosition` start alone is ambiguous, so the old `filter instanceof DcbSubscriptionFilter || startsAtExplicitDcbPosition(startAt)` misrouted a stream replay-from-0 subscription to the DCB model, which rejects the stream filter.
- **Blocking stream catch-up DCB decoupling: DONE (branch `johan/blocking-catchup-decouple`, stacked on the reactive PR, pending merge).** Split `subscription/util/blocking/catchup-subscription` into a DCB-free `stream-catchup-subscription` (`StreamCatchupSubscriptionModel` plus the shared `FixedSizeCache`/window/handover helpers, keeping both the position and legacy time paths) and a `dcb-catchup-subscription` (`DcbCatchupSubscriptionModel` plus the `CatchupSubscriptionModel` dispatcher). The dispatcher keeps the class name, package, and all public constructors, and the dcb module keeps the `catchup-subscription` artifactId, so no consumer import or dependency changed except a new bom entry. Applied the same filter-first `routesToDcb` fix as reactive (the byte-identical latent bug, dodged before only because the blocking annotation processor maps `@StreamSubscription(BEGINNING_OF_TIME)` to a `TimeBasedSubscriptionPosition`). A `subscriptionModelContextType` was threaded into both inner models so `StartAt.dynamic` callers that pattern-match on `CatchupSubscriptionModel.class` keep working across the module boundary. The reactive `PositionCatchupPipeline` abstraction was deliberately NOT ported to blocking, because the blocking position loop persists the subscription position per delivered event interleaved with delivery, so the position-window loops were kept as faithful direct ports instead.
- **Reactive `@StreamSubscription` cannot honor a specific historical start time (deferred, ADR 45).** Reactive stream catch-up is position-based: it supports `BEGINNING_OF_TIME` (maps to position 0), `NOW`, and `DEFAULT`, but a specific `startAtISO8601`/`startAtTimeEpochMillis` fails loud. Blocking honors a specific time via its time/`$natural` cursor. Future options: translate a requested time to a position via a lookup, or add reactive time-window replay.
- **Counter-contention benchmark: deferred (ADR 45 non-goal).** Unified position uses one shared counter with per-append block reservation reserved outside the write transaction (mirrors Postgres sequence caching). Parkster load is modest (pushnotification-prod: M20, hundreds of ops/sec) so no benchmark was run. If an open-source user reports scale pain, benchmark the counter (naive vs block-reserved) against a production-like Atlas cluster reading WiredTiger write-ticket/write-conflict metrics; sharded/partitioned counters are the escape hatch.
- Reactive competing-consumer remains the main open reactive gap. Stream catch-up is now DCB-free and dual-mode routed on both stacks (reactive + blocking done).

## Examples With Web UIs (both shipped)

- Blocking `example/domain/course-enrollment` (DCB, blocking starter): materialized-view dashboard, `@DcbSubscription` from BEGINNING (in-memory read model), strongly-consistent detail read via `DcbDomainEventQueries.queryForSequence`, tag-scoped SSE activity feed via blocking `DcbSubscriptions` named subscribe + cancel, Thymeleaf + HTMX. Shipped.
- Reactive `example/domain/hotel-booking` (DCB, reactive starter): same shape reactively, SSE feed via `DcbSubscriptions` `Flux`. Shipped this session (see Current State).
