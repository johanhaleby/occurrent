# AGENTS.md

Guidance for AI coding agents, and human contributors, working in this repository.

## What this is

Occurrent is a Maven multi-module JVM event-sourcing library built on CloudEvents. Java 21 baseline, Kotlin coexists in most modules (the root build adds `src/main/kotlin` and `src/test/kotlin`). It ships as small composable libraries rather than a framework: domain models stay independent of Occurrent.

## Module layout

- `test-support`: shared test/domain fixtures.
- `eventstore`: event-store APIs (`api/common`, `api/blocking`, `api/reactor`) and implementations (`inmemory`, `mongodb/native`, `mongodb/spring/blocking`, `mongodb/spring/reactor`).
- `subscription`: subscription APIs (blocking/reactor), Mongo/native/Spring/Redis/in-memory adapters, durable/catchup/competing-consumer wrappers.
- `cloudevents-extension`: Occurrent CloudEvent stream metadata extensions.
- `common`: shared condition/filter/time/retry/Mongo utility modules.
- `application`: `ApplicationService`/`GenericApplicationService`, command composition, `CloudEventConverter`, CloudEvent type mapping.
- `dsl`: query, subscription, module, decider, Arrow decider, and view DSLs.
- `framework`: Spring Boot MongoDB starter and annotation support.
- `deadline`: deadline scheduling API plus in-memory and JobRunr implementations.
- `library`: higher-level libraries, currently `hederlig`.
- `bom`: published dependency-management BOM.
- `example`: example applications, built by the default-enabled `examples-module` profile.

DCB (Dynamic Consistency Boundary) is a capability layered on the same CloudEvent storage, not a parallel event model. It is shipped for the in-memory, native, and both Spring MongoDB (blocking and reactive) event stores.

## Architecture Decision Records

ADRs live in `doc/architecture/decisions/`, **not** `doc/adr/`. Filenames are `NNNN-kebab-case-title.md`, numbered sequentially from the highest existing number. Write one for architectural decisions, not for minor implementation details.

## Changelog

Update `changelog.md` after any change that affects code behavior, a public API, build or runtime behavior, or a notable user-facing capability. Small documentation-only edits do not need an entry.

Unreleased changes go under the existing `### Changelog next version` heading, never under a versioned `### X.Y.Z (date)` section. A version number and date are assigned only at release time, when the maintainer renames that heading. Never invent a version or date for pending work.

## Coding conventions

- Java 21 and Kotlin coexist in most modules.
- Public APIs are small capability interfaces composed together, not large monoliths.
- Nullness uses JSpecify (`@NullMarked`, `@Nullable`) in newer APIs, not uniformly across older code.
- Validate nulls and invalid arguments eagerly, with `Objects.requireNonNull` or `IllegalArgumentException`.
- Prefer static factories and builders for fluent public APIs.
- Apache 2 license headers on source files.
- Kotlin extension names must not collide with Java members on the same type (see ADR 0012).
- When changing a Kotlin wrapper around Java generics, run at least `test-compile` on the affected module. Kotlin type inference can fail even when the equivalent Java compiles cleanly.

## Testing

- JUnit 5 plus AssertJ is the dominant style. jqwik covers a small set of property tests. Awaitility backs async, subscription, and deadline assertions.
- Docker and Testcontainers-backed tests are common, mainly MongoDB and Redis. Some tests bind MongoDB to host port `27017`, which can collide with a locally running MongoDB or a concurrent test run.
- On a macOS Docker runtime such as Colima, Mongo Testcontainers can intermittently fail with `MongoSocketOpenException` or "Prematurely reached end of stream" right after container start. Retry once before concluding a test is broken.
- `MongoDBContainer.getReplicaSetUrl()` (no argument) always targets the `test` database. Use `getReplicaSetUrl(String databaseName)` for isolation, and do not string-concat a suffix onto the URL, because MongoDB forbids dots in database names, so the name silently stays `test` and causes cross-test collisions.
- Restart-pattern tests that boot a fresh context with `SpringApplication.run(...)` rather than `@SpringBootTest` get no `@ServiceConnection`, so pass `--spring.data.mongodb.uri=...` in the args. Those tests pin host port `27017` with `.withReuse(true)`, which is reliable here.
- There is no Failsafe split. Unit and integration-style tests both run under Surefire.

## Build and verification

- Full build: `mvn -B package --file pom.xml` (CI runs this on Temurin Java 21 and 25).
- Focused module test: `mvn -pl <module-path> -am test`.
- Focused test class: `mvn -pl <module-path> -am -Dtest=<TestClass> -Dsurefire.failIfNoSpecifiedTests=false test`.
- Release: `mvn_release.sh` (Java 21, `mvn deploy -Prelease`, GPG signing, Sonatype Central publishing).
- Publishing exclusions: when you add a new aggregate parent POM (a `packaging` of `pom` that only groups `<modules>` and has no publishable artifact of its own), add its `artifactId` to `<excludeArtifacts>` in the root `pom.xml` under the `central-publishing-maven-plugin` config. Aggregate POMs flatten to metadata-less POMs, so if one is left in the release it fails Central validation with missing name, description, url, license, scm, and developers. The `bom` is the only pom-packaged module that is published (it uses `flattenMode=bom` to keep that metadata), so it stays off the exclude list.

## Deeper context

`.context/ORCHESTRATOR.md`, when present, holds a maintained map of in-flight work, past decisions with their rationale, and known-fragile areas. An agent operating in an orchestrator-style session should read it. It is session-scoped working memory, not a substitute for this file.

Keep it current: when a change warrants a memory update (a new capability, a shipped release, an architectural decision, a new fragile area, or a shift in in-flight work), update `.context/ORCHESTRATOR.md` in the SAME pull request or push as the work it describes, so the memory lands together with the change rather than drifting behind it. Also prune detail that has become git, changelog, or ADR history so the file stays a small durable map, not a change log. Trivial changes need no update. Only when folding it into the original PR or push is not possible (the work already merged, or it is a standalone memory refresh with no accompanying change) commit `.context/ORCHESTRATOR.md` straight to `main` without a pull request, prefixing the commit message with `[ci skip]` so the update does not trigger a CI build.
