# AGENTS.md

Guidance for AI coding agents, and human contributors, working in this repository.

## What this is

Occurrent is a Maven multi-module JVM event-sourcing library built on CloudEvents. Java 21 baseline, Kotlin coexists in most modules (the root build adds `src/main/kotlin` and `src/test/kotlin`). It ships as small composable libraries rather than a framework: domain models stay independent of Occurrent.

## Module layout

- `test-support`: shared test/domain fixtures.
- `eventstore`: event-store APIs (`api/common`, `api/blocking`, `api/reactor`) and implementations (`inmemory`, `mongodb/native`, `mongodb/spring/blocking`, `mongodb/spring/reactor`).
- `subscription`: subscription APIs (blocking/reactor), Mongo/native/Spring/Redis/in-memory adapters, durable/catchup/competing-consumer wrappers, the CloudEvent `push` models, and the `synchronous` wrapper.
- `cloudevents-extension`: Occurrent CloudEvent stream metadata extensions.
- `common`: shared condition/filter/time/retry/Mongo utility modules.
- `application`: `ApplicationService`/`GenericApplicationService`, command composition, `CloudEventConverter`, CloudEvent type mapping, `command-dispatch` plus its `-annotation` and `-dcb` extensions.
- `dsl`: query, subscription, module, decider, Arrow decider, view, projection, DCB, snapshot, and saga DSLs.
- `framework`: Spring Boot MongoDB starter and annotation support.
- `deadline`: deadline scheduling API plus in-memory and JobRunr implementations.
- `library`: higher-level libraries, currently `hederlig`.
- `bom`: published dependency-management BOM.
- `example`: example applications, built by the default-enabled `examples-module` profile.

DCB (Dynamic Consistency Boundary) is a capability layered on the same CloudEvent storage, not a parallel event model. It is shipped for the in-memory, native, and both Spring MongoDB (blocking and reactive) event stores.

## Claiming a GitHub issue before working on it

Several agent sessions run against this repository at the same time, and they cannot see each other. GitHub is the only shared state, so the issue itself is the lock.

Before you start work on an issue, and before you *suggest* an issue to the user as the next thing to pick up, check that nobody else already holds it:

```
gh issue view <N> --json state,labels,assignees,comments
```

Treat the issue as taken if it carries the `in-progress` label, has an assignee, or has a recent claim comment. Say so and pick something else rather than starting in parallel.

If it is free, claim it *before* the first line of work, not after:

```
gh issue edit <N> --add-label in-progress
gh issue comment <N> --body "Claimed by an AI session on <UTC timestamp>, branch \`<branch>\`."
```

The claim is a lease, not a deed. Release it when the work is done or abandoned:

- When a pull request is opened, reference the issue from the PR body (`Fixes #N`) and drop the label — the PR is a stronger, self-updating claim than the label is.
- If you stop without a PR, remove the label and comment that you are dropping it, so the issue does not stay silently blocked.
- A claim with no branch, no PR, and no activity for a day or so is stale. Take it over, but say in a comment that you are doing it.

The check applies to any GitHub task you act on, including issues the user names directly. Claiming is cheap and a duplicated implementation is not.

## Architecture Decision Records

ADRs live in `doc/architecture/decisions/`, **not** `doc/adr/`. Filenames are `NNNN-kebab-case-title.md`, numbered sequentially from the highest existing number. Write one for architectural decisions, not for minor implementation details.

## Changelog

Update `changelog.md` after any change that affects code behavior, a public API, build or runtime behavior, or a notable user-facing capability. Small documentation-only edits do not need an entry.

Unreleased changes go under the existing `### Changelog next version` heading, never under a versioned `### X.Y.Z (date)` section. A version number and date are assigned only at release time, when the maintainer renames that heading. Never invent a version or date for pending work.

When your change refines a feature that is itself still unreleased (its entry already lives under `### Changelog next version`), do not add a separate entry describing the refinement as a change. The release notes describe what ships, not how it was built, so a reader upgrading from the last release should see one coherent entry per feature, not its development history. Fold the final behavior into that feature's existing entry, or drop it if it is purely internal. For example, if flow sagas are new this release, describe the bounded received-event window inside the saga entry rather than adding "the flow saga log is now bounded" as its own change. Words like "now", "hardened", "restored", or "instead of" in an entry for a feature that never shipped are the tell that it should be folded in. This rule is only about refinements to still-unreleased features. A change to behavior that shipped in a previous release is a real change and gets its own entry as usual.

The same release distinction governs whether an API change is safe to make freely. Occurrent is a published library whose external callers cannot be observed from this repository, so do not judge the blast radius of a breaking or shape-changing API change by grepping call sites here (the tests and examples in this repo are not the population of users). Judge it by release status instead. A type or method whose feature still lives under `### Changelog next version` has not shipped, so it can be renamed, reshaped, or removed with no migration path. Once a feature has shipped in a versioned section, assume external callers depend on it and follow the migration conventions: an `org.occurrent.UpgradeToOccurrent_*` OpenRewrite recipe plus an entry under `doc/migration/upgrading-to-*.md`.

## Design intentions

These are the standing intentions behind the design, not conventions you can trade away for convenience. The first is a
constraint. The rest are how the maintainer wants calls made when there is a choice.

**Isolation is a hard rule: no design may lose events, and no saga, projection or subscription may be blocked by
another one being faulty.** It applies per consumer, so a shared delivery carrying one acknowledgement for several
consumers cannot satisfy it whatever else the design has going for it: one consumer that keeps failing holds up the
acknowledgement, and every consumer behind it either never sees the message or loses it when the broker gives up on
it. Check a push or fan-out design against this before anything else. It is what decided the one-sink-per-consumer
topology in ADR 90.

**Aim for the best long-term answer, not the cheapest one that passes.** An easier solution is fine when it yields
roughly the same result. It is not fine when the gap is isolation or correctness.

**Pre-1.0 means past mistakes get corrected.** While Occurrent is 0.x, APIs and the assumptions under them are still
allowed to move. A breaking change is acceptable when there is a clear migration path, ideally an
`org.occurrent.UpgradeToOccurrent_*` OpenRewrite recipe, and it is preferred over carrying a design that is known to be
wrong into 1.0. Avoid breakage where avoiding it costs nothing, but do not preserve a mistake in order to avoid it.
The release-status rule in the changelog section says when a change is breaking at all. This says what to do once it
is.

**Existing structure is not a constraint to design around.** A `final`, a class layout or an interface shape that makes
the right design awkward is itself a candidate for change. Say what the right shape is, then adjust what is in the way,
rather than contorting the new code to fit. Single-consumer registration is the worked example: the first attempt added
an overridable method purely to route around `RegisteringSubscribable.subscribe` being `final`. Questioning the `final`
instead produced a constructor argument, a better design, and the `final` stayed because it turned out to earn its
place. Question it first, then keep it if it does.

Together with the library-not-application rule below, these cover the two questions that come up most: who the change
is for, and what it is allowed to cost.

## Coding conventions

- Java 21 and Kotlin coexist in most modules.
- Public APIs are small capability interfaces composed together, not large monoliths.
- **"Nothing in this repository calls it" is not evidence that nobody needs it.** Occurrent is a published library, so
  its callers are outside this repository, and the tests and examples here are not the population of users. The
  changelog section below states this for removing or reshaping an API, and it applies just as much to *adding* one. An
  overload or accessor that completes an obvious gap in a public type earns its place because a user driving that type
  directly cannot work around its absence, not because something in this tree calls it. `SagaRunner.run` waiting
  unconditionally is the shape of the mistake: no in-repo caller wanted a choice, and a user embedding it had no way to
  get one.
  <br>What this does not license is inventing a public interface for a design that is not settled yet. The line is
  between completing a type that already exists and shipping a new abstraction whose only consumer is imagined. When
  the second is tempting, build it where its first real use lands.
  <br>Everything added this way still needs tests, unless testing it is genuinely disproportionate in effort or runtime.
  Tests are how a capability with no in-repo caller stays honest.
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
- New publishable modules (Maven Central): when you add a new publishable leaf module (a normal `jar` artifact), (1) register it in its parent aggregator's `<modules>` and add a `${project.version}` dependency entry to `bom/pom.xml`; (2) do NOT add it to `<excludeArtifacts>`, since leaf modules are published by default; (3) do NOT redeclare `name`, `description`, `url`, `licenses`, `developers`, `scm`, or the source/javadoc/GPG/flatten plugins, because they are all inherited from the root `pom.xml` (keep the POM as minimal as the `occurrent-command-dispatch` sibling); (4) verify a release install (`mvn -Prelease ... install`, or `mvn_local_snapshot.sh`) emits the main jar plus `-sources.jar`, `-javadoc.jar`, and a flattened consumer POM, and that the module is not reported as skipped by `central-publishing-maven-plugin`.

## Deeper context

`.context/ORCHESTRATOR.md`, when present, holds a maintained map of in-flight work, past decisions with their rationale, and known-fragile areas. An agent operating in an orchestrator-style session should read it. It is session-scoped working memory, not a substitute for this file.

Keep it current: when a change warrants a memory update (a new capability, a shipped release, an architectural decision, a new fragile area, or a shift in in-flight work), update `.context/ORCHESTRATOR.md` in the SAME pull request or push as the work it describes, so the memory lands together with the change rather than drifting behind it. Also prune detail that has become git, changelog, or ADR history so the file stays a small durable map, not a change log. Trivial changes need no update. Only when folding it into the original PR or push is not possible (the work already merged, or it is a standalone memory refresh with no accompanying change) commit `.context/ORCHESTRATOR.md` straight to `main` without a pull request, prefixing the commit message with `[ci skip]` so the update does not trigger a CI build.
