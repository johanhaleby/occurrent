# 55. Uniform Occurrent artifact coordinate naming

Date: 2026-07-11

## Status

Accepted

## Context

Occurrent publishes around 82 artifacts, all under the single groupId `org.occurrent`, one BOM, one `${revision}` version, released as one train. The artifactId scheme grew organically and is now inconsistent in several ways.

Some artifactIds are bare, generic jar names that say nothing about the project: `filter`, `retry`, `time`, `decider`, `annotations`. The reactive variant of a module is named two different ways: most modules append `-reactor`, but `subscription/util` prepends `reactor-` instead, and the starter uses `-reactive` instead of either. The blocking modules in `subscription/util` have neither a subsystem prefix nor a `-blocking` marker, so nothing in the name says what they are. The two Spring Boot starters use the `spring-boot-starter-*` prefix, which is reserved for Spring's own starters. Spring's documentation is explicit that third parties must not lead with that prefix.

Occurrent 0.30.0 already breaks stable APIs (`Stream` to `List` in [ADR 54](0054-list-instead-of-stream-for-event-store-writes.md), the `Checkpoint` rename family in [ADR 46](0046-rename-subscription-position-to-checkpoint.md), the move to Java 21). Consumers are already updating for those changes, so this is the cheapest point to normalize the artifact coordinates too. Doing it now means one migration instead of two.

## Decision

Keep the single `org.occurrent` groupId. Prefix every published artifact with `occurrent-`. This is the convention other single-groupId monorepos use for the same reason: `org.springframework:spring-*`, `io.micrometer:micrometer-*`, `com.fasterxml.jackson.core:jackson-*`. The rule for a new artifactId is `occurrent-` plus the existing descriptive name, with descriptive repairs where the existing name was too generic to survive the prefix on its own.

The scope is published leaves only. The aggregator POMs, the root `occurrent` parent, `test-support`, and every `example-*` module are not renamed, because they are never published. Adding `occurrent-` to them would suggest they are dependable published modules when they are not. This also means no published leaf ends up in the release `<excludeArtifacts>` list, so that list needs no changes.

Reactive naming is normalized alongside the prefix. Blocking modules that need a marker get an explicit `-blocking` suffix. Reactive library modules are standardized on the `-reactor` suffix, replacing both the `reactor-` prefix in `subscription/util` and the `-reactive` suffix on the starter. The two starters move to Spring's convention for third-party starters, `occurrent-mongodb-spring-boot-starter` and `occurrent-mongodb-reactive-spring-boot-starter`, with the matching autoconfigure module renamed to `occurrent-mongodb-spring-boot-autoconfigure`.

### Rejected alternatives

**Subgroup groupIds**, for example `org.occurrent.subscription:inmemory`. Dotted subgroups make sense for projects that release their parts separately, like Spring Boot or Spring Data. Occurrent releases everything as one train, so a subgroup buys nothing here and causes real jar-name collisions once the subsystem name is dropped from the artifactId: `inmemory`, `common`, `api-blocking`, and `mongodb-spring-blocking` all exist under both `eventstore` and `subscription`. This is exactly why Spring keeps its project prefix in the artifactId even though it also has a groupId subgroup, `spring-boot-starter-web`, never `starter-web`.

**A minimal pass that fixes only the most ambiguous names.** This leaves a patchwork scheme behind and risks a second breaking rename later, once more consumers depend on the half-fixed names.

**Prefixing the unpublished modules too.** This is pure churn with no benefit to any consumer, since nothing depends on those coordinates. It would also require rewriting the exclude list in lockstep, with a real footgun: a missed entry would silently publish a demo module.

**A subsystem-first scheme with no project prefix.** A name like `subscription-inmemory` does not identify the project at all, and modules that do not sit under a subsystem, like `cloudevent-converter-*` or `command-composition`, would need an awkward standalone token instead.

## Consequences

Every consumer's coordinates change, not only the worst-named ones. That is the deliberate cost of doing this once instead of leaving some artifacts inconsistent and needing a second pass later.

An OpenRewrite recipe automates the coordinate rewrite for Maven and Gradle consumers. The upgrade guide's mapping table covers everyone else, including other build tools and documentation that references the old names.

Old coordinates stay on Maven Central pointing at 0.20.x and earlier. That is true of any rename and is acceptable for a project that has not reached 1.0 yet.

No Java package, type, public API, or directory changes anything here. This is a coordinate-only change.

The standing rule going forward: every new published artifact gets the `occurrent-` prefix, and unpublished modules do not.
