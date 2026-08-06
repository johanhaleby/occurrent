# Upgrading to Occurrent 0.30.0

0.30.0 renames every published artifact coordinate, renames several types, moves the write side from `Stream` to `List`, and changes a few runtime defaults. Most of the mechanical work is handled by an OpenRewrite recipe. This guide covers the coordinate change first, then splits the code change into three groups: what the recipe fixes for you, what needs a manual pass, and what to read before you deploy.

## 1. Update your dependency coordinates

0.30.0 gives every published artifact an `occurrent-` prefix. The `org.occurrent` groupId is unchanged, and this is a coordinate-only change, so no package or type imports move because of it. The two Spring Boot starters also move to Spring's third-party convention, so `spring-boot-starter-mongodb` becomes `occurrent-mongodb-spring-boot-starter` and `spring-boot-starter-mongodb-reactive` becomes `occurrent-mongodb-reactive-spring-boot-starter`.

The recipe in the next section rewrites these coordinates for Maven and Gradle automatically, so you can skip ahead if you use one of those. For any other build tool, or to check the result by hand, the full mapping is below.

| Old artifactId | New artifactId |
|----------------|----------------|
| `annotations` | `occurrent-annotations` |
| `application-service-blocking` | `occurrent-application-service-blocking` |
| `application-service-common` | `occurrent-application-service-common` |
| `application-service-reactor` | `occurrent-application-service-reactor` |
| `catchup-subscription` | `occurrent-subscription-catchup-blocking` |
| `cloudevent-converter-api` | `occurrent-cloudevent-converter-api` |
| `cloudevent-converter-core` | `occurrent-cloudevent-converter-core` |
| `cloudevent-converter-generic` | `occurrent-cloudevent-converter-generic` |
| `cloudevent-converter-jackson` | `occurrent-cloudevent-converter-jackson` |
| `cloudevent-converter-jackson3` | `occurrent-cloudevent-converter-jackson3` |
| `cloudevent-converter-xstream` | `occurrent-cloudevent-converter-xstream` |
| `cloudevent-type-mapper-api` | `occurrent-cloudevent-type-mapper-api` |
| `cloudevent-type-mapper-reflection` | `occurrent-cloudevent-type-mapper-reflection` |
| `cloudevents-extension` | `occurrent-cloudevents-extension` |
| `command-composition` | `occurrent-command-composition` |
| `competing-consumer-subscription` | `occurrent-subscription-competing-consumer-blocking` |
| `dcb-annotation-taggenerator` | `occurrent-dcb-annotation-taggenerator` |
| `dcb-dsl-blocking` | `occurrent-dcb-dsl-blocking` |
| `dcb-dsl-common` | `occurrent-dcb-dsl-common` |
| `dcb-dsl-reactor` | `occurrent-dcb-dsl-reactor` |
| `deadline-api-blocking` | `occurrent-deadline-api-blocking` |
| `deadline-inmemory` | `occurrent-deadline-inmemory` |
| `deadline-jobrunr` | `occurrent-deadline-jobrunr` |
| `decider` | `occurrent-decider` |
| `decider-arrow` | `occurrent-decider-arrow` |
| `durable-subscription` | `occurrent-subscription-durable-blocking` |
| `eventstore-api-blocking` | `occurrent-eventstore-api-blocking` |
| `eventstore-api-common` | `occurrent-eventstore-api-common` |
| `eventstore-api-dcb` | `occurrent-eventstore-api-dcb` |
| `eventstore-api-dcb-reactor` | `occurrent-eventstore-api-dcb-reactor` |
| `eventstore-api-reactor` | `occurrent-eventstore-api-reactor` |
| `eventstore-capability` | `occurrent-eventstore-capability` |
| `eventstore-inmemory` | `occurrent-eventstore-inmemory` |
| `eventstore-mongodb-common` | `occurrent-eventstore-mongodb-common` |
| `eventstore-mongodb-dcb-common` | `occurrent-eventstore-mongodb-dcb-common` |
| `eventstore-mongodb-native` | `occurrent-eventstore-mongodb-native` |
| `eventstore-mongodb-position-backfill` | `occurrent-eventstore-mongodb-position-backfill` |
| `eventstore-mongodb-spring-blocking` | `occurrent-eventstore-mongodb-spring-blocking` |
| `eventstore-mongodb-spring-reactor` | `occurrent-eventstore-mongodb-spring-reactor` |
| `filter` | `occurrent-filter` |
| `functional-support` | `occurrent-functional-support` |
| `hederlig` | `occurrent-hederlig` |
| `inmemory-filter-matching` | `occurrent-common-inmemory-filter-matching` |
| `module-dsl-blocking` | `occurrent-module-dsl-blocking` |
| `mongodb-native-filter-bsonfilter-conversion` | `occurrent-common-mongodb-native-filter-bsonfilter-conversion` |
| `mongodb-specialfilterhandling` | `occurrent-common-mongodb-specialfilterhandling` |
| `mongodb-spring-filter-query-conversion` | `occurrent-common-mongodb-spring-filter-query-conversion` |
| `mongodb-spring-sort-conversion` | `occurrent-common-mongodb-spring-sort-conversion` |
| `mongodb-timerepresentation` | `occurrent-common-mongodb-timerepresentation` |
| `query-dsl-blocking` | `occurrent-query-dsl-blocking` |
| `query-dsl-reactor` | `occurrent-query-dsl-reactor` |
| `reactor-catchup-subscription` | `occurrent-subscription-catchup-reactor` |
| `reactor-durable-subscription` | `occurrent-subscription-durable-reactor` |
| `reactor-stream-catchup-subscription` | `occurrent-subscription-stream-catchup-reactor` |
| `retry` | `occurrent-retry` |
| `spring-boot-autoconfigure-mongodb-common` | `occurrent-mongodb-spring-boot-autoconfigure` |
| `spring-boot-starter-mongodb` | `occurrent-mongodb-spring-boot-starter` |
| `spring-boot-starter-mongodb-reactive` | `occurrent-mongodb-reactive-spring-boot-starter` |
| `stream-catchup-subscription` | `occurrent-subscription-stream-catchup-blocking` |
| `subscription-api-blocking` | `occurrent-subscription-api-blocking` |
| `subscription-api-reactor` | `occurrent-subscription-api-reactor` |
| `subscription-core` | `occurrent-subscription-core` |
| `subscription-dsl-blocking` | `occurrent-subscription-dsl-blocking` |
| `subscription-dsl-common` | `occurrent-subscription-dsl-common` |
| `subscription-dsl-reactor` | `occurrent-subscription-dsl-reactor` |
| `subscription-inmemory` | `occurrent-subscription-inmemory` |
| `subscription-mongodb-base` | `occurrent-subscription-mongodb-common-base` |
| `subscription-mongodb-common-blocking-competing-consumer-strategy` | `occurrent-subscription-mongodb-common-blocking-competing-consumer-strategy` |
| `subscription-mongodb-native-blocking` | `occurrent-subscription-mongodb-native-blocking` |
| `subscription-mongodb-native-blocking-competing-consumer-strategy` | `occurrent-subscription-mongodb-native-blocking-competing-consumer-strategy` |
| `subscription-mongodb-native-blocking-position-storage` | `occurrent-subscription-mongodb-native-blocking-position-storage` |
| `subscription-mongodb-spring-blocking` | `occurrent-subscription-mongodb-spring-blocking` |
| `subscription-mongodb-spring-blocking-competing-consumer-strategy` | `occurrent-subscription-mongodb-spring-blocking-competing-consumer-strategy` |
| `subscription-mongodb-spring-blocking-position-storage` | `occurrent-subscription-mongodb-spring-blocking-position-storage` |
| `subscription-mongodb-spring-common` | `occurrent-subscription-mongodb-spring-common` |
| `subscription-mongodb-spring-reactor` | `occurrent-subscription-mongodb-spring-reactor` |
| `subscription-mongodb-spring-reactor-position-storage` | `occurrent-subscription-mongodb-spring-reactor-position-storage` |
| `subscription-redis-spring-blocking-position-storage` | `occurrent-subscription-redis-spring-blocking-position-storage` |
| `subscription-util-predicates` | `occurrent-subscription-util-predicates` |
| `time` | `occurrent-time` |
| `view-dsl` | `occurrent-view-dsl` |

Unpublished modules keep their names. The aggregator POMs, `test-support`, and the `example-*` modules are not renamed, because you never depend on them.

## 2. Run the recipe first

Add the `rewrite-maven-plugin` and point it at the umbrella recipe, `org.occurrent.UpgradeToOccurrent_0_30`:

```xml
<plugin>
  <groupId>org.openrewrite.maven</groupId>
  <artifactId>rewrite-maven-plugin</artifactId>
  <version><!-- use the latest rewrite-maven-plugin release --></version>
  <configuration>
    <activeRecipes>
      <recipe>org.occurrent.UpgradeToOccurrent_0_30</recipe>
    </activeRecipes>
  </configuration>
  <dependencies>
    <dependency>
      <groupId>org.occurrent</groupId>
      <artifactId>occurrent-rewrite</artifactId>
      <version>0.30.0</version>
    </dependency>
  </dependencies>
</plugin>
```

Then run:

```
mvn org.openrewrite.maven:rewrite-maven-plugin:run
```

`UpgradeToOccurrent_0_30` composes three recipes:

* `org.occurrent.MigrateCoordinates_0_30`, the artifact-coordinate rewrite from section 1. Safe to run and commit without review.
* `org.occurrent.MigrateOccurrentRenames_0_30`, the pure renames. Safe to run and commit without review.
* `org.occurrent.MigrateStreamToList_0_30`, the `Stream` to `List` migration. It rewrites what it can prove is safe and leaves the rest for you (bucket 2 below).

The renames cover the `SubscriptionPosition` to `Checkpoint` family (about 16 types, plus method renames like `StartAt.subscriptionPosition` to `StartAt.checkpoint` and `globalSubscriptionPosition` to `globalCheckpoint`), `PolicySideEffect` to `SideEffect` (`executePolicy` to `executeSideEffect`), and `OccurrentSubscriptionFilter` to `StreamSubscriptionFilter`. It also fixes two package moves: `ExecuteFilter` moves from `...application.service.blocking` to `...application.service`, and `OccurrentProperties` moves from `...springboot.mongo.blocking` to `...springboot.mongo.common`. The recipe is the authoritative list, this is a summary rather than the full set.

The renames run on Kotlin too, both the type renames and instance-method renames. One case does not carry over: a call to the static factory `StartAt.subscriptionPosition(...)` is left untouched in Kotlin, so change it to `StartAt.checkpoint(...)` by hand.

## 3. Manual: the `Stream` to `List` write side

The write side of the API moved from `Stream`/`Sequence` to `List`. A decider's domain function is now `Function<List<E>, List<E>>` (was `Function<Stream<E>, Stream<E>>`), `EventStore.write` takes `List<CloudEvent>`, `CloudEventConverter.toCloudEvents` returns `List`, the view DSL's `evolve*` fold helpers gained `List` and `Iterable` overloads while keeping their `Stream` (Java) and `Sequence` (Kotlin) forms (applying events to a view is read-side, so those stay), and command composition uses `ListCommandComposition` (`StreamCommandComposition` and `CommandConversion` are gone). Reads stay lazy and are unaffected.

`MigrateStreamToList_0_30` rewrites the safe cases: call sites that only pass a `Stream` through, and signatures with no method body to reinterpret. It cannot rewrite a lambda body that calls `Stream` operations like `.filter()` or `.map()` on the events, since turning that into `List` code is a judgment call OpenRewrite can't make safely. Anything like that is left in place for you to fix by hand, and the compiler will point you at every remaining spot once you've run the recipe.

Rationale for the change is in [ADR 54](../architecture/decisions/0054-list-instead-of-stream-for-event-store-writes.md).

### Kotlin: what stays manual

The renames in group 1 cover Kotlin, but the `Stream` to `List` rewrites above run on Java only, so a Kotlin write site is manual. These removed Kotlin extensions are not automated either. Fix them with the compiler's help:

* `executeSequence` and `executeList` are removed. Call `execute { events: List<E> -> ... }` instead.
* `sideEffectOnSequence` is renamed to `sideEffectOnList`.
* The `write(String, Sequence<CloudEvent>)` extensions are removed. Use the `List` overload.
* The module DSL's `command(... Sequence ...)` overloads are gone. Use the `List` overload.
* The top-level `executePolicies` and `andThenExecutePolicy` extensions are removed along with the `PolicySideEffect` name. There is no `SideEffect`-named replacement for these two functions, so rework those call sites by hand. The synchronous side effect now lives on the DSL as `ExecuteOptions.sideEffect(...)`.

### Subscriptions: the annotation and DSL split

0.30.0 splits subscriptions into three forms: `@StreamSubscription` and `@DcbSubscription` for the capability-scoped cases, and a revived capability-neutral `@Subscription`. The recipe does not migrate these, so a 0.20.5 application that uses the following needs a manual pass:

* `@Subscription` no longer has `startAtTimeEpochMillis` or `startAtISO8601`. A neutral subscription can't honor a specific historical start time, so those attributes moved to `@StreamSubscription`. If you start a subscription from a point in time, change the annotation to `@StreamSubscription`.
* On the neutral `@Subscription`, the start constant `StartPosition.BEGINNING_OF_TIME` is now `StartPosition.BEGINNING`. The new `@StreamSubscription` keeps its own `StartPosition.BEGINNING_OF_TIME`, so only the neutral annotation's constant changed name.
* In the Kotlin DSL, `subscriptions { }` is now the neutral form and its `subscribe(filter = ...)` takes a capability-neutral filter. Renaming `OccurrentSubscriptionFilter` to `StreamSubscriptionFilter` (which the recipe does) is not enough to make a filtered `subscriptions { }` block compile, because the neutral builder no longer accepts a stream filter. Move those blocks to `streamSubscriptions { }`.
* On the reactive stack, `ReactorDurableSubscriptionModel` was redesigned around the `Subscribable` lifecycle. Its old `subscribe(subscriptionId, action)` and `subscribe(subscriptionId, filter, action)` methods that returned `Mono<Void>`, and `findStartAtForSubscription`, are gone. Use `subscribe(subscriptionId, filter, startAt, action)`, which returns a `Subscription`. See [ADR 44](../architecture/decisions/0044-reactive-spring-boot-starter.md).

## 4. Read before you upgrade

No code transform applies here. These are runtime and behavioral changes.

* **Java 21 is now required** (was Java 17). Your stored data is unaffected, this is only about the JVM you run on.
* **Integration tests now run against MongoDB 8.0** (was 4.2). One behavior change worth knowing: combining a `$natural` sort step with other sort steps now throws `IllegalArgumentException`, because MongoDB 7.0 and later reject that combination server side.
* **`Decider.compose` now requires at least two deciders.** It throws `IllegalArgumentException` for zero or one, where it previously passed through as a no-op.
* **Blocking catch-up now fails loudly.** If the resume token is unavailable after a long replay, it throws `IllegalStateException` instead of silently dropping events.
* **DCB stays opt-in.** The default event store capability is still `STREAM`, including in the Spring Boot starters, where it's controlled by `occurrent.event-store.capabilities`. If your application only uses streams today, it keeps behaving exactly the same after the upgrade.
* **New stores get a global position by default, existing ones need a look.** A global, monotonically increasing `position` now exists for stream events too, on by default for new stores. If you're upgrading a deployment that already has events in it, those existing events have no `position`, only new ones will. A MongoDB store detects an existing position-less collection at startup and turns position off for itself, logging how to enable it, so the upgrade won't trigger a surprise index build on your existing data. If you want position-based catch-up against that existing data, follow [the position-backfill runbook](../runbooks/position-backfill.md), which walks through creating the index, seeding the counter, and running the [position-backfill tool](../../eventstore/migration/position-backfill/README.md).
* **Default subscription ids change, which can orphan an existing checkpoint.** The subscription DSL now derives a default subscription id from the CloudEvent type instead of the domain event class's simple name. If you use the reified `subscribe<MyEvent> { }` form, or `@Subscription` without an explicit `id`, the id it computes changes on upgrade: with the default reflection-based `CloudEventTypeMapper` it becomes the fully-qualified class name rather than the simple name. A changed id no longer matches the stored checkpoint, so that subscription starts over from its configured start position after the upgrade. Before upgrading, set the id explicitly (pass a subscription id, or set `id` on the annotation) for any subscription that relied on the derived default, or configure a stable custom `CloudEventTypeMapper`. Subscriptions with an explicit id are unaffected.
