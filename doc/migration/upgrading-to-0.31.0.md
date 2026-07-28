# Upgrading to Occurrent 0.31.0

0.31.0 has four breaking changes. First, the `ResumeBehavior` and `StartupMode` enums move out of `@Subscription`,
`@StreamSubscription`, and `@DcbSubscription` and become shared top-level types. `Subscription.StartPosition`
and `DcbSubscription.DcbStartPosition` move the same way, to a shared top-level `org.occurrent.annotation.StartPosition`
(the constants are unchanged). Second, the four subscription checkpoint-storage modules are renamed from
`-position-storage` to `-checkpoint-storage`. Third, `EventMetadata` moves from `org.occurrent.dsl.subscription.EventMetadata`
to `org.occurrent.cloudevents.EventMetadata` and is rewritten from a Kotlin `data class` to a plain Java class. Fourth,
the Spring Boot annotation machinery moves from `org.occurrent.springboot.mongo.common` to `org.occurrent.springboot.common`,
with a matching module coordinate rename. One OpenRewrite recipe handles all four rewrites for you.

## 1. Run the recipe

Add the `rewrite-maven-plugin` and point it at the umbrella recipe, `org.occurrent.UpgradeToOccurrent_0_31`:

```xml
<plugin>
  <groupId>org.openrewrite.maven</groupId>
  <artifactId>rewrite-maven-plugin</artifactId>
  <version><!-- use the latest rewrite-maven-plugin release --></version>
  <configuration>
    <activeRecipes>
      <recipe>org.occurrent.UpgradeToOccurrent_0_31</recipe>
    </activeRecipes>
  </configuration>
  <dependencies>
    <dependency>
      <groupId>org.occurrent</groupId>
      <artifactId>occurrent-rewrite</artifactId>
      <version>0.31.0</version>
    </dependency>
  </dependencies>
</plugin>
```

Then run:

```
mvn org.openrewrite.maven:rewrite-maven-plugin:run
```

`UpgradeToOccurrent_0_31` composes `org.occurrent.MigrateOccurrentRenames_0_31`, which rewrites every reference to
a nested `ResumeBehavior`/`StartupMode`, for example `Subscription.ResumeBehavior`, `StreamSubscription.StartupMode`,
`DcbSubscription.ResumeBehavior`, or `Projection.StartupMode`, to the shared top-level
`org.occurrent.annotation.ResumeBehavior`/`org.occurrent.annotation.StartupMode`. It also rewrites
`Subscription.StartPosition` and `DcbSubscription.DcbStartPosition` to the shared top-level
`org.occurrent.annotation.StartPosition`, and rewrites every reference, import, and static import of
`EventMetadata` from its old package to the new one (see section 4). This same recipe also rewrites the Spring Boot
annotation machinery (`OccurrentProperties` and friends) from `org.occurrent.springboot.mongo.common` to
`org.occurrent.springboot.common` (see section 5). It also composes `org.occurrent.MigrateCoordinates_0_31`, which
renames the four checkpoint-storage dependency coordinates (see section 3) and the Spring Boot autoconfigure
module coordinate, from `occurrent-mongodb-spring-boot-autoconfigure` to `occurrent-spring-boot-autoconfigure`
(see section 5), in your Maven and Gradle build files. Safe to run and commit without review.

One entry in that list will never match your code. `Projection.StartupMode` and `Projection.ResumeBehavior` are a
nested shape that only ever existed in pre-release snapshots, since `@Projection` itself is new in 0.31.0. They are
covered for anyone who tracked a snapshot build, and are harmless if you are coming from the released 0.30.0.

## 2. What changed

`ResumeBehavior` (`SAME_AS_START_AT`, `DEFAULT`) and `StartupMode` (`DEFAULT`, `WAIT_UNTIL_STARTED`, `BACKGROUND`)
used to be declared separately, and identically, inside each of the four annotations. They are now one
`ResumeBehavior` and one `StartupMode`, both in `org.occurrent.annotation`, shared by all four. The constants, their
names, and what they mean are unchanged, only the enclosing type moved, so this is otherwise a drop-in upgrade with
no behavioral change. Rationale is in [ADR 60](../architecture/decisions/0060-unify-resumebehavior-and-startupmode-enums.md).

`resumeBehavior()` and `startupMode()` on all four annotations are unchanged in name and default. Only their
declared return type changed, from a nested enum to the shared one.

`Subscription.StartPosition` and `DcbSubscription.DcbStartPosition` also move, to a single shared
`org.occurrent.annotation.StartPosition` (`BEGINNING`, `NOW`, `DEFAULT`, unchanged). `@Projection` and `@Snapshot`
are new in 0.31.0 and use this shared `StartPosition` from the start. `StreamSubscription.StartPosition` is untouched: its constant is
`BEGINNING_OF_TIME`, not `BEGINNING`, a genuinely different start position over wall-clock time rather than the
unified global or DCB position, so it stays nested and annotation-specific.

## 3. Checkpoint-storage module coordinates

0.30.0 renamed the `SubscriptionPosition` type family to `Checkpoint` (see [ADR 46](../architecture/decisions/0046-rename-subscription-position-to-checkpoint.md)), including the storage adapter classes, but left the four modules that ship those adapters named `-position-storage`. 0.31.0 renames the coordinates to match the `CheckpointStorage` type each ships. The `org.occurrent` groupId, the packages, and the classes are unchanged, so this is a coordinate-only change, and the recipe from section 1 rewrites it for Maven and Gradle. Rationale is in [ADR 65](../architecture/decisions/0065-rename-checkpoint-storage-module-coordinates.md).

| Old artifactId | New artifactId |
|---|---|
| `occurrent-subscription-mongodb-native-blocking-position-storage` | `occurrent-subscription-mongodb-native-blocking-checkpoint-storage` |
| `occurrent-subscription-mongodb-spring-blocking-position-storage` | `occurrent-subscription-mongodb-spring-blocking-checkpoint-storage` |
| `occurrent-subscription-mongodb-spring-reactor-position-storage` | `occurrent-subscription-mongodb-spring-reactor-checkpoint-storage` |
| `occurrent-subscription-redis-spring-blocking-position-storage` | `occurrent-subscription-redis-spring-blocking-checkpoint-storage` |

## 4. EventMetadata moves to cloudevents-extension

`EventMetadata` moves from `org.occurrent.dsl.subscription.EventMetadata` (module `dsl/subscription-dsl/common`) to
`org.occurrent.cloudevents.EventMetadata` (module `cloudevents-extension`). It is also rewritten from a Kotlin
`data class` to a plain Java class, so the Kotlin-only surface (reified `get<T>`, operator `get`, `copy`) is dropped.
The typed accessors you actually fold events with, `getStreamId()`, `getStreamVersion()`, `getPosition()`,
`getData()`, the static `empty()`, and the static `from(CloudEvent)`, are unchanged in name and behavior, so this is
otherwise a drop-in upgrade. `DcbEventMetadata` stays in `dsl/dcb-dsl/common` and only its import of `EventMetadata`
changes. Rationale is in [ADR 71](../architecture/decisions/0071-relocate-eventmetadata-to-cloudevents-extension.md).

## 5. Spring Boot annotation machinery moves to a store-neutral module

The Spring Boot annotation machinery moves from `org.occurrent.springboot.mongo.common` to
`org.occurrent.springboot.common`: `OccurrentProperties`, `SubscriptionAnnotations`,
`Jackson3CloudEventConverterConfiguration`, and the five autoconfiguration conditions
(`OnDcbEventStoreCapabilityCondition`, `OnDomainEventQueriesCapabilityCondition`,
`OnMissingCloudEventConverterAndCloudEventTypeMapperCondition`, `OnPositionEnabledCondition`, and
`OnStreamEventStoreCapabilityCondition`). The module coordinate moves the same way, from
`org.occurrent:occurrent-mongodb-spring-boot-autoconfigure` to `org.occurrent:occurrent-spring-boot-autoconfigure`.
None of this was ever MongoDB-specific, it only lived in the MongoDB autoconfigure module because that was the only
store with Spring Boot support at the time (issue #409).

Property keys are unchanged. `OccurrentProperties` is annotated with a hard-coded
`@ConfigurationProperties(prefix = "occurrent")`, so no `application.yml` edit is needed and IDE completion still
works. The types themselves are identical apart from their package, so this is otherwise a drop-in upgrade. The
recipe from section 1 rewrites the type references and the module coordinate for Maven and Gradle.

## 6. If the recipe cannot reach a reference

The recipe rewrites source references it can see. A reference produced only in a compiled `.class` from 0.30.0
(a binary dependency, not source you can run the recipe over) needs a rebuild against 0.31.0 instead, since the
nested types no longer exist to link against. A Javadoc `{@link org.occurrent.dsl.subscription.EventMetadata}`
reference is also outside the recipe's reach and needs a manual fix to point at the new package.
