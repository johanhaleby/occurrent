# occurrent-rewrite

OpenRewrite recipes that upgrade an application from one Occurrent release to the next. There is one umbrella recipe
per release. Run the one matching the version you are upgrading to, or run them in order if you are crossing several.

| Upgrading to | Recipe | Migration guide |
|---|---|---|
| 0.30.0 | `org.occurrent.UpgradeToOccurrent_0_30` | [upgrading to 0.30.0](../doc/migration/upgrading-to-0.30.0.md) |
| 0.31.0 | `org.occurrent.UpgradeToOccurrent_0_31` | [upgrading to 0.31.0](../doc/migration/upgrading-to-0.31.0.md) |
| 0.32.0 | `org.occurrent.UpgradeToOccurrent_0_32` | [upgrading to 0.32.0](../doc/migration/upgrading-to-0.32.0.md) |

## Run it

Add the `rewrite-maven-plugin` to the project you want to upgrade and point it at the umbrella recipe for your target
version:

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <!-- use the latest rewrite-maven-plugin release -->
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_32</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.32.0</version>
        </dependency>
    </dependencies>
</plugin>
```

Then run:

```
mvn rewrite:run
```

## 0.32.0

`org.occurrent.UpgradeToOccurrent_0_32` runs two things:

- `org.occurrent.MigrateOccurrentRenames_0_32` renames the reactor `org.occurrent.subscription.api.reactor.SubscriptionModel`
  to `FluxSubscriptionModel`, in Java and Kotlin. The old name now belongs to a new interface meaning what the blocking
  `SubscriptionModel` means, so the rename is what keeps your references pointing at the type you wrote them against. It
  matches on the fully qualified name, so the blocking `SubscriptionModel` is left alone.
- `org.occurrent.MigrateSubscriptionModeProperty_0_32`, described below.

`org.occurrent.MigrateSubscriptionModeProperty_0_32` rewrites the deprecated `occurrent.subscription.enabled`
configuration property to `occurrent.subscription.mode`, in `.properties` and `.yaml` alike. It is value-dependent,
`false` becoming `disabled` and `true` becoming `auto`, so a plain key rename would leave a value the new property
cannot bind.

It is not restricted to `application.properties` or `application.yml`, so it also reaches profile files and a `config/`
directory. Three cases it steps around rather than guessing at: a value it cannot read as a boolean, anything outside
your configuration files such as an environment variable, and a file that already sets both keys, where it drops the
deprecated one. The old property still works this release, so what it skips keeps running.

## 0.31.0

`org.occurrent.UpgradeToOccurrent_0_31` runs two things:

- `org.occurrent.MigrateOccurrentRenames_0_31` applies the type renames in that release: the `ResumeBehavior`,
  `StartupMode`, and `StartPosition` enums moving out of the annotations into top-level types in
  `org.occurrent.annotation`, the `EventMetadata` move to `org.occurrent.cloudevents`, and the Spring Boot annotation
  types moving to `org.occurrent.springboot.common`. Java and Kotlin both.
- `org.occurrent.MigrateCoordinates_0_31` renames the four checkpoint-storage artifacts from `-position-storage` to
  `-checkpoint-storage`, and the autoconfigure artifact to `occurrent-spring-boot-autoconfigure`, for Maven and Gradle.

## 0.30.0

`org.occurrent.UpgradeToOccurrent_0_30` runs two things:

- `org.occurrent.MigrateOccurrentRenames_0_30` applies the mechanical type, method, and package renames in that release (the `SubscriptionPosition` to `Checkpoint` family, `PolicySideEffect` to `SideEffect`, `OccurrentSubscriptionFilter` to `StreamSubscriptionFilter`, and the `ExecuteFilter` and `OccurrentProperties` package moves). It rewrites both Java and Kotlin, and is safe to run and commit.
- `org.occurrent.MigrateStreamToList_0_30` migrates the write side from `Stream` to `List`. This one only touches Java. It rewrites the cases it can prove safe (`Stream.of(...)` and `Stream.empty()` passed to `EventStore.write(...)`, and `StreamCommandComposition` to `ListCommandComposition`) and leaves a `TODO` comment on the call sites it cannot rewrite, such as a lambda body that runs `Stream` operations. Read those comments and finish them by hand.

Run the two separately if you want to review each step on its own. `MigrateOccurrentRenames_0_30` is the zero-risk
part, so you can land it first and tackle the `Stream` to `List` work after.

### What 0.30.0 does not cover

The recipe only touches source code. It does not handle the behavioral and operational changes in 0.30.0 (Java 21, MongoDB 8, `Decider.compose` needing at least two deciders, the fail-loud catch-up, and the global position backfill).

Kotlin gets the renames but not the write-side migration, so on Kotlin the `Stream` to `List` work is manual, as are the removed Kotlin extensions (`executeSequence`, `sideEffectOnSequence`, `write(Sequence)`, and the module-DSL `command(Sequence)` overloads). One rename also needs a manual touch in Kotlin: a call to the static factory `StartAt.subscriptionPosition(...)` is not rewritten, so change it to `StartAt.checkpoint(...)` yourself. The full checklist is in [the migration guide](../doc/migration/upgrading-to-0.30.0.md).
