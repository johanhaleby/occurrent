# occurrent-rewrite

OpenRewrite recipes that upgrade an application's source from Occurrent 0.20.5 to 0.30.0.

The umbrella recipe `org.occurrent.UpgradeToOccurrent_0_30` runs two things:

- `org.occurrent.MigrateOccurrentRenames_0_30` applies the mechanical type, method, and package renames in this release (the `SubscriptionPosition` to `Checkpoint` family, `PolicySideEffect` to `SideEffect`, `OccurrentSubscriptionFilter` to `StreamSubscriptionFilter`, and the `ExecuteFilter` and `OccurrentProperties` package moves). It rewrites both Java and Kotlin, and is safe to run and commit.
- `org.occurrent.MigrateStreamToList_0_30` migrates the write side from `Stream` to `List`. This one only touches Java. It rewrites the cases it can prove safe (`Stream.of(...)` and `Stream.empty()` passed to `EventStore.write(...)`, and `StreamCommandComposition` to `ListCommandComposition`) and leaves a `TODO` comment on the call sites it cannot rewrite, such as a lambda body that runs `Stream` operations. Read those comments and finish them by hand.

## Run it

Add the `rewrite-maven-plugin` to the project you want to upgrade and point it at the umbrella recipe:

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <!-- use the latest rewrite-maven-plugin release -->
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
mvn rewrite:run
```

Run the two recipes separately if you want to review each step on its own. `MigrateOccurrentRenames_0_30` is the zero-risk part, so you can land it first and tackle the `Stream` to `List` work after.

## What it does not cover

The recipe only touches source code. It does not handle the behavioral and operational changes in 0.30.0 (Java 21, MongoDB 8, `Decider.compose` needing at least two deciders, the fail-loud catch-up, and the global position backfill).

Kotlin gets the renames but not the write-side migration, so on Kotlin the `Stream` to `List` work is manual, as are the removed Kotlin extensions (`executeSequence`, `sideEffectOnSequence`, `write(Sequence)`, and the module-DSL `command(Sequence)` overloads). One rename also needs a manual touch in Kotlin: a call to the static factory `StartAt.subscriptionPosition(...)` is not rewritten, so change it to `StartAt.checkpoint(...)` yourself. The full checklist is in [the migration guide](../doc/migration/upgrading-to-0.30.0.md).
