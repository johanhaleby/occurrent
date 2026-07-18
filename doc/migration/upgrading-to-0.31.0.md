# Upgrading to Occurrent 0.31.0

0.31.0 has one breaking change: the `ResumeBehavior` and `StartupMode` enums move out of `@Subscription`,
`@StreamSubscription`, `@DcbSubscription`, and `@Projection` and become shared top-level types. `Subscription.StartPosition`
and `DcbSubscription.DcbStartPosition` move the same way, to a shared top-level `org.occurrent.annotation.StartPosition`
(the constants are unchanged). An OpenRewrite recipe handles the rewrite for you.

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
`org.occurrent.annotation.StartPosition`. This covers a fully-qualified reference, an import, and a static import.
Safe to run and commit without review.

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
already used this same shared `StartPosition`. `StreamSubscription.StartPosition` is untouched: its constant is
`BEGINNING_OF_TIME`, not `BEGINNING`, a genuinely different start position over wall-clock time rather than the
unified global or DCB position, so it stays nested and annotation-specific.

## 3. If the recipe cannot reach a reference

The recipe rewrites source references it can see. A reference produced only in a compiled `.class` from 0.30.0
(a binary dependency, not source you can run the recipe over) needs a rebuild against 0.31.0 instead, since the
nested types no longer exist to link against.
