# 60. Unify ResumeBehavior and StartupMode into shared top-level enums

Date: 2026-07-15

## Status

Accepted

## Context

`@Subscription`, `@StreamSubscription`, `@DcbSubscription`, and `@Projection` each declare their own nested
`ResumeBehavior` enum (`SAME_AS_START_AT`, `DEFAULT`) and `StartupMode` enum (`DEFAULT`, `WAIT_UNTIL_STARTED`,
`BACKGROUND`). The four copies are identical in name, constants, and meaning: `ResumeBehavior` picks between
resuming from a durable checkpoint and always starting from the annotation's configured start position, and
`StartupMode` picks whether the factory method blocks until the subscription is live or returns while it catches
up in the background. Nothing about DCB versus stream versus capability-agnostic subscriptions, or a subscription
versus a projection, changes what either enum means.

The duplication was not free. `OccurrentBlockingAnnotationBeanPostProcessor` and
`OccurrentReactiveAnnotationBeanPostProcessor` both back `@Projection`, which can register either a stream-style
or a DCB-style subscription depending on the annotated method's return type. Since `Projection.ResumeBehavior` and
`Projection.StartupMode` are their own types, distinct from `Subscription.ResumeBehavior`/`StartupMode` and from
`DcbSubscription.ResumeBehavior`/`StartupMode`, each bean post-processor carried a `toAgnosticResumeBehavior`/
`toAgnosticStartupMode` pair to bridge `Projection`'s enums onto `Subscription`'s, and a parallel
`toDcbResumeBehavior`/`toDcbStartupMode` pair to bridge them onto `DcbSubscription`'s. Four enums that mean the
same thing forced three converter method pairs and, further down, three `shouldWaitUntilStarted` overloads (one
per enum family) just to re-derive the same boolean from values that were never allowed to differ.

## Decision

**`ResumeBehavior` and `StartupMode` move out of the four annotations and become shared top-level types,
`org.occurrent.annotation.ResumeBehavior` and `org.occurrent.annotation.StartupMode`.** `@Subscription`,
`@StreamSubscription`, `@DcbSubscription`, and `@Projection` all reference the same two enums instead of declaring
their own. This is a plain deduplication: the constants, their names, and their meaning are unchanged, only the
enclosing type moves.

**The bean post-processors' per-annotation converter methods go away, and their `shouldWaitUntilStarted` overloads
now share one `StartupMode` parameter instead of one per enum family.** With one `ResumeBehavior` and one
`StartupMode` shared across every annotation, `toAgnosticResumeBehavior`, `toAgnosticStartupMode`,
`toDcbResumeBehavior`, and `toDcbStartupMode` have nothing left to convert between, so they are removed. The reactor
bean post-processor now needs only one `shouldWaitUntilStarted` method. The blocking bean post-processor still
overloads on its first parameter's shape, a `StartPositionToUse` for the catch-up path and a plain `boolean` for the
agnostic path, but both overloads now take the same shared `StartupMode` as their second parameter, so the
redundant per-enum copies are what collapses, not the overload count itself. The bean post-processors keep
everything else about how `@Projection` picks a stream-style or DCB-style subscription path, only the
enum-bridging plumbing goes away.

**This is a source- and binary-breaking change for 0.30.0 callers.** The four enums shipped nested (for example
`Subscription.ResumeBehavior`, `DcbSubscription.StartupMode`) in the released 0.30.0, so any reference to one of
those nested types, including a fully-qualified reference, a static import, or a compiled binary produced against
0.30.0, breaks against 0.31.0. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe (composing
`org.occurrent.MigrateOccurrentRenames_0_31`) rewrites the nested references to the top-level types automatically.
See the [upgrade guide](../../migration/upgrading-to-0.31.0.md).

**`StartPosition` and `DcbStartPosition` are left nested and are not part of this unification.** Unlike
`ResumeBehavior`/`StartupMode`, the two start-position enums are not duplicates: `Subscription.StartPosition` uses
`BEGINNING` while `StreamSubscription.StartPosition` uses `BEGINNING_OF_TIME`, and `DcbSubscription.DcbStartPosition`
carries its own DCB-specific constants. Unifying types whose constants genuinely differ per annotation would mean
either losing that distinction or building a supertype wide enough to cover all of them, which is not a
deduplication, it is a design change nobody asked for here. `StartPosition`/`DcbStartPosition` keep their existing
per-annotation shape.

## Consequences

- Callers on 0.30.0 that reference a nested `ResumeBehavior` or `StartupMode` must move to the top-level
  `org.occurrent.annotation.ResumeBehavior`/`StartupMode`, either by running `UpgradeToOccurrent_0_31` or by hand.
  Everywhere else the change is behaviorally inert: the constants and their semantics are exactly what 0.30.0
  shipped.
- `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, and `@Projection` keep their own `resumeBehavior()`
  and `startupMode()` attributes, only the enum type each attribute returns is now shared.
- A future fifth annotation that needs resume/startup semantics reuses the shared enums directly instead of adding
  a fifth nested copy and a fourth converter pair.
