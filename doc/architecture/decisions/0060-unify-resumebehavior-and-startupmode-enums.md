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

**`StartPosition` and `DcbStartPosition` were left nested at the time of this decision and were not part of the
original unification.** Unlike `ResumeBehavior`/`StartupMode`, they looked like non-duplicates: `Subscription.StartPosition`
uses `BEGINNING` while `StreamSubscription.StartPosition` uses `BEGINNING_OF_TIME`. See the amendment below: that
comparison was the wrong one, and `StartPosition` unifies too, just not with `StreamSubscription`.

## Amendment: StartPosition, Capability, and Mode also unify

The reasoning above for `ResumeBehavior`/`StartupMode` turned out to apply further than this ADR first assumed.
`@Projection` and `@Snapshot` each declared their own nested `Capability` (`AGNOSTIC`, `STREAM`) and `Mode`
(`ASYNC`, `SYNCHRONOUS`), identical in name, constants, and meaning between the two annotations, for the same
reason `ResumeBehavior`/`StartupMode` were identical across four annotations: nothing about a projection versus a
snapshot changes what capability scope or processing mode means. Both now move to shared top-level
`org.occurrent.annotation.Capability` and `org.occurrent.annotation.Mode`. Neither annotation shipped in a release
yet, so this is a plain pre-release cleanup with no migration to stage.

`StartPosition` gets the same treatment, and this time the earlier reasoning in this ADR was wrong. The original
decision treated `Subscription.StartPosition` and `DcbSubscription.DcbStartPosition` as non-duplicates, pointing at
`StreamSubscription.StartPosition`'s different constant (`BEGINNING_OF_TIME` instead of `BEGINNING`) as proof they
were not interchangeable. But `StreamSubscription` was never the comparison that mattered.
`Subscription.StartPosition`, `DcbSubscription.DcbStartPosition`, `Projection.StartPosition`, and
`Snapshot.StartPosition` all carry the exact same three constants, `BEGINNING`, `NOW`, `DEFAULT`, over their own
kind of unified position (the global position for `Subscription` and `Projection`, the DCB sequence position for
`DcbSubscription` and a DCB-scoped `Snapshot`). That is the same duplication this ADR already unified for
`ResumeBehavior`/`StartupMode`, just missed the first time. All four now share one top-level
`org.occurrent.annotation.StartPosition`. `StreamSubscription.StartPosition` stays nested exactly as this ADR
originally decided: `BEGINNING_OF_TIME` is a start position over wall-clock time, not over either unified position,
so it is a genuinely different type and unifying it would either drop that distinction or force a supertype nobody
asked for.

`Subscription` and `DcbSubscription` shipped in 0.30.0, so moving their `StartPosition` is a source- and
binary-breaking change for existing callers, same as the `ResumeBehavior`/`StartupMode` move. The
`org.occurrent.MigrateOccurrentRenames_0_31` recipe gained two more `ChangeType` entries covering
`Subscription.StartPosition` and `DcbSubscription.DcbStartPosition`, so the same `UpgradeToOccurrent_0_31` recipe run
described above covers this too. `Projection.StartPosition` and `Snapshot.StartPosition` need no recipe entry: like
`Capability` and `Mode`, neither annotation has shipped yet.

The bean post-processors gain the same payoff described above for `ResumeBehavior`/`StartupMode`: with one
`StartPosition` shared across `Subscription`, `DcbSubscription`, `Projection`, and `Snapshot`, the
`toAgnosticStartPosition`/`toDcbStartPosition` converter pairs that bridged `Projection`'s and `Snapshot`'s own
`StartPosition` onto `Subscription`'s and `DcbSubscription`'s have nothing left to convert between, so they are
removed, and the call sites pass the annotation's `startAt()` straight through.

Separately, and unrelated to this unification, `@Projection.startAtPosition()` and `@Snapshot.startAtPosition()`
are renamed to `startAtGlobalPosition()` to match the released `@Subscription.startAtGlobalPosition()`. Neither
annotation has shipped, so this is a pre-release rename with no recipe or migration note of its own.

## Consequences

- Callers on 0.30.0 that reference a nested `ResumeBehavior` or `StartupMode` must move to the top-level
  `org.occurrent.annotation.ResumeBehavior`/`StartupMode`, either by running `UpgradeToOccurrent_0_31` or by hand.
  Everywhere else the change is behaviorally inert: the constants and their semantics are exactly what 0.30.0
  shipped.
- `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, and `@Projection` keep their own `resumeBehavior()`
  and `startupMode()` attributes, only the enum type each attribute returns is now shared.
- A future fifth annotation that needs resume/startup semantics reuses the shared enums directly instead of adding
  a fifth nested copy and a fourth converter pair.
