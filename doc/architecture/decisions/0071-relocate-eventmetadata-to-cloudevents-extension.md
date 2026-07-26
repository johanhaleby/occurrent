# 71. Relocate EventMetadata to cloudevents-extension

Date: 2026-07-21

## Status

Accepted. Breaking for 0.30.0 callers, automated by the `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe.

## Context

`EventMetadata` (`org.occurrent.dsl.subscription.EventMetadata`, module `dsl/subscription-dsl/common`) started as a
subscription-delivery type: the bag of stream id, stream version, and position built from a CloudEvent's extensions
and handed to a subscriber alongside the event. [ADR 65](0065-first-grade-event-metadata-in-the-dsls.md) made it the shared fold currency for the saga DSL and the
projection/view DSLs too, so a saga reaction, a projection fold, and a view fold now all receive an `EventMetadata`,
none of them through the subscription DSL. The type outgrew the module it lives in: `dsl/subscription-dsl/common`
is pulled in by saga, projection, and DCB DSL modules purely for a type that is no longer subscription-specific, and
its name and package still claim it is.

The natural home is `cloudevents-extension`. That module already owns the CloudEvent extension keys `EventMetadata`
reads (`OccurrentCloudEventExtension`: streamid, streamversion, position), so the type moving there collapses a
cross-module dependency into a same-module one. `cloudevents-extension` is also foundational: the eventstore and
subscription core depend on it transitively, so every DSL module already has it on the classpath regardless of which
capability it targets.

`cloudevents-extension` is pure Java, and `EventMetadata` was a Kotlin `data class`. Moving it as-is would put
kotlin-stdlib on every consumer of that foundational module, including plain-Java applications that use nothing but
the blocking eventstore. That is a materially worse dependency footprint than the type is worth, so the move is paired
with a rewrite to a plain Java class rather than a package-only move.

`DcbEventMetadata` (`dsl/dcb-dsl/common`) is a different case. It wraps `EventMetadata` with the DCB position and
tags and is used only by DCB-DSL-adjacent code, so it is legitimately DCB-scoped and stays where it is. Only its
import of `EventMetadata` changes.

`SubscriptionFilters` is the other type in `dsl/subscription-dsl/common`. It is genuinely subscription-specific
(filtering criteria for the subscription DSL) and is unaffected by this move, so after this change it is the sole
remaining type in that module.

## Decision

**`EventMetadata` moves to `org.occurrent.cloudevents.EventMetadata` in the `cloudevents-extension` module, rewritten
as a plain Java class.** The Kotlin-only surface the data class carried (reified `get<T>`, operator `get`, and
`copy`) is dropped. It was essentially unused: callers read metadata through the typed accessors, not through generic
or copy-based access. The typed accessors are preserved with identical behavior: `getStreamId()`, `getStreamVersion()`,
`getPosition()`, `getData()`, the static `empty()`, and the static `from(CloudEvent)` factory.

**`DcbEventMetadata` stays in `dsl/dcb-dsl/common`.** Only its import of `EventMetadata` is updated to the new
package. Its own type and location are unaffected.

**`SubscriptionFilters` stays in `dsl/subscription-dsl/common`.** It remains the sole type in that module going
forward.

**The move is automated.** The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites every reference,
import, and static import from `org.occurrent.dsl.subscription.EventMetadata` to `org.occurrent.cloudevents.EventMetadata`.

## Consequences

- Modules that used `EventMetadata` only for the fold callback (saga DSL, projection DSL, view DSL) drop their
  dependency on `dsl/subscription-dsl/common`, since the type they need now lives in a module they already depend on
  transitively. The dependency graph gets strictly simpler, not more tangled.
- `cloudevents-extension` stays pure Java. No consumer of that foundational module picks up kotlin-stdlib as a side
  effect of this move.
- The dropped Kotlin sugar (reified `get<T>`, operator `get`, `copy`) has no replacement. A caller that used it
  switches to the typed accessors or to `EventMetadata.from(...)` to build a fresh instance. This is expected to
  affect very few callers, since the sugar was not exercised by any first-party DSL code.
- The OpenRewrite recipe rewrites source references, imports, and static imports. One thing it cannot reach is a
  Javadoc `{@link}` reference to the old FQN, which needs a manual fix where present.
- This is breaking for a type shipped in 0.30.0. See the changelog and the [upgrade guide](../../migration/upgrading-to-0.31.0.md)
  for caller-facing detail.
