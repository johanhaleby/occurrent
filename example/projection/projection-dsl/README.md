# Projection DSL examples

Four small, tested vignettes showing the higher-level projection DSL: one call that both creates the subscription
and keeps a read model up to date. The read model is the pure fold (`View`); the `Projection` (or `DcbProjection`)
couples that fold with the events that feed it and the id that keys each instance, and a runner turns it into a live,
subscription-fed read model or an on-demand query fold. It is the read-side mirror of `DcbDecider` on the write side.

Every vignette builds its own `InMemoryEventStore` plus `InMemorySubscriptionModel` in its test. No Spring, no Docker:
this module is a demonstration, not an application.

## The four vignettes

| Vignette | Language | Capability | What it shows |
|---|---|---|---|
| [`streamjava`](src/main/java/org/occurrent/example/projection/dsl/streamjava) | Java | stream | The Java handler builder (`Projection.builder(...).on(Class, ...)`) feeding a materialized view over a plain map, filter derived from the registered handler types. |
| [`dcbjava`](src/main/java/org/occurrent/example/projection/dsl/dcbjava) | Java | DCB | A tag-scoped single-instance read model ("is this coupon redeemed?"), run both push and pull from one descriptor. |
| [`streamkotlin`](src/main/kotlin/org/occurrent/example/projection/dsl/streamkotlin) | Kotlin | stream | The Kotlin `projection { on<T> { } }` DSL, and an explicit `Filter` selecting on more than event type (here the CloudEvent subject) to scope a read model to one user. |
| [`dcbkotlin`](src/main/kotlin/org/occurrent/example/projection/dsl/dcbkotlin) | Kotlin | DCB | Issue [#194](https://github.com/johanhaleby/occurrent/issues/194) verbatim: `dcbProjection { tags(...); on<T> { } }` as `isUsernameClaimedProjection(username)`, run push and pull. |

## The ideas worth calling out

**The handlers are the filter.** You register a fold per event type, and that set of types becomes the subscription's
selector. There is no second list of event types to keep in sync with the fold, which is the duplication a hand-written
subscription usually carries. An explicit `Filter` (the `streamkotlin` vignette) overrides that when you need to select
on subject, source, data, or time rather than only type.

**One descriptor, push or pull.** The same `Projection` runs two ways. Push it through a subscription and it maintains a
stored, eventually-consistent read model. Fold it over a query (`DomainEventQueries`/`DcbDomainEventQueries`) and it
answers a question strongly-consistently, on demand, without a stored view. The `dcbjava` and `dcbkotlin` vignettes show
the same descriptor both ways and assert they agree.

**DCB scopes the read with a tag.** A `DcbProjection` carries a `DcbCriteria`, so `tags("coupon:$code")` or
`tags("username:$name")` reads only the events that ever mentioned that one key. The fold ignores event types it does
not handle, so a broad read boundary stays correct; it just does a little more folding.

**Id derivation is a typed accessor, not reflection.** Each vignette derives the view-instance id straight from the
event (`OrderEvent::orderId`, `it.userId`), replacing the reflection-based id extraction that this DSL was built to
retire.

Each vignette is one projection plus a JUnit 5 + AssertJ test covering the materialized result and, for the DCB ones,
the unclaimed/initial-state path.
