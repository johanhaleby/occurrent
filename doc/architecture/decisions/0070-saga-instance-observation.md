# 70. Observing saga instances

Date: 2026-07-25

## Status

Accepted

Refines [ADR 63](0063-saga-dsl.md).

## Context

ADR 63 put a saga's timers in its own state envelope rather than in JobRunr, and accepted a named cost for it: "no
JobRunr dashboard/retry surface for saga timers specifically". Nothing replaced that surface, so a deployment running
sagas had no supported way to answer an operational question. Two in particular:

- Is instance X still running, and which step is it on?
- Which instances have stopped moving, so a timer that should have fired has not?

`SagaStateStore` could answer neither. It offers `find(sagaId)`, `compareAndSave`, `findWithDueTimers(now, limit)` and
`delete(sagaId)`. The first needs an id the asker does not have when the question is "which instances are stuck", and the
third is the poller's own query, scoped to instances with a *due* timer.

Reading the collection directly is not an alternative. ADR 63's G5 carve-out declares the flow-state bookkeeping fields
non-wire-format, so the document shape is deliberately not a contract, and a flow saga's received log persists as
CloudEvent JSON strings, which is hostile to ad-hoc querying.

There was also a shape problem. `SagaEnvelope` is a record, so all ten components are public accessors. Six describe the
instance's lifecycle; four (`version`, `timers`, `streamWatermarks`, `positionWatermark`) exist only so the executor is
safe under at-least-once delivery. Issue #377 proposed narrowing that the way [ADR 63's](0063-saga-dsl.md) `FlowState`
was narrowed in the same spirit, and deferred it for want of a demonstrated need.

## Decision

Add an observation surface in three parts, and one enumeration method to the SPI to make it possible.

### `SagaInstance`, a narrow view the envelope implements

`SagaInstance` exposes `sagaId`, `status`, `isCompleted`, the three lifecycle timestamps, `nextTimerAt` and
`currentStep`. `SagaEnvelope` implements it and remains the `SagaStateStore` type.

**This is not the `FlowState` narrowing, and it is worth being precise about why.** `FlowState` worked because the
concrete `FlowStateImpl` could move into an `internal` package, putting the bookkeeping genuinely out of reach.
`SagaEnvelope` cannot move: the SPI both returns and accepts it, so relocating it would make the SPI name an internal
type. The interface therefore narrows what an observing caller is *handed*; it does not hide the envelope's components
from anyone holding one. That is a smaller claim than encapsulation, and it is only worth making because a facade
returns it on both execution paths. Had the Spring half been left out, the honest outcome would have been the
enumeration method alone and no new type.

The state `S` is excluded. A caller folding over process-internal state couples itself to how the process is written,
and a read model shaped for querying belongs in the projection DSL. `currentStep` is the one exception, because #377
named "which step is it on" as a trigger question and `FlowState` is already a narrow public interface, so reading
`currentStep` off it exposes nothing the flow layer does not already publish.

The interface is **not generic**. No member needs the state type, so a type parameter would only force callers to write
`SagaInstance<?>`.

`SagaEnvelope.Status` becomes a top-level `SagaStatus`, so the user-facing view does not name the store's envelope type
in its own signature. The persisted values are unchanged.

### `SagaStateStore.findByStatus(SagaStatus, Instant updatedBefore, int limit)`

One purpose-shaped method, not a query object or filter language. A query object would put a translation burden on every
store and invite a surface nobody asked for; two purpose-named methods (list-active and find-stalled) would be the same
query twice.

The contract is specified on the interface because correctness here is cross-store agreement, not any single
implementation:

- `updatedBefore` is exclusive. `Instant.now()` means "everything in this status"; `now` minus a threshold means
  "everything quiet for longer than that".
- Ascending by `updatedAt`, so the stalest instance arrives first. Descending would fill `limit` with the *least* stale
  of a stuck set and push the genuinely stuck past the end, which inverts the primary use case.
- `limit` is a bound, **not** a page. `updatedAt` persists at millisecond precision, so instances saved in one executor
  tick tie, and a cursor resuming from the last row's timestamp would silently drop the rest of a tie group. Correct
  paging needs `(updatedAt, sagaId)` and is not offered rather than offered wrongly.
- An instance with a null `updatedAt` is never returned. The executor always stamps it, so this only excludes a
  hand-built envelope, and it stops a store whose query engine skips a missing field from disagreeing with one that
  could read null as matching.

Unlike `findWithDueTimers` it reads whole instances, because `currentStep` cannot be answered without the state. That is
why `limit` is required rather than optional: enumerating flow-saga instances decodes their received logs.

The due-timer projection is widened to include the three timestamps. It omitted them, so its envelopes read null for all
three while the in-memory store returned them — invisible while only the poller consumed them, but a contract violation
once an envelope is also a `SagaInstance`.

### `SagaInstances`, reachable from both paths

A read-only facade over a store, returning `SagaInstance`. It offers nothing that writes: the executor owns instance
transitions, and a compare-and-set from outside it would race the subscription and the poller.

Programmatically it hangs off `SagaSubscription`. On the Spring stack sagas had no handle at all before: the registrar
kept its subscriptions private and the zero-config path built the store inline without registering it.

Spring gets **two** ways in, because the two lookups are genuinely different and neither subsumes the other:

- `SagaInstancesRegistry`, a normal `@Bean`, keyed by saga id. Being a bean definition it exists during refresh, so it
  is constructor-injectable, and it can enumerate the registered saga ids, which a dashboard needs so it does not have
  to hardcode them. It offers both a throwing `get(id)` and an `Optional`-returning `find(id)`: code holding a constant
  id has a bug when that id is unknown and should fail at the mistake (the message names every id that *is* registered,
  since "unknown saga id" with no list is a miserable error for a typo), while code resolving an id from a request has
  no bug when it misses.
- Each saga's `SagaInstances` published as a singleton named `sagaInstances-<id>`, so a `getBean` or `@Qualifier` lookup
  reaches one directly. Per id rather than one bean of the type, so two sagas do not make a by-type injection ambiguous.

**The registry is empty until the `@Saga` scan has run, and that is inherent rather than a defect.** A `@Saga` factory
can only be invoked once the beans it collaborates with are wired, which is after refresh, so the sagas genuinely do not
exist earlier. Injecting the registry into a constructor is fine; *reading* it from one is not. That constraint is
documented on the type instead of being papered over. It is not a practical limitation, because anything observing a
saga instance runs in response to a request, a schedule or a health check.

The per-id singleton keeps the sharper form of the same constraint: it is not a bean definition at all, so it cannot be
constructor-injected. `ObjectProvider`, `getBean`, or the registry all work.

The two paths are populated independently: the registry is a bean defined during refresh and so gets filled whatever
kind of context this is, whereas `registerSingleton` needs a `ConfigurableApplicationContext`. If that is missing (only
reachable from an exotic harness, since every Boot context is configurable) the registry still works, and the registrar
warns rather than failing a saga that is otherwise running.

The registry type lives in `dsl/saga-dsl/common` beside `SagaInstances`, not in a starter, because it is a Spring-free
map from saga id to `SagaInstances`; putting it in the starter would force a programmatic user wanting the same lookup to
depend on Spring Boot. Its `@Bean` goes in the **blocking** starter's autoconfiguration, not in
`spring-boot-autoconfigure-mongodb-common`: `@Saga` is blocking-only (the reactive starter has no saga registrar at
all), and that common module is for stack-neutral wiring the reactive starter also consumes. Keeping the `@Bean` beside
`SagaAnnotationRegistrar` also means [#409](https://github.com/johanhaleby/occurrent/issues/409) moves the two together
when the annotation machinery becomes store-neutral, rather than having to undo a placement in the common module.

### Why now rather than when an ops need appears

#377's own reasoning was that the view is additive whenever it is wanted. That holds for the interface and not for the
SPI method. Per the repository's release-status rule, an unreleased type may be reshaped with no migration path, and the
saga DSL is unreleased — so `findByStatus` is free today and breaks every out-of-tree `SagaStateStore` once it ships.
[Issue #411](https://github.com/johanhaleby/occurrent/issues/411) adds a SQL `SagaStateStore`, so defining the method
before a second implementation exists also avoids writing it twice.

## Consequences

- A saga's lifecycle is observable on both execution stacks without exposing the executor's bookkeeping, and a
  stuck-instance check is expressible: active instances not updated for longer than a threshold, stalest first.
- Every `SagaStateStore` implementation gains a method. Two in-tree stores and two test doubles, and out-of-tree
  implementors would break — acceptable only because the DSL is unreleased.
- Mongo gains a `{status, updatedAt}` index beside the due-timer one. Occurrent creates missing indexes and never
  removes them, so this is additive on an existing collection.
- The ordering and boundary contract is the likeliest place for a defect, since it must hold identically across stores
  that share no code. It is covered by one test body run against both, written so it lifts into the store TCK
  ([#395](https://github.com/johanhaleby/occurrent/issues/395)).
- Enumerating flow-saga instances decodes their received logs. `limit` is mandatory for that reason, and a caller
  wanting cheap counts should not use this method.
- One residual of the projection issue is accepted rather than fixed: `findWithDueTimers` still omits the state, so
  `currentStep` reads null on an envelope it returns. Including the state would defeat the projection's entire purpose,
  and no user-facing path hands out a poller envelope, since `SagaInstances` only calls `find` and `findByStatus`. The
  envelope's javadoc says so at the accessor. A store TCK asserting "every returned envelope is a fully populated
  `SagaInstance`" would have to exempt the due-timer query.
- True paging is absent. A deployment with more instances in one status than a sensible `limit` cannot walk them all,
  and closing that needs a compound `(updatedAt, sagaId)` ordering.
- Spring has two entry points to keep current for one capability, and a saga id therefore appears in two places: the
  registry and a bean name. They are populated in one method so they cannot drift apart.
- Refresh timing is the one thing a Spring caller must understand: nothing observable exists until the `@Saga` scan has
  run. Reading the registry from a constructor yields an empty registry, and a `sagaInstances-<id>` constructor injection
  fails outright. Both are documented at the point of use, and no amount of wiring removes the underlying constraint,
  since a saga factory cannot precede its own collaborators.
- `SagaInstancesRegistry` is a mutable bean with a `register` method that is public only because the registrar sits in
  another module, the same compromise `FlowStateImpl` makes for the Mongo store. It rejects a duplicate id rather than
  silently keeping one of two sagas.
