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

### `findByStatus`, on an optional `SagaStateStoreQueries` capability

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

**It is an optional capability, not part of the core `SagaStateStore`.** The executor never calls it: running a saga needs
only `find`, `compareAndSave` and `findWithDueTimers`. AGENTS.md requires public APIs to be small capability interfaces
composed together, and this repository already does exactly that for the analogous case, where `EventStore` carries the
minimal contract and `EventStoreQueries`, `EventStoreOperations` and `ReadEventStreamWithFilter` are optional capabilities
layered on. `SagaStateStoreQueries` follows that convention, standalone rather than extending the store, and is named for
the store it extends the way `EventStoreQueries` is.

An earlier draft of this ADR put the method on the core interface and only ever argued about its *shape*, never about
where it belonged. The objection that any store able to do `findWithDueTimers` can surely do `findByStatus` is wrong, and
the reason is instructive: `findWithDueTimers` may return instances in **any order at all** — the in-memory store iterates
a hash map and breaks at `limit` — while `findByStatus` demands ascending `updatedAt`. Ordering is a genuinely new demand
on a store, which is precisely why the in-memory implementation had to grow a sort. A store can serve the executor
faithfully and still be unable to enumerate.

`SagaInstances` therefore takes a plain `SagaStateStore` and checks for the capability when enumeration is *attempted*,
not at construction: a store that only lacks enumeration can still answer every by-id question, and refusing to build the
facade would deny it that. The failure names the store's class and the interface it must implement, following the
filtered-read precedent in `GenericApplicationService`. This also dissolves a risk an earlier version of this ADR listed:
adding the method no longer breaks an out-of-tree store, and the SQL store in
[#411](https://github.com/johanhaleby/occurrent/issues/411) can ship without observation support.

### Observation reads no saga state

Both enumeration queries project the state away, and `currentStep` is denormalized to a top-level document field written
on save — exactly as `nextTimerFiresAt` already was, for exactly the same reason: so a query need not decode a structure
to answer one derived question about it. `findWithDueTimers` avoided decoding `timers` that way; `findByStatus` now avoids
decoding a flow saga's received log.

This is what makes the governing invariant true: **every envelope a store returns answers every `SagaInstance` member.**
Not the broader "no envelope differs between stores" — that is false and is not claimed, since `streamWatermarks` and
`positionWatermark` still differ on the due-timer path, being executor bookkeeping the poller does not read. The narrow
rule is the one a TCK should assert. `state()` is deliberately outside it: it is not a `SagaInstance` member, which is
exactly why the enumerations are free to project it away, and `find(sagaId)` remains the way to get it.

Three problems disappear together as a result, which is why this was worth a storage-format change rather than an
API-only one:

- The cost. Enumeration is now a bounded indexed read whose price does not scale with how much history each instance
  carries. An earlier version warned callers not to poll `findByStatus` at subscription frequency because 100 instances
  against the default 100-event history window meant on the order of ten thousand CloudEvent deserializations. That
  warning is gone because the cost is.
- The failure mode. Because no state is decoded, an instance whose state can no longer be read — a received event whose
  class was renamed away, a state document that no longer matches its type — is simply reported with its lifecycle
  intact. A previous iteration caught the decode failure per instance and degraded to a null state with a warning; that
  path is deleted rather than kept, because there is no longer anything to tolerate. `find(sagaId)` still throws on such
  an instance, which is correct: the executor loads one in order to fold and save it, and a silently null state there
  would restart the process from its initial state and re-dispatch its commands.
- The carve-out. `currentStep` used to be derived from `state` on access, so a poll that projected state away could not
  answer it, and "every envelope is a fully populated `SagaInstance`" held only with an exemption for the due-timer
  query. There is no exemption now.

`currentStep` becoming a real record component introduces the one hazard a denormalized duplicate always has: it can
drift from `state.currentStep()`. That is prevented structurally rather than by discipline. `SagaEnvelope`'s compact
constructor re-derives `currentStep` from `state` whenever the state is present and honours a passed value only when the
state is `null` — which is exactly and only the projected-read case a store needs it for. A caller cannot construct an
envelope whose `currentStep` disagrees with its state, because a record compact constructor can reassign the component
before it is bound to the field, the same mechanism the existing `List.copyOf(timers)` defensive copy already uses.

Projecting the *nested* `state.currentStep` instead was rejected as strictly worse rather than a middle ground: the read
path would reconstruct a `FlowStateImpl` with a silently empty `received()` and a defaulted `windowStart`, which is a
corrupted state object rather than an absent one.

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

  **It is a read-only interface, with the write side on an implementation in `org.occurrent.dsl.saga.internal`.** The
  registry is populated by the `@Saga` registrar and by nothing else: unlike `SagaStateStore.compareAndSave`, which an
  application legitimately calls when it wires its own store, `register` has no legitimate application caller at all.
  Occurrent's callers are unknown and unobservable, so the standing rule is to prevent a footgun at the type level rather
  than document it — a public `register` on a bean applications are told to inject is exactly the footgun, and it would
  contradict the guarantee `SagaInstances` makes two paragraphs up. Splitting it is how ADR 63's `FlowState` handled the
  same problem, and the `internal` package is what carries the not-user-API signal; a public mutable type in a public
  package carries no signal at all. The starter constructs the implementation, declares the `@Bean` as the interface, and
  resolves the concrete type when it needs to write, so replacing the bean with your own implementation leaves it
  unpopulated and logs a warning rather than failing a saga.
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
- No `SagaStateStore` implementation is obliged to change. Enumeration is an optional capability, so an existing or
  out-of-tree store keeps compiling and simply cannot be enumerated; both in-tree stores opt in, and neither test double
  needed to, which is the clearest evidence the split was at the right altitude.
- Mongo gains a `{status, updatedAt}` index beside the due-timer one. Occurrent creates missing indexes and never
  removes them, so this is additive on an existing collection.
- The ordering and boundary contract is the likeliest place for a defect, since it must hold identically across stores
  that share no code. It is covered against both stores, but the assertions are **hand-duplicated** between
  `InMemorySagaStateStoreTest` and `SpringMongoSagaStateStoreMongoTest` rather than shared: there is no test-jar
  dependency between the modules. Both files say so in a comment. Sharing one body waits on the store TCK
  ([#395](https://github.com/johanhaleby/occurrent/issues/395)), and until then a contract change has to be made twice.
- The exclusive `updatedBefore` boundary holds in *direction* on every store but not at a common *resolution*: Mongo
  persists `updatedAt` as epoch millis and compares truncated values, while the in-memory store compares whole
  `Instant`s and the executor stamps a possibly sub-millisecond one. No store is more inclusive than the boundary, but an
  instance updated inside the same millisecond may be excluded. The javadoc says so, and a TCK asserting sub-millisecond
  boundary behaviour would fail on Mongo for reasons that are not defects.
- Enumeration reads no saga state, so its cost does not scale with per-instance history. `limit` still bounds the result
  set, but it no longer bounds a decode cost, and there is no longer a reason to avoid a periodic sweep.
- "Every envelope answers every `SagaInstance` member" holds without exemption, for the first time. A store TCK can
  assert it flatly across both enumeration methods. The price is a persisted derived field, `currentStep`, whose drift is
  prevented by the envelope's constructor rather than by convention.
- `SagaEnvelope` gained an eleventh component, so every construction site changed. Free to do only because the saga DSL
  is unreleased; the constructor re-derivation means almost every caller passes `null` and gets the right value anyway.
- The new `{status, updatedAt}` index is maintained on **every** `compareAndSave`, because `updatedAt` changes on every
  save. Saga writes therefore carry index-maintenance cost for a query that only observation uses. Accepted as the price
  of an indexed enumeration, but it is a write-path cost paid for a read-path feature, so a deployment that never observes
  still pays it.
- `createIndex` runs in the store's constructor, so on an existing large collection startup blocks while the index builds.
  This matches how the due-timer index already behaved, but it is now two indexes rather than one.
- True paging is absent. A deployment with more instances in one status than a sensible `limit` cannot walk them all,
  and closing that needs a compound `(updatedAt, sagaId)` ordering.
- Spring has two entry points to keep current for one capability, and a saga id therefore appears in two places: the
  registry and a bean name. They are populated in one method so they cannot drift apart.
- Refresh timing is the one thing a Spring caller must understand: nothing observable exists until the `@Saga` scan has
  run. Reading the registry from a constructor yields an empty registry, and a `sagaInstances-<id>` constructor injection
  fails outright. Both are documented at the point of use, and no amount of wiring removes the underlying constraint,
  since a saga factory cannot precede its own collaborators.
- The registry is two types rather than one: a read-only interface an application injects, and an internal
  implementation the framework writes to. The cost is an extra type and a concrete-type lookup in the registrar; the
  benefit is that no application can corrupt the registry, and the read-only guarantee is enforced by the compiler
  instead of by a comment. It rejects a duplicate id rather than silently keeping one of two sagas.
- Two Spring conflicts fail fast rather than degrading, on the principle that a footgun is prevented or made loud rather
  than documented. A `SagaInstancesRegistry` bean Occurrent cannot populate would report "no sagas" forever, so it is
  rejected at startup instead of warned about; a supported-looking extension point that cannot work is worse than none.
  And a pre-existing bean named `sagaInstances-<id>` is rejected with a message naming the saga and the name, because
  `registerSingleton` already failed there — with a bare duplicate-singleton error from inside
  `afterSingletonsInstantiated` that says nothing about sagas. Neither changes success into failure; both turn an opaque
  or silent failure into a diagnosable one. A registry that is simply absent (annotation processing off) still warns and
  carries on, since that is a legitimate configuration.
