# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

Four things are worth reading, one of them a compile-time break. At compile time, if you use the flow saga's
deprecated `join` or Kotlin's `expect<T>`, both are gone. Read
[section 1](#1-a-flow-sagas-join-kotlins-expectt-and-expectation-are-removed). A flow saga's `stepWindow` now
counts and evicts only the events its own steps declare, which most callers need to do nothing about. Read
[section 2](#2-a-flow-sagas-stepwindow-now-caps-only-its-own-declared-events). A projection, a subscription, a
query, or a snapshot that declares an event type whose concrete subtypes cannot be found is now refused, the same
refusal 0.33.0 already shipped for a saga and an annotation-based subscription. Read
[section 3](#3-a-projection-a-subscription-a-query-or-a-snapshot-declaring-a-supertype-event-is-refused). At
startup, if you set a MongoDB collection name, a MongoDB time representation, or whether a subscription restarts
after losing change-stream history, through `OccurrentProperties`, four configuration keys are deprecated and have
a recipe that rewrites them for you. Read [section 4](#4-four-mongodb-only-keys-move-under-mongodb).

## 1. A flow saga's `join`, Kotlin's `expect<T>` and `Expectation` are removed

`StepBuilder.join`, Kotlin's `expect<T>`/`join`, and the `Expectation` type are gone. `join` was already deprecated
in 0.33.0 in favor of `on(StepCondition, ...)` with `allOf(...)`, and that replacement is what every caller now
needs. An expectation of `n` events of a type becomes `event(type, n)`, and the whole list becomes one `allOf(...)`
tree:

Java, before and after:

```java
// Before
step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end());

// After
step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end());
```

Kotlin, before and after:

```kotlin
// Before
join(expect<PlayerReady>(2), then = end)

// After
on(allOf(event<PlayerReady>(2)), then = end)
```

`whenFulfilled`, or the trailing reaction lambda in Kotlin, carries over unchanged. It still reads
`ReceivedEvents`, not a single triggering event.

[ADR 125](../architecture/decisions/0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md)
had rejected removing `join` outright. No recipe covered it, so removal would have broken every caller with no
automated fix. That recipe now exists, so this release acts on the decision ADR 125 already reasoned through
rather than relitigating it. See [#707](https://github.com/johanhaleby/occurrent/issues/707) and
[#806](https://github.com/johanhaleby/occurrent/issues/806).

### Run the recipe

`UpgradeToOccurrent_0_34` rewrites the shapes it can prove, both `join` overloads.

A `join` call rewrites when its expecting argument is a literal `List.of(...)` or `Arrays.asList(...)`, and every
element of that list is itself a literal `Expectation.of(Class)` or `Expectation.of(Class, int)` call. The recipe
cannot see what a variable or a method call contains, so a call built either way is left alone.

Two expectations naming the same type collapse to the higher of their counts, the same way `join` itself always
did, but only when both counts are integer literals. A count that is a variable or an expression cannot be
compared at rewrite time, so a duplicate-typed pair with a non-literal count is also left alone.

Every call the recipe leaves alone stops compiling once `join` is removed, so the compiler finds it for you. Fix
it by hand using the Java example above, generalized to your own list contents.

The recipe is Java only. `expect<T>` is an inline reified Kotlin function with no call site left in the compiled
class to match, and Kotlin's `join` takes named arguments and a trailing lambda, syntax the Java-template
machinery behind this recipe cannot rewrite. Every Kotlin call site needs the by-hand translation below.

### By hand

Translate a Java call the recipe left alone, or any Kotlin call, using the shapes above. Two more cases are worth
naming directly.

A `join` built from a variable or a method call, rather than a literal list, translates the same way once you
have the list of expectations in front of you. Build the `StepCondition` tree from that same list by hand, and
`on(allOf(...), ...)` replaces `join(...)` exactly as it does above.

A duplicate-typed pair whose count is not a literal needs you to work out which count wins before you write the
`event(...)` leaf. `join(List.of(Expectation.of(Type.class, a), Expectation.of(Type.class, b)), ...)` always meant
whichever of `a` and `b` is larger, so `event(Type.class, Math.max(a, b))` is the direct translation in Java, and
the Kotlin equivalent reads the same way.

## 2. A flow saga's `stepWindow` now caps only its own declared events

No recipe, and most callers need to do nothing. This only matters if your flow sets a
`narrowingFilter`, a `replacementFilter` wider than the flow's own declared types, or uses a
`CloudEventTypeMapper` that collapses several domain types onto one CloudEvent type string.

The 0.33.0 upgrade guide's [section 9](upgrading-to-0.33.0.md#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in)
and [section 10's replacement-filter caveat](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)
describe `stepWindow` as it shipped in 0.33.0, where every correlated event counted toward the cap
regardless of whether any step declared its type. That let an event outside a flow's own declared
types evict one of the step's own events, and the absolute bound section 9 states,
`historyWindow + 2 * stepWindow + 1`, held because of that same defect.

`stepWindow` now counts and evicts only events of a type some step's `on(...)` branch or
window-condition leaf actually names. An event of any other type is still retained, never
discarded, but it no longer takes one of the cap's slots or evicts a declared event to make room
for itself. The bound in section 9 still holds for a flow's own declared-type events. It no longer
bounds a step fed only events of a type no step declares, which is not a new gap. It was always the
kind of growth `stepWindow` and `historyWindow` alone did not close, only masked. Watch the
0.33.0 store-boundary warning if your flow admits such events and you care about total document
size. See [ADR 129](../architecture/decisions/0129-a-flow-sagas-stepwindow-caps-only-its-own-declared-events.md)
for the full decision.

## 3. A projection, a subscription, a query, or a snapshot declaring a supertype event is refused

0.33.0 made a saga and an annotation-based subscription expand a declared sealed event type into the
concrete types it permits, and refuse a declared type whose concrete types cannot all be found ([section
10 of that guide](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)).
Four more places derived a type filter the same old way and did not get that fix. The projection DSL, the
subscription DSL's `filterFromEventTypes`, `DomainEventQueries` on both the blocking and reactor stacks, and
the Spring Boot starter's `@Snapshot` registrar on both stacks all kept the old derivation. 0.34.0 brings all
four in line with the saga and the annotation-based subscription. See [ADR 126](../architecture/decisions/0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md).

**Read this as a report about a projection, subscription, query, or snapshot that was already missing
events, not as a regression.** Under every type mapper Occurrent ships, a handler or a query keyed on a
sealed supertype was asking for that supertype's own CloudEvent type and nothing else, so it silently
matched fewer events than it looked like it should. This release either fixes that silently, by asking for
every concrete type the supertype permits, or refuses it loudly when the concrete types cannot all be
found, the same choice 0.33.0 already made for sagas and subscriptions.

`ProjectionFilters.filterFor` throws `IllegalArgumentException` naming the type, the first time a runner or a
query starts a projection and its filter is derived, `Projections.project(projection, queries)` included.
`SnapshotAnnotationRegistrar` throws the same shape when it registers the `@Snapshot`, at Spring Boot startup.
For example:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent
cannot all be enumerated, so a filter derived from it would miss some of them. Register the concrete event
types instead, make OrderEvent and every level below it final or sealed, or set an explicit filter(...),
which is used instead of deriving one and is the way out when a CloudEventTypeMapper of your own maps the
whole hierarchy onto a single CloudEvent type string.
```

`DomainEventQueries` reports the same shape for `query(OrderEvent.class)` or `query(List.of(OrderEvent.class))`,
pointing you at `query(Filter, ..)` instead of a `filter(...)` override, since the query DSL has no override
of its own:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent
cannot all be enumerated, so a filter derived from it would miss some of them. Query the concrete event types
instead, make OrderEvent and every level below it final or sealed, or call query(Filter, ..) directly with a
filter of your own, which is the way out when a CloudEventTypeMapper of your own maps the whole hierarchy
onto a single CloudEvent type string.
```

The subscription DSL's `filterFromEventTypes` (and the `subscriptionFilterFromEventTypes`/
`agnosticSubscriptionFilterFromEventTypes` built on it) throw the same shape without the override
suggestion, since that Kotlin function has none either.

You are affected when a declared or registered type is one of these:

| Shape | Java | Kotlin |
|---|---|---|
| An interface that is not sealed | `interface OrderEvent` | `interface OrderEvent` |
| An abstract class that is not sealed | `abstract class OrderEvent` | `abstract class OrderEvent` |
| A sealed hierarchy reopened below the declared type | `non-sealed class Base implements OrderEvent` | `open class Base : OrderEvent` or `abstract class Base : OrderEvent` |
| An array type | `OrderEvent[]` | `Array<OrderEvent>` |
| A primitive class literal | `int.class` | `Int::class` |

A projection, a subscription, a query, or a snapshot that declares concrete types, or a sealed type whose
every level is sealed or final, is unaffected. Java records and Kotlin data classes are final already, so an
ordinary sealed hierarchy of records needs nothing.

### Seal the hierarchy

The better remedy when you own the events, since a handler keyed on the supertype keeps working as you add
event types under it. In Java, mark the reopened level `sealed` and list what it permits:

```java
// Before, refused: Base reopens the hierarchy, so nothing below it can be found
public sealed interface OrderEvent permits Base { }
public non-sealed class Base implements OrderEvent { }

// After
public sealed interface OrderEvent permits Base { }
public sealed class Base implements OrderEvent permits OrderPlaced, PaymentReserved { }
```

In Kotlin, an `open class` or an `abstract class` in the middle becomes `sealed`:

```kotlin
sealed interface OrderEvent
sealed class Base : OrderEvent            // was open class or abstract class
data class OrderPlaced(val orderId: String) : Base()
```

### Or declare the concrete event types

Use this when the hierarchy is not yours to seal, or when it is deliberately open. Register a handler, or
list a type, per concrete type instead of the supertype: `Projection.Builder.on(OrderPlaced.class, ...)` and
`.on(PaymentReserved.class, ...)` in place of a single `.on(OrderEvent.class, ...)`, `SnapshotView.Builder.on(...)`
the same way, `filterFromEventTypes(converter, arrayOf(OrderPlaced::class, PaymentReserved::class))` in place
of `arrayOf(OrderEvent::class)`, and `domainEventQueries.query(OrderPlaced.class, PaymentReserved.class)` or
`query(List.of(OrderPlaced.class, PaymentReserved.class))` in place of `query(OrderEvent.class)`.

### Or set an explicit filter

`Projection.Builder.filter(Filter)` and `SnapshotView.Builder.filter(Filter)` already exist for selecting on
more than event type, and either one also skips expansion entirely for that projection or snapshot, so it is
the way out when a `CloudEventTypeMapper` of your own collapses a hierarchy onto one CloudEvent type string.
`DomainEventQueries` has no such override on its `Class`/`Collection` overloads, since it never derived one
before this release either. Call `query(Filter, ..)` directly with a `Filter` of your own instead. The
subscription DSL's `filterFromEventTypes` has no override, so build the `Filter` yourself and pass it to
`StreamSubscriptionFilter.filter(...)` or `AgnosticSubscriptionFilter.filter(...)` in place of calling
`filterFromEventTypes`.

### Empty still means empty on `DomainEventQueries`

`DomainEventQueries.query(Collection)` and its sibling overloads already treated a `null` or empty
collection as "match nothing", returning an empty stream rather than every event, and that stays true under
this release. Expansion only runs on a non-empty collection, so an empty or `null` one is never turned into
`Filter.all()` the way an empty `eventTypes()` is for a projection, a subscription, or a snapshot, which
match everything. If your code passed an empty collection expecting an empty result, nothing changes for
you.

### Why there is no recipe for this one

The same reason [section 10 of the 0.33.0 guide](upgrading-to-0.33.0.md#why-there-is-no-recipe-for-this-one)
gives for the saga and subscription case. Telling a refused declaration from a sealed one that now works
needs the sealed modifier from the class declaration, which OpenRewrite does not expose on the type behind a
class literal, so a mechanical rewrite or even a review marker is not possible. A projection throws the
first time a runner or a query derives its filter, a `@Snapshot` throws at registration, and
`DomainEventQueries` and the subscription DSL throw at the first query or subscription registration that
needs one, so a test that exercises your projections, queries, subscriptions, and snapshots finds every
affected declaration.

## 4. Four MongoDB-only keys move under `mongodb`

`occurrent.event-store.collection`, `occurrent.event-store.time-representation`, `occurrent.subscription.collection`
and `occurrent.subscription.restart-on-change-stream-history-lost` never configured anything but a MongoDB event
store or a MongoDB subscription model, even though the module they live in (`occurrent-spring-boot-autoconfigure`)
dropped its `mongodb` name back in 0.30.0 because the rest of its code is store-neutral.

A second store, the SQL event store, is coming, and its own starter would otherwise inherit four keys promising a
collection and a change stream it does not have.

Each key now has the `mongodb` qualifier that was always true of it:

| Old | New |
|---|---|
| `occurrent.event-store.collection` | `occurrent.event-store.mongodb.collection` |
| `occurrent.event-store.time-representation` | `occurrent.event-store.mongodb.time-representation` |
| `occurrent.subscription.collection` | `occurrent.subscription.mongodb.collection` |
| `occurrent.subscription.restart-on-change-stream-history-lost` | `occurrent.subscription.mongodb.restart-on-change-stream-history-lost` |

Each old key still works and is deprecated, so nothing breaks if you upgrade without touching your configuration.
Every one of them is removed in the release after next.

Setting both the old and the new key is allowed while they agree, which is deliberate. A recipe rewrites
configuration files but cannot reach an environment variable, so an application mid-migration can legitimately have
both set. Setting both so they contradict each other fails at startup, naming both keys.

### Run the recipe

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_34</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.34.0</version>
        </dependency>
    </dependencies>
</plugin>
```

```bash
mvn rewrite:run
```

It rewrites `.properties` and `.yaml` alike, and it is deliberately not restricted to `application.properties` or
`application.yml`, so it also reaches a profile file, a `config/` directory, and anything you pull in with
`spring.config.import`. Expect the diff to cover every configuration file that sets one of the four keys, wherever
it lives.

Unlike the `occurrent.subscription.enabled` migration in 0.32.0, no value changes here, only the key, so the recipe
is a plain rename in `.properties`. In `.yaml` it renames the key in place rather than expanding it into a nested
`mongodb:` block, so `event-store.collection: events` becomes `event-store.mongodb.collection: events` on one line
rather than a new nested mapping.

Spring's relaxed binding resolves either shape to the same property name, so this only changes how the file reads,
not what it configures. Restructure it into a nested block yourself if you prefer that layout.

### What the recipe leaves for you

Two cases, both of which it steps around on purpose rather than guessing:

- **An environment variable or anything outside your configuration files.** `OCCURRENT_EVENT_STORE_COLLECTION` is
  invisible to a source rewrite. Search your deployment configuration for it by hand. This is exactly why setting
  both the old and the new key is tolerated while they agree.
- **A file that already sets both the old and the new key.** The recipe drops the old one and keeps the
  `mongodb`-qualified key, on the assumption that the key you migrated to is the one you meant.
- **A multi-document `.yaml` file where one profile sets the old key and a different profile sets the new one.**
  The drop-the-old-key guard evaluates across the whole file, not the one profile that set the new key, so it
  can also drop the old key from a profile that never set the new key at all, and remove that profile's document
  entirely if the dropped key was its only content. Review the diff before you commit it, and see
  [#828](https://github.com/johanhaleby/occurrent/issues/828) for the fix.
