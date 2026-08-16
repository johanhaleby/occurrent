# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

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

## 2. A projection, a subscription, a query, or a snapshot declaring a supertype event is refused

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
