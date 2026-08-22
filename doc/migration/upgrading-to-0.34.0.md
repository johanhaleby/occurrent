# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

Eight things are worth reading, three of them compile-time breaks. At compile time, if you use the flow saga's
deprecated `join` or Kotlin's `expect<T>`, both are gone. Read
[section 1](#1-a-flow-sagas-join-kotlins-expectt-and-expectation-are-removed). A flow saga's `stepWindow` now
counts and evicts only the events its own steps declare, which most callers need to do nothing about. Read
[section 2](#2-a-flow-sagas-stepwindow-now-caps-only-its-own-declared-events). A projection, a subscription, a
query, or a snapshot that declares an event type whose concrete subtypes cannot be found is now refused, the same
refusal 0.33.0 already shipped for a saga and an annotation-based subscription, and one shape that was exempt
everywhere, a concrete class that is neither final nor sealed, is now refused on all six. Read
[section 3](#3-declaring-an-event-type-whose-concrete-subtypes-cannot-be-found-is-refused). At
startup, if you set a MongoDB collection name, a MongoDB time representation, or whether a subscription restarts
after losing change-stream history, through `OccurrentProperties`, four configuration keys are deprecated and have
a recipe that rewrites them for you. Read [section 4](#4-four-mongodb-only-keys-move-under-mongodb). A
`@Projection`, `@Saga`, or `@Snapshot` factory method no longer runs through a proxy, so class-level advice that
ran as a side effect of building the descriptor at startup no longer runs at all. Read
[section 5](#5-a-descriptor-factorys-class-level-advice-no-longer-runs-at-startup).
`WriteResult` and `DcbAppendResult` both gain a fourth component. Deconstructing either with a record pattern is
a second compile-time break, and comparing either whole for equality fails silently at runtime instead. Read
[section 6](#6-writeresult-and-dcbappendresult-gain-a-fourth-component-the-append-id). And if
`DurableSubscriptionModel` wraps a MongoDB subscription model on a shared Atlas cluster, a fresh subscription that
used to start without a recorded position is now refused at `subscribe(..)`. Read
[section 7](#7-durablesubscriptionmodel-refuses-a-first-subscription-when-no-start-position-can-be-recorded).
Finally, a saga instance whose event keeps failing is now suspended instead of retried forever, which changes five
things about the saga API at once. `SagaEnvelope` and `SagaRunnerConfig` each gain a record component, `SagaInstance`
gains a method, and `SagaStatus` gains a constant that `findByStatus(ACTIVE, ..)` no longer returns. Read
[section 8](#8-a-saga-instance-that-keeps-failing-is-quarantined-and-four-saga-types-change-with-it).

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

## 3. Declaring an event type whose concrete subtypes cannot be found is refused

0.33.0 made a saga and an annotation-based subscription expand a declared sealed event type into the
concrete types it permits, and refuse a declared type whose concrete types cannot all be found ([section
10 of that guide](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)).
Six more places derived a type filter the same old way and did not get that fix. The projection DSL, the
subscription DSL's `filterFromEventTypes`, `DomainEventQueries` on both the blocking and reactor stacks, the
Spring Boot starter's `@Snapshot` registrar on both stacks, `ExecuteFilter`'s `type(Class)` and
`includeTypes(Class, ...)`, and `DcbCriteriaBuilder`'s `type(Class)` and `types(Class, ...)` all kept the old
derivation, or in `ExecuteFilter` and `DcbCriteriaBuilder`'s case had none at all. 0.34.0 brings all six in
line with the saga and the annotation-based subscription. See [ADR 126](../architecture/decisions/0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md).

0.34.0 also removes the one shape that was exempt everywhere, a concrete class that is neither final nor
sealed, which is the concrete-class row of the table below. That part changes a saga and an annotation-based
subscription too, so read it even if the six places above are not what you use. See
[#753](https://github.com/johanhaleby/occurrent/issues/753) and [#912](https://github.com/johanhaleby/occurrent/issues/912).

`ExecuteFilter.excludeTypes` mostly sits outside this refusal. A declared type it cannot fully expand is
widened to every concrete subtype a downward walk can find instead, since excluding a supertype has to
exclude everything under it, and widening can only exclude more. That walk is the sealed-permits walk, which
starts at the declared type, follows a `permits` clause through `Class.getPermittedSubclasses`, and stops at
the first level that is not sealed, so a type it cannot reach means an event you wanted out stays in, rather
than the reverse. That widening needs no migration step by itself. It still refuses an array or a
primitive declared type, the same two shapes `type`/`includeTypes` refuse, though for two different reasons.
No event is ever an instance of a primitive class, so declaring one is a mistake and the concrete event types
are what you meant. An array is refused for consistency with `type`/`includeTypes` rather than because
excluding one is impossible, and an array class is already concrete, so there is no narrower type to declare.
Build the `StreamReadFilter` yourself with `ExecuteFilter.from(StreamReadFilter)` if you do mean to exclude an
array type. Declaring either to `excludeTypes` was accepted up to 0.33.0, whatever your `CloudEventTypeGetter`
happened to return for it, so that shape is new. Nobody sensibly excludes by array or primitive type, so
in practice this affects close to nobody, but it is still a behavior change worth naming rather than folding
into "no migration step."

**Widening is not completeness, and this is the one place in this section worth reading even if you never hit
a refusal.** Two declared-type shapes still leave a gap after this fix, and one of them can silently exclude
nothing at all rather than merely less than hoped.

A concrete class that is declared directly and is itself neither final nor sealed contributes itself to the
widened exclusion: reflection cannot discover a subclass stored under its own name, so
`excludeTypes(OrderPlaced.class)` on such a class still only excludes events of `OrderPlaced`'s own CloudEvent
type, exactly as before, but that exclusion is not empty.

An interface or an abstract class whose hierarchy reopens before the downward walk finds anything concrete is
different, and this is the shape to check your own declarations against. `excludeTypes(SensitiveEvent.class)`
on a sealed `SensitiveEvent` that permits only a non-sealed abstract class, with nothing concrete found above
that level, contributes `SensitiveEvent`'s own declared name and nothing else. How much that excludes is then
decided by your `CloudEventTypeMapper` rather than by the walk, and the two answers are as far apart as they
get.

Under a mapper that stores each type under its own class name, which is what `ReflectionCloudEventTypeMapper`
does in both its qualified and its simple form, no stored event is written under `SensitiveEvent`'s own name, so the
filter excludes zero real events, silently, exactly as it did before this fix, and nothing about the
exception-free result tells you that. Under a mapper of your own that maps the whole hierarchy onto one
CloudEvent type string, the same declaration excludes the whole family, because that one string is what the
concrete events are stored under. Seal the hierarchy, or declare the concrete types directly, for an exclusion
that does not depend on which of those two you configured. See the changelog entry under `#### Changes` for
[#912](https://github.com/johanhaleby/occurrent/issues/912).

Widening on a boundary-seeded `DcbCriteriaBuilder` and DCB append conditions built from a `DcbCriteriaBuilder`
also change, worth calling out even though `DcbCriteriaBuilder` has no `excludeTypes`. `type`/`types` now name
every concrete subtype a declared supertype permits, so a `DcbCriterion` built from `type(OrderEvent.class)`
matches more events than it used to whenever `OrderEvent` is sealed. Two consequences follow from that. A
`DcbCriteriaBuilder` seeded with a boundary carrying `excludingTypes(...)` (`DcbCriterion.excludingTypes`) now
throws `IllegalArgumentException("Types and excluded types cannot overlap")` if the newly expanded types include
one already excluded on the boundary, where before expansion that overlap was unreachable unless you named the
excluded type directly. And `DcbCriteriaBuilder`'s constructors build `DcbAppendCondition` boundaries as well as
read criteria (`DcbAppendCondition#failIfEventsMatch`), so an append boundary built from a sealed supertype now
conflicts with more concurrent writes than before, the same correctness fix as the read side, applied to
optimistic concurrency checks instead of a query.

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

`ExecuteFilter.type(Class)` and `includeTypes(Class, ...)` throw the same shape the first time
`ApplicationService#execute` resolves the filter, pointing you at `ExecuteFilter.from(StreamReadFilter)`:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent
cannot all be enumerated, so a filter derived from it would miss some of them. Filter on the concrete event
types instead, make OrderEvent and every level below it final or sealed, or build the StreamReadFilter
yourself with ExecuteFilter.from(..), which is the way out when a CloudEventTypeMapper of your own maps the
whole hierarchy onto a single CloudEvent type string.
```

`DcbCriteriaBuilder.type(Class)` and `types(Class, ...)` throw when the call builds the criterion, pointing
you at building a `DcbCriterion` from the raw CloudEvent type string instead, since the builder has no
override of its own:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent
cannot all be enumerated, so a criterion derived from it would miss some of them. Declare the concrete event
types instead, make OrderEvent and every level below it final or sealed, or build the DcbCriterion yourself
with the raw type string, which is the way out when a CloudEventTypeMapper of your own maps the whole
hierarchy onto a single CloudEvent type string.
```

You are affected when a declared or registered type is one of these:

| Shape | Java | Kotlin |
|---|---|---|
| An interface that is not sealed | `interface OrderEvent` | `interface OrderEvent` |
| An abstract class that is not sealed | `abstract class OrderEvent` | `abstract class OrderEvent` |
| A sealed hierarchy reopened below the declared type | `non-sealed class Base implements OrderEvent` | `open class Base : OrderEvent` or `abstract class Base : OrderEvent` |
| An array type | `OrderEvent[]` | `Array<OrderEvent>` |
| A primitive class literal | `int.class` | `Int::class` |
| A concrete class that is neither final nor sealed | `class OrderPlaced` | `open class OrderPlaced`, or an `enum class` whose constants have bodies |

A projection, a subscription, a query, or a snapshot that declares concrete types, or a sealed type whose
every level is sealed or final, is unaffected. Java records and Kotlin data classes are final already, so an
ordinary sealed hierarchy of records needs nothing.

### The concrete-class row also changes a saga and an annotation-based subscription

The first five shapes were already refused for a saga and an annotation-based subscription in 0.33.0, and
0.34.0 only brings the other six places in line. The concrete-class row is different. 0.33.0 exempted a concrete class
that is neither final nor sealed everywhere, on purpose, to keep every caller declaring one working. 0.34.0
removes that exemption, so a saga and an annotation-based subscription now refuse it too.

What the exemption did was accept the declaration and derive a filter naming that one class. A caller
declaring `class OrderPlaced` and publishing a `class SpecialOrderPlaced extends OrderPlaced` got a filter
asking for `OrderPlaced` and nothing else. Under every `CloudEventTypeMapper` Occurrent ships the subclass is
stored under its own name, so it never reached the handler and nothing said why. Dispatch would have accepted
it, since a handler declared on a supertype receives every concrete subtype.

```java
// Refused from 0.34.0. Accepted in 0.33.0, and SpecialOrderPlaced never arrived
public class OrderPlaced { }
public class SpecialOrderPlaced extends OrderPlaced { }
```

Marking the declared class `final` is the smallest fix when nothing extends it, and it is the fix for a class
that was only ever left open by habit:

```java
public final class OrderPlaced { }
```

When something does extend it, the three remedies below apply unchanged. Seal the hierarchy, declare the
concrete types, or set an explicit filter. In Kotlin, dropping `open` is the same smallest fix, since a
Kotlin class is final unless it says otherwise.

**If your own `CloudEventTypeMapper` maps the whole hierarchy onto one CloudEvent type string, you were not
losing anything, and you are the one caller here with a real regression.** The subclass was stored under the
declared class's type string, so the derived filter did ask for it and it did reach the handler. 0.34.0
refuses the declaration anyway, because nothing in the type model tells the expansion that your mapper
collapses the hierarchy. Set an explicit filter, which skips expansion entirely for that registration and is
what the "Or set an explicit filter" section below is for. That is the same escape the four shapes above
already point a collapsing mapper at.

The reason the refusal is worth the break for everyone else is that the alternative is silent. A caller on
0.33.0 using a mapper Occurrent ships who publishes a subclass loses those events with nothing in a log to
explain it, and no later release makes that loss visible without the same break. Waiting only adds another
release of loss in front of it.

### A Kotlin enum with constant bodies cannot be sealed or made final

This is the concrete-class row of the table in Kotlin form, and it gets its own section because two of that
row's remedies are not available here. Kotlin compiles an `enum class` whose constants have bodies as a
concrete class that is neither final nor sealed, and each constant body as a separate class no `permits` clause
points the walk at. The declaration is refused, and the refusal message offers to make the class final or
sealed, neither of which you can write on an enum.

```kotlin
// Refused from 0.34.0. Neither final nor sealed once a constant has a body
enum class PaymentEvent : DomainEvent {
    Reserved { override fun toString() = "reserved" },
    Settled { override fun toString() = "settled" }
}
```

Two things do work. Declare the constants, since each body compiles to its own final class:

```kotlin
ExecuteFilter.includeTypes(PaymentEvent.Reserved.javaClass, PaymentEvent.Settled.javaClass)
dcbCriteriaBuilder.types(PaymentEvent.Reserved.javaClass, PaymentEvent.Settled.javaClass)
```

Or move the per-constant behavior into a constructor parameter or a `when`, so the enum needs no constant
bodies and Kotlin compiles it final again, which lets you declare the enum itself:

```kotlin
// Accepted, and PaymentEvent::class.java can be declared directly
enum class PaymentEvent(private val label: String) : DomainEvent {
    Reserved("reserved"),
    Settled("settled");

    override fun toString() = label
}
```

Removing the bodies also unblocks a sealed event interface above the enum. An enum with constant bodies reopens
such an interface, so declaring the interface is refused too, and that is usually where the refusal reaches you
rather than on the enum itself.

The two shapes are stored under different CloudEvent types, so pick between them before you have events in the
store rather than after. `PaymentEvent.Reserved.javaClass` is `PaymentEvent$Reserved` while the bodiless
version's constants are all `PaymentEvent`, and `ReflectionCloudEventTypeMapper` maps whichever class it is
handed.

A Java enum with constant bodies is unaffected, since javac seals that construct implicitly (JLS 8.9) and the
walk finds every constant through the `permits` clause javac writes.

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
of `arrayOf(OrderEvent::class)`, `domainEventQueries.query(OrderPlaced.class, PaymentReserved.class)` or
`query(List.of(OrderPlaced.class, PaymentReserved.class))` in place of `query(OrderEvent.class)`,
`ExecuteFilter.includeTypes(OrderPlaced.class, PaymentReserved.class)` in place of `type(OrderEvent.class)`,
and `dcbCriteriaBuilder.types(OrderPlaced.class, PaymentReserved.class)` in place of `type(OrderEvent.class)`.

### Or set an explicit filter

`Projection.Builder.filter(Filter)` and `SnapshotView.Builder.filter(Filter)` already exist for selecting on
more than event type, and either one also skips expansion entirely for that projection or snapshot, so it is
the way out when a `CloudEventTypeMapper` of your own collapses a hierarchy onto one CloudEvent type string.
`DomainEventQueries` has no such override on its `Class`/`Collection` overloads, since it never derived one
before this release either. Call `query(Filter, ..)` directly with a `Filter` of your own instead. The
subscription DSL's `filterFromEventTypes` has no override, so build the `Filter` yourself and pass it to
`StreamSubscriptionFilter.filter(...)` or `AgnosticSubscriptionFilter.filter(...)` in place of calling
`filterFromEventTypes`. A saga's override is `replacementFilter(Filter)`, which already skipped expansion
before this release and still does. An annotation-based subscription has none, so list the concrete types in
the annotation's `eventTypes` attribute instead. `ExecuteFilter`'s override is the already-existing
`ExecuteFilter.from(StreamReadFilter)`, which skips expansion entirely by building the `StreamReadFilter`
yourself. `DcbCriteriaBuilder` has no override, so build the `DcbCriterion` from the raw CloudEvent type
string with `DcbCriteria.type(String)` or `types(String, ...)` instead of going through the builder for that
criterion.

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
first time a runner or a query derives its filter, a `@Snapshot` throws at registration,
`DomainEventQueries` and the subscription DSL throw at the first query or subscription registration that
needs one, `ExecuteFilter` throws the first time `ApplicationService#execute` resolves the filter, and
`DcbCriteriaBuilder` throws when `type(...)`/`types(...)` builds the criterion, so a test that exercises your
projections, queries, subscriptions, snapshots, execute filters, and DCB criteria finds every affected
declaration.

The concrete-class row of the table, a class that is neither final nor sealed, has a second reason on top of
that one. Even given the modifier, a recipe would have to pick between two remedies that mean different
things about your domain. Adding `final` says nothing may ever extend the class, and a recipe cannot know
whether a subclass exists in another module, in another repository, or in an application that only depends
on your events. Declaring the concrete types instead says the hierarchy is open and you are listing what you
handle. Getting that wrong either breaks a subclass a recipe never saw or narrows a handler that meant to be
wide, so the choice stays yours.

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

## 5. A descriptor factory's class-level advice no longer runs at startup

`@Projection`, `@Saga` and `@Snapshot` factory methods now always run directly on the bean's own class, never through
a proxy.

Before this release, a bean advised under CGLIB, the default (`spring.aop.proxy-target-class=true`), had its factory
invoked through the proxy. Any class-level advice matching the bean, `@Transactional` or a custom aspect for example,
ran once as a side effect of building the descriptor at startup.

That was never a documented or supported behavior. [ADR 127 section 4](../architecture/decisions/0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md#4-a-descriptor-annotation-is-read-after-the-singletons-are-instantiated)
calls it "surprising but survivable", the accidental result of a JDK interface proxy and a CGLIB proxy handling the
same invocation differently. The fix for the JDK interface proxy crash in [#836](https://github.com/johanhaleby/occurrent/issues/836)
makes both proxy kinds behave the same way, skipping the proxy entirely, since a descriptor factory runs exactly once
at startup with no request for its advice to usefully observe.

If your application relied on that advice running, deliberately or not, move whatever it did into the factory method
itself, or into a separate lifecycle hook such as `@PostConstruct` on the same bean, since this shortcut never
reached it in the first place.

There is no recipe for this change. Nothing in your source code declares a requirement on advice running through a
startup-only reflective invocation, so there is nothing a rewrite could search for.

### A null-returning factory now fails differently on reactor

A reactor `@Projection`, `@Saga` or `@Snapshot` factory method that returns `null` instead of its declared descriptor
now fails with `IllegalStateException`. It previously failed with `IllegalArgumentException`.

The blocking stack already used `IllegalStateException` for the same mistake, so this only changes the reactor side,
and only for a factory that is already broken. A catch block scoped to `IllegalArgumentException` around that
specific failure no longer catches it.

## 6. `WriteResult` and `DcbAppendResult` gain a fourth component, the append id

Both records gain a fourth component, `Optional<AppendId> appendId()`, the identifier every store now
stamps on every event a single write or DCB append call persists. A write that persists no events reports
`Optional.empty()`, and so does a result built through the three-argument constructor both records keep.
See [ADR 132](../architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md)
for the full design, including what a later release does with the identifier once it ships.

Two things break, and only one of them is a compile error.

### A whole-record equality assertion starts failing silently

`assertThat(result).isEqualTo(new WriteResult(streamId, 0, 1))` compares the append id too, and a fresh one
is minted for every write that persists something. The assertion still compiles. It runs and fails, with
nothing at build time pointing at what needs attention.

Compare the components you actually mean to assert on instead:

```java
// Before
assertThat(result).isEqualTo(new WriteResult(streamId, 0, 1));

// After
assertThat(result.streamId()).isEqualTo(streamId);
assertThat(result.oldStreamVersion()).isEqualTo(0);
assertThat(result.newStreamVersion()).isEqualTo(1);
```

An assertion against an empty write is unaffected, since `Optional.empty()` compares equal on both sides
regardless of this change.

#### Why there is no recipe for this one

A recipe would need to know the append id an assertion should expect, and nothing in the source states
that value anywhere a rewrite could read it. `UpgradeToOccurrent_0_34` leaves every `isEqualTo(...)` call
against a `WriteResult` or `DcbAppendResult` alone. A test that exercises the affected code path finds it
for you, the first time it runs against 0.34.0.

### A record pattern naming the original three components stops compiling

A record pattern has to name every component of the canonical constructor, and that constructor now has
four:

```java
// Before, stops compiling
case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion) -> ...

// After
case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion, var appendId) -> ...
```

The same applies to `DcbAppendResult`, and to an `instanceof` pattern as much as a `switch` case.

#### Run the recipe

Unlike the equality case above, the record-pattern break is mechanical. A record pattern's arity is a fact
the compiler enforces, not a judgement call, so `UpgradeToOccurrent_0_34` appends the fourth binding,
`var appendId`, to any three-component deconstruction pattern against either type, whatever the first
three bindings were named or typed. If a name called `appendId` is already bound in the pattern or an
enclosing scope, the recipe falls back to `appendId1`, then `appendId2`, and so on, so the added binding
never collides with one that is already there. Run it the same way
[section 4](#4-four-mongodb-only-keys-move-under-mongodb) does.

## 7. `DurableSubscriptionModel` refuses a first subscription when no start position can be recorded

`DurableSubscriptionModel.subscribe(..)` now throws `IllegalStateException` when the caller asks for
`StartAt.subscriptionModelDefault()` (the default when no `StartAt` is given), no checkpoint is stored for the
subscription id, and the wrapped model's `globalCheckpoint()` answers `null`. Both MongoDB subscription models
answer `null` when the server refuses the `hostInfo` command, which shared MongoDB Atlas clusters (M0, Flex and
similar tiers) do, so this is the setup that hits the refusal. On a server that permits `hostInfo`, and on a
subscription that has run before, nothing changes.

Up to 0.33.0 such a subscription started anyway, from wherever the feed happened to be, with nothing in checkpoint
storage. It looked like it worked, and it did keep working as long as the very first delivery succeeded. A crash
before the first checkpoint was saved started over from wherever the feed had reached by then, so an event whose
delivery failed just before the crash was never seen again.

The refusal replaces that quiet loss with an error at `subscribe(..)`, which for a Spring Boot application means
at startup. Nothing is registered for the id, so subscribing again once the model can answer works.

Three ways forward, and the first needs no code change:

* Set `occurrent.subscription.start-when-no-start-position-can-be-recorded=true` in the Spring Boot starter, or
  configure `DurableSubscriptionModelConfig.startWhenNoStartPositionCanBeRecorded(true)` when building the model
  yourself. The subscription then starts the way it did before this release, with nothing recorded until the
  first checkpoint is saved and the loss window that comes with it, only now chosen deliberately instead of
  decided silently by what the server refuses.
* Run against a cluster that permits `hostInfo`, which on Atlas means a dedicated tier (M10 and up). The
  subscription then records its start position before anything is delivered and resumes from it after a crash,
  which is the promise `DurableSubscriptionModel` exists to make.
* Subscribe with a `StartAt` of your own, `StartAt.now()` for example. That records no position and makes no
  resume promise for the time before the first checkpoint is saved.

The blocking `ManualStartSubscriptionModel` and the reactor `ReactorDurableSubscriptionModel` have answered a
`null` position source this way since 0.33.0. This change gives `DurableSubscriptionModel` the same answer. The
property reaches the reactive starter too, where
`ReactorDurableSubscriptionModelConfig.startWhenNoStartPositionCanBeRecorded(true)` now lets
`ReactorDurableSubscriptionModel` start such a registration as well, so a reactive application on a shared Atlas
cluster gets the same no-code-change path out of the refusal it has had since 0.33.0.


## 8. A saga instance that keeps failing is quarantined, and four saga types change with it

A saga has one subscription, and every instance of that saga is fed by it. Up to 0.33.0, an event that a saga's
`evolve`, its `react` or its command dispatcher could not handle propagated to the subscription model, which
redelivered it and tried again, without limit. One correlation id that could never make progress therefore stopped
every other correlation id behind it, for as long as nobody noticed.

From 0.34.0 the executor times the failing rather than counting the attempts. The first failure of an event records
the instant it started failing and rethrows, exactly as before. Once that event has kept failing for the same
instance for longer than `SagaRunnerConfig.quarantineAfter`, five minutes by default, the instance moves to the new
`SagaStatus.QUARANTINED` and the executor stops rethrowing, so the subscription acknowledges the event and delivers
the rest to everybody else.

A quarantined instance receives no further events and fires no timers, and its redelivery watermarks stop moving, so
nothing it skipped is recorded as handled. That is what makes it recoverable. Call
`SagaSubscription.release(sagaId)` once the cause is fixed and the saga's subscription is replayed from the position
the instance stopped at, which pauses delivery to every instance of that saga until the replay finishes. The other
instances recognise the replayed events as redeliveries through their own watermarks, so no command is dispatched a
second time.

There are two limits to know before you rely on it.

**Quarantine is available only on a subscription model that can be resumed at a chosen position,** which is
`NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `CatchupSubscriptionModel`, including any of them
behind `DurableSubscriptionModel` or `CompetingConsumerSubscriptionModel`. On any other model the runner switches the
budget off at startup and logs why, so the saga keeps the 0.33.0 behaviour of blocking. That is deliberate rather than
an omission. Quarantining means returning normally, which acknowledges the event to whatever fed it, and on a push
feed behind a broker bridge that is what stages the offset and moves past the record. The one copy this saga could
ever be given would be gone at the moment of quarantine rather than at the release. Between an instance that blocks
and an event that cannot be asked for again, this keeps the event.

An event with no global position is never quarantined either, for the same reason. There would be no position to
replay from. Occurrent's own stored events always have one, and a feed that drops the CloudEvent extensions on the way
in does not.

### The five breaks

**`SagaStatus.QUARANTINED` is a new constant.** An exhaustive Java `switch` or Kotlin `when` over `SagaStatus` stops
compiling until you add a branch for it. What that branch should do is a question about your code, so decide it
rather than copying the `COMPLETED` branch. A quarantined instance is not finished, it is stopped and waiting for
somebody to look at it.

**`findByStatus(ACTIVE, ..)` no longer returns a quarantined instance,** and it breaks nothing at compile time.
If you use that call to sweep for instances that have gone quiet, which is what it was built for, it now misses the
instances most worth finding. Enumerate `QUARANTINED` as well.

```java
List<SagaInstance> stuck = new ArrayList<>();
stuck.addAll(instances.findByStatus(SagaStatus.ACTIVE, Instant.now().minus(threshold), 100));
stuck.addAll(instances.findByStatus(SagaStatus.QUARANTINED, Instant.now(), 100));
```

**`SagaInstance` gains a `failure()` method,** which breaks anyone implementing that interface outside this
repository. It tells you what a quarantined instance stopped on, which is the failing event's position, the
exception's class name and message, and when the failing started, and it answers `null` for an instance that is
failing on nothing. `SagaEnvelope` implements it from its new `failure` component, so a store that carries that
component answers it for free.

**`SagaEnvelope` gains two record components, `started` and `failure`,** which changes its canonical constructor and
the arity of any record pattern over it. Only a `SagaStateStore` implemented outside this repository constructs one.
The old eleven-argument form is kept as a deprecated constructor that fills in `started = true` and `failure = null`,
so an existing call site compiles unchanged, but a store built that way can never report a quarantined instance.
Persist both components and read them back to support quarantine, and read a missing `started` field as `true`, since
every instance written before 0.34.0 had started. A record pattern has no such fallback and has to name the two new
components.

```java
// 0.33.0
case SagaEnvelope(String sagaId, var state, var status, long version, var timers,
                  var streamWatermarks, var positionWatermark, var createdAt,
                  var updatedAt, var completedAt, var currentStep) -> ...

// 0.34.0
case SagaEnvelope(String sagaId, var state, var status, long version, var timers,
                  var streamWatermarks, var positionWatermark, var createdAt,
                  var updatedAt, var completedAt, var currentStep,
                  boolean started, var failure) -> ...
```

**`SagaRunnerConfig` gains a fifth record component, `quarantineAfter`.** The four-argument form stays as a
constructor that defaults it to five minutes, so a call site written against 0.33.0 compiles unchanged and gets the
new behaviour. A record pattern over `SagaRunnerConfig` has to name the fifth component. Pass `null` to keep the
0.33.0 behaviour of retrying forever.

```java
SagaRunnerConfig config = SagaRunnerConfig.defaults().withQuarantineAfter(null);
```

### Why there is no recipe for this one

None of the five can be rewritten mechanically. What your new `case QUARANTINED` branch should do depends on what the
`switch` is for, and whether a given `findByStatus(ACTIVE, ..)` call site wants quarantined instances included is a
question about that caller's intent rather than about the API. The two record-component additions could in principle
be rewritten, but a recipe that fixed those two and left the two that matter would read as a migration that had been
handled. This section is the migration.
