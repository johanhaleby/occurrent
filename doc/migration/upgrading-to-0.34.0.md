# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

Nineteen things are worth reading, four of them compile-time breaks. At compile time, if you use the flow saga's
deprecated `join` or Kotlin's `expect<T>`, both are gone. Read
[section 1](#1-a-flow-sagas-join-kotlins-expectt-and-expectation-are-removed). A flow saga's `stepWindow` now
counts and evicts only the events its own steps declare, plus the type that starts the flow, which most
callers need to do nothing about. Read
[section 2](#2-a-flow-sagas-stepwindow-now-caps-its-declared-events-and-the-start-type). A projection, a subscription, a
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
Then a saga instance whose event keeps failing can now be quarantined instead of left active and failing
indefinitely, which changes five things about the saga API at once. `SagaEnvelope` gains
two record components and `SagaRunnerConfig` gains one, `SagaInstance` gains a method, and `SagaStatus` gains a constant that `findByStatus(ACTIVE, ..)` no longer returns. Read
[section 8](#8-a-saga-instance-that-keeps-failing-is-quarantined-and-four-saga-types-change-with-it).
Then a reactor catch-up subscription now delivers an event a second time when a write that was in flight during the
replay was read by a history window, which needs a handler that is safe to run twice on the same event. Read
[section 9](#9-a-reactor-catch-up-subscription-can-deliver-a-concurrent-write-twice).
Then, if your application ever called `updateEvent` while running 0.33.0 or earlier, some of your stored
events are damaged and a one-off repair puts them back. Read
[section 10](#10-events-updateevent-damaged-before-0340-need-a-one-off-repair).
Then a subscription handler Spring's proxy cannot invoke now fails startup instead of silently losing its advice,
and every annotation-based handler registers later, once singleton construction has finished, so a live-only
subscription no longer sees an event a bean wrote from its own startup. Read
[section 11](#11-a-subscription-handler-spring-cannot-invoke-now-fails-startup-and-a-live-subscription-can-miss-a-startup-write).
Then a projection feed's `accept(..)` no longer reports an event it did not apply as handled. On the blocking
stack it now waits during a catch-up until the event is applied and throws when it was not, so a call on the same
thread that later starts the catch-up waits until another thread runs the catch-up, takes the feed live, calls
`stopCatchUp()` or interrupts it. On the reactor stack its `Mono` now errors for an event fed while the feed is
stopped. Read
[section 12](#12-a-projection-feeds-accept-waits-until-the-event-is-applied-and-fails-when-it-is-not).
Then feeding a push model's `accept(..)` is supported only from the in-memory event store's write path, where
0.33.0 also named a broker listener, a Spring application event and an HTTP endpoint. Read
[section 13](#13-only-the-in-memory-event-stores-write-path-may-feed-a-push-models-accept).
Then a projection feed's `goLive()` called while a catch-up of the same projection is replaying now waits for
that replay to end, where 0.33.0 did not wait, and fails when a catch-up of that projection failed meanwhile. Read
[section 14](#14-a-projection-feeds-golive-waits-for-a-running-catch-up).
Then a reactor subscription handler, or the code that applies an event to a reactor projection feed, that feeds an
event back into its own subscription or feed no longer waits forever. The call is answered once the event is queued,
and when applying that event fails, the subscription or feed fails for good. Read
[section 15](#15-feeding-a-reactor-catch-up-from-its-own-handler-no-longer-waits-forever).
Then, on `NativeMongoSubscriptionModel`, a subscription made while the model is stopped now starts when `start()`
opens its change stream. A call that waits for it to start hangs when it runs before `start()` on the thread that calls
`start()`. Read
[section 16](#16-a-native-mongodb-subscription-made-while-the-model-is-stopped-starts-with-start).
Then a blocking catch-up subscription from `StartAtTime.offsetDateTime(..)` now also delivers the events stored at
the time you give, so passing the time of the last event you handled delivers that event again. Read
[section 17](#17-startattimeoffsetdatetime-includes-the-events-stored-at-that-time).
Then `CompetingConsumerSubscriptionModel.start(..)` and `resumeSubscription(..)` no longer throw what the lease
strategy or the wrapped model threw for a competing subscription. They log it and return, and the subscription is tried
again on a thread of its own. Read
[section 18](#18-a-competing-consumers-start-and-resumesubscription-log-a-failure-and-return).
Finally, `SpringMongoSubscriptionModel` no longer skips an event whose action keeps failing, forgets a subscription
whose history was lost when it is told not to restart it, and no longer builds on Spring Data's
`MessageListenerContainer`, which removes the `protected` constructor of `SpringMongoSubscription`. A
`DurableSubscriptionModel` over a MongoDB model also writes a checkpoint once a minute for a subscription that receives
no events. Read
[section 19](#19-springmongosubscriptionmodel-reads-its-own-cursor-and-a-quiet-durable-subscription-saves-its-position).

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

## 2. A flow saga's `stepWindow` now caps its declared events and the start type

No recipe, and most callers need to do nothing. This only matters if your flow sets a
`replacementFilter` wider than the flow's own declared types, or uses a `CloudEventTypeMapper` that
collapses several domain types onto one CloudEvent type string.

The 0.33.0 upgrade guide's [section 9](upgrading-to-0.33.0.md#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in)
and [section 10's replacement-filter caveat](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)
describe `stepWindow` as it shipped in 0.33.0, where every correlated event counted toward the cap
regardless of whether any step declared its type. That let an event outside a flow's own declared
types evict one of the step's own events, and the absolute bound section 9 states,
`historyWindow + 2 * stepWindow + 1`, held because of that same defect.

`stepWindow` now counts and evicts only events of a type some step's `on(...)` branch or
window-condition leaf actually names, plus an event of the type that starts the flow. An
event of any other type no longer takes one of the cap's slots or evicts a declared event to
make room for itself, but it can still be dropped. When the window advances past it to evict
enough declared events, it is swept out together with them, and `historyWindow` can drop it on a
later transition, once the step it arrived in has been left. The bound in section 9 still holds
for a flow's own declared-type events, the start type included. It no longer bounds a step fed only
events of a type no step declares and that is not the start type, which is not a new gap. It was
always the kind of growth `stepWindow` and `historyWindow` alone did not close, only masked. Watch
the 0.33.0 store-boundary warning if your flow admits such events and you care about total document
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
exclude everything under it, and widening can only exclude more. That walk starts at the declared type, follows
a `permits` clause through `Class.getPermittedSubclasses`, reads the constants of an enum that has no such
clause, and stops at the first level that is neither sealed nor an enum, so a type it cannot reach means an event you wanted out
stays in, rather than the reverse. That widening needs no migration step by itself. It still refuses an array or a
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
| A concrete class that is neither final nor sealed | `class OrderPlaced` | `open class OrderPlaced` |

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

### An enum with constant bodies is expanded into its constant classes

An enum closes its own hierarchy, since neither Java nor Kotlin lets anything outside the declaration extend an
enum type, so its constants are every class an instance can have. Declaring one is accepted, and so is declaring a
sealed event interface above one. On 0.33.0 a saga or an annotation-based subscription refused a Kotlin enum whose
constants have bodies, because Kotlin compiles that construct without the implicit sealing javac gives it, so this
is the one part of section 3 that accepts a declaration 0.33.0 rejected rather than the other way round.

```kotlin
// Accepted. Matches PaymentEvent$Reserved and PaymentEvent$Settled
enum class PaymentEvent : DomainEvent {
    Reserved { override fun toString() = "reserved" },
    Settled { override fun toString() = "settled" }
}
```

Which CloudEvent type a constant is stored under is decided by the constant's own class rather than by the enum. A
constant with a body has its own class, `PaymentEvent$Reserved`, while a constant without one is an instance of
`PaymentEvent` itself. So adding or removing a constant body changes the type an event is stored under, and
`ReflectionCloudEventTypeMapper` maps whichever class it is handed. Decide whether a constant has a body before you
have events in the store rather than after.


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
`evolve`, its `react` or its command dispatcher could not handle propagated to the subscription model, and wherever
that model offered the event again the saga tried again, without limit.

What the failing event holds up in the meantime is decided by whatever feeds the subscription, and the javadoc on
`SagaStatus.QUARANTINED` says what that can be.

From 0.34.0 the executor times the failing rather than counting the attempts. Where a quarantine budget is in force, an
instance's first failing event can write down the instant it started failing, and it rethrows whether or not that write
succeeds, exactly as before. Some ways of failing write nothing, a failing timeout among them, and the javadoc on
`SagaStatus.QUARANTINED` lists them. Where nothing was recorded, the next delivery decides on whatever the store holds
then. Once that instance has kept failing for at least `SagaRunnerConfig.quarantineAfter`, five minutes by default, it
can move to the new `SagaStatus.QUARANTINED`, and when it does the executor stops rethrowing.

Reaching the budget is not enough on its own. The javadoc on `SagaStatus.QUARANTINED` lists what else has to hold, so
an instance past its budget can still be `ACTIVE`. Read its status rather than working it out from the time.

The clock belongs to the instance rather than to one event. An instance where two events both fail keeps the earlier
instant and renames the record to whichever event failed last, so `SagaFailure.firstFailedAt()` is the start of that
instance's current run of failing and is not always the first time `SagaFailure.input()` failed. Reading it as an
event-specific clock would under-report how long the instance has been stuck.

The budget covers everything the executor does with an event once it knows which instance the event belongs to, not
the reaction alone. Checking for a redelivery, `evolve`, `react`, the dispatcher and the store all count, and an
`Error` counts like a `RuntimeException`. The one exclusion is `OutOfMemoryError`, which says the JVM ran out of
heap while some instance held the thread rather than anything about that instance, and nothing in 0.34.0 brings an
instance back out of quarantine.

One case has no instance to quarantine, and it keeps the 0.33.0 behaviour. An event whose converter or id extractor
throws never reaches an instance, so the subscription is never let past it, whatever the budget. It may still belong to
an instance, and once the subscription moved past it the next event for that instance would mark the instance as
having handled it, so the event would be lost. It is refused instead, and the first failure is logged at `WARN` and after that at `ERROR` once per interval, naming the event and what stopped it. That
interval is the quarantine budget when the saga has one, and a fixed five-minute default when it does not, so the
`ERROR` still repeats on a subscription model this saga cannot quarantine anything on, for as long as that model keeps
offering the event. A model that does not offer a refused delivery again gets only the first `WARN`, and
`DeliveryFailurePolicy` is where a consume-side broker bridge's choice is configured.
Where the event is offered again, repair the converter or the id extractor and the saga applies it in the order it was
written.

A quarantined instance receives no further events and fires no timers, and its redelivery watermarks stop moving, so
nothing it skipped is recorded as handled. What it stopped on stays on the record instead of being lost.

0.34.0 stops there. Nothing in it brings an instance back out of quarantine, so read `SagaInstance.failure()` to see
which input it stopped on and what the saga threw, and call `SagaStateStore.delete(sagaId)` to abandon the instance
once you have decided not to recover it. [The quarantined saga runbook](../runbooks/quarantined-saga-instance.md) has
the whole sequence, including the log lines that announce a quarantine and what deleting an instance costs against a
redelivery.

Two of the conditions on `SagaStatus.QUARANTINED` need explaining before you rely on quarantine.

**Quarantine is available only where the subscription model can say that acknowledging the failing event is not what would destroy the last copy of it,**
which it declares by implementing `HistoryRetainingSubscriptions`. `NativeMongoSubscriptionModel` and
`SpringMongoSubscriptionModel` hold everything they deliver and say so, including either of them behind
`DurableSubscriptionModel`, `CompetingConsumerSubscriptionModel` or `CatchupSubscriptionModel`, since a wrapper that
declares nothing itself is answered by the model it wraps. On a model that declares nothing at all, a bare
`PushSubscriptionModel` being the one you are most likely to meet, the runner switches the budget off at startup and
logs why, so the saga keeps the 0.33.0 behaviour of never quarantining. That model hands the acknowledge-or-redeliver
decision to the listener that called `accept`. What the failing event holds up is that listener's call as well.

A model that cannot promise to hold everything gets no quarantine either, even where it can answer for the event an
instance actually stopped on. `CatchupThenPushSubscriptionModel` is that case, since it replays an event store and
takes live events from a feed that may deliver events nothing here ever wrote. The reason is what a quarantine does
afterwards, which is that the instance goes inert and skips everything addressed to it, and skipping acknowledges.
Protecting the failing event alone would leave the ones behind it unprotected.

That is deliberate rather than an omission. Quarantining means returning normally, which acknowledges the event to
whatever fed it, and on a push feed behind a broker bridge that is what stages the offset and moves past the record.
The one copy this saga could ever be given would be gone at the moment of quarantine. Between an instance left active
and an event this saga would be acknowledging away, this refuses the acknowledgement and leaves what happens to the
event to whatever fed it.

**An event with no redelivery key is not quarantined either.** The failure record identifies the failing event by its
stream id with its stream version, or by its global position when it has no stream metadata. An event with neither
cannot be told apart from its own redelivery, so the budget could never elapse for it, and the saga keeps the 0.33.0
behaviour of never quarantining.

A feed that drops the Occurrent CloudEvent extensions on the way in is how an event ends up like that.
`SagaRunnerConfig.redeliveryDetection` already refuses such an event under `REQUIRED`, its default, before the saga
sees it, so you reach this case only after setting that to `BEST_EFFORT`.

An event store that assigns no global position is not one of these cases. A store built with
`EventStoreConfig.Builder.withoutStreamPosition()`, and an upgrade where stream position stays disabled on an existing
collection, both still give every event a stream id and a stream version, so a saga on such a store quarantines like
any other and `SagaInstance.failure().position()` answers `null` for it.

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
repository. It tells you what a quarantined instance stopped on, which is the failing event's redelivery key with its
position beside it when the store assigns one, the exception's class name and message, and when the instance started
failing, and it answers `null` for an instance that has no failure recorded. That does not mean the instance is not
failing. Several ways of failing record nothing, a failing timeout among them, and the javadoc on
`SagaStatus.QUARANTINED` lists them. `SagaEnvelope` implements it from its new `failure` component, so a store that carries that
component answers it for free.

**`SagaEnvelope` gains two record components, `started` and `failure`,** which changes its canonical constructor and
the arity of any record pattern over it. Only a `SagaStateStore` implemented outside this repository constructs one.
The old eleven-argument form is kept as a deprecated constructor that fills in `started = true` and `failure = null`,
so an existing call site compiles unchanged, but a store built that way can never report a quarantined instance.
Persist both components and read them back to support quarantine, and read a missing `started` field as `true`, since
every instance written before 0.34.0 had started. A record pattern has no such fallback and has to name the two new
components.

**`SagaStateStore` gains two `default` methods, `findWithoutState` and `compareAndSaveWithoutState`, and your store
compiles without them.** They both inherit to `find` and `compareAndSave`, so a store that ignores them behaves in
0.34.0 exactly as it did in 0.33.0. Override them if you want a quarantine to work on an instance whose state can no
longer be decoded, which a renamed event class or a changed converter produces. The executor decides and records a
quarantine through these two rather than through `find`, because loading such an instance throws, and an instance that
throws on every load records nothing and never reaches its budget.
In a store that overrides them, `findWithoutState` answers with an envelope whose `state` is `null` and every other
member populated, the way `findByStatus` already does, and `compareAndSaveWithoutState` saves under the same
compare-and-set rule while leaving the stored state where it is. That is the contract for an override and not what you
inherit. The defaults do the opposite, since `findWithoutState` delegates to `find` and hands the state back, and
`compareAndSaveWithoutState` delegates to `compareAndSave` and writes it. So override both or neither, because the
executor saves what it read, and a store that answers the read with no state and then writes the envelope whole erases
the state it was careful not to decode.

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
new behaviour. A record pattern over `SagaRunnerConfig` has to name the fifth component. Pass `null` to never
quarantine, so the saga keeps rethrowing for as long as the subscription model offers the event again, which is the
0.33.0 behaviour.

```java
SagaRunnerConfig config = SagaRunnerConfig.defaults().withQuarantineAfter(null);
```

On the annotation path you never build a `SagaRunnerConfig`, so the budget is a property instead. It defaults to five
minutes, and zero is how it says never, because a `Duration` property that is not set binds to its default rather than
to null.

```properties
occurrent.saga.quarantine-after=0
```

### Why there is no recipe for this one

None of the five can be rewritten mechanically. What your new `case QUARANTINED` branch should do depends on what the
`switch` is for, and whether a given `findByStatus(ACTIVE, ..)` call site wants quarantined instances included is a
question about that caller's intent rather than about the API. The two record-component additions could in principle
be rewritten, but a recipe that fixed those two and left the two that matter would read as a migration that had been
handled. This section is the migration.

## 9. A reactor catch-up subscription can deliver a concurrent write twice

A reactor catch-up subscription now delivers an event a second time when a write that was still in flight during the
replay was read by a history window. Before this release the cache suppressed that second delivery whenever the
event's id was still in it, and since it held the most recently replayed `handoverCacheSize` ids, for an event this
close to the head it was.

A position is reserved before its write commits, so a write in flight when the replay read the head holds a position
at or below that head, and a history window reads it even though it is not history. The replay used to put every id
it read into the cache the live delivery filters on, the history reads included, so the change stream's own delivery
of that event was dropped for as long as its id stayed in the cache. The history windows now fill no cache, which is what all three blocking paths already did,
so the live subscription delivers that event again. The reasoning is in
[ADR 135](../architecture/decisions/0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md).

This applies to `ReactorStreamCatchupSubscriptionModel` and `ReactorDcbCatchupSubscriptionModel`, both of which
shipped in 0.30.0, and to `ReactorCatchupSubscriptionModel`, which routes to them. The blocking catch-up models are
unchanged, because their history reads never filled that cache in the first place.

On a single-primary MongoDB, the case [ADR 135](../architecture/decisions/0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md)
verifies, the second delivery goes to an event a history window read whose write committed after the catch-up took
its live resume checkpoint. A store with nothing being written during the replay sees none at all, so an application
that rebuilds a read model offline is unaffected.

That bound rests on the resume checkpoint landing strictly past every event committed by then, and there are
deployments where it does not. Under a secondary read preference or a sharded `mongos`, MongoDB's `operationTime`
can lag entries already in the oplog, and the catch-up constructors take any `CheckpointAwareSubscriptionModel`, so
one of your own can answer whatever it likes. Where the checkpoint lags, the live stream delivers pre-replay history
too, and the cache no longer suppresses it, so the repeats reach as far back as the lag rather than covering
concurrent writes alone. That suppression was never the guarantee it looks like, because the cache held only the most recently replayed
`handoverCacheSize` ids and evicted the eldest, so a lag wider than the cache already produced these repeats before
this release. What is gone is the suppression of everything inside that window. The handler you need is the same one
either way.

Catch-up delivery on these models has always been at-least-once, and the same composition already re-delivers a whole
replay when a stopped catch-up is started again, so a handler written to tolerate a repeat needs no change. What
changes is that a repeat now actually happens on this path, for the writes described above, where before it did not.
If your reactor projection or subscription handler is not safe to run twice on the same event, one that increments a
counter or writes an unconditional insert for example, make it safe before upgrading. Keying the work by the
CloudEvent id is the usual way.

`handoverCacheSize` changes meaning along with it. It used to be filled by the whole replay and now sizes the
reconciliation overlap alone, which is what `cacheSize` on the blocking position catch-up models already means. A
value you raised to cover a large rebuild's history is now bigger than it needs to be. Nothing fails if you leave
it, the cache just holds fewer ids than it has room for.

`handoverCacheSize` reaches the position catch-up models and nothing else. The catch-up-then-push handover has a
setting of its own, `CatchupThenLiveOptions.dedupCacheSize`, and this section does not change what that one does.

There is no recipe for this change. Nothing in your source code declares a requirement that an event arrives once, so
there is nothing a rewrite could search for.
## 10. Events `updateEvent` damaged before 0.34.0 need a one-off repair

There is no recipe for this. It is not a code change, it is stored data that needs fixing, and only if your
application called `EventStoreOperations.updateEvent` while running 0.33.0 or earlier.

Up to and including 0.33.0, `updateEvent` rebuilt the stored document through the stream-only mapper, which writes
`position` through the general CloudEvent extension writer. That writer has no `Long` overload, so `position` came
back as a string instead of a number, and the indexed `dcbTags` array was dropped entirely. 0.34.0 fixes the write
path on all three MongoDB stores. It does not repair events that are already stored.

MongoDB compares values within a type, so a string `position` matches neither end of a numeric range. An event
damaged this way is missing from DCB reads, from `exists` and `count`, from position-ordered stream reads in both
directions, from position-based catch-up, and from the conflict query behind a conditional append, where it means
an append that should have been refused is accepted. Nothing raises an error at any point.

Note that the position half of this affects a store with stream position enabled even if it never used DCB.

### How to tell whether this is you

One query, which uses the `position` index and is cheap on a large collection:

```javascript
db.events.countDocuments({ position: { $type: "string" } })
```

Replace `events` with your event collection name. From 0.34.0 a store that writes position runs the same check when
it starts and logs a warning naming the repair when it finds something, so an affected store of that kind tells you
on its next deploy. By default a store that writes no position does not run it, so run the query above yourself
there unless you turn on the setting below.

If you would rather that store refused to start than kept accepting conditional appends against a damaged event, set
`EventStoreConfig.Builder.requireRepairedEvents(true)`. It refuses while any event's position is not a positive
integer or is above the store's position counter, while any DCB event's `dcbtags` has an empty line or whitespace
around a tag or its `dcbTags` array does not hold the tags `dcbtags` lists, while an event without `dcbtags` has a
`dcbTags` field, and while the counter is negative or is not the int32 or int64 every writer stores. A missing counter document
counts as zero, since every read takes it to be zero. That takes in the query above, a DCB event whose position was
dropped, a position set by hand above the counter or at or below zero, a `null`, `NaN` or array position, a tag
array that is missing or names other tags, and an event collection renamed without its `_position` collection. The checks in step 6 of the [repair runbook](../runbooks/update-event-repair.md)
are the ones it runs, and a startup that finds no damage reads the whole collection. It can also keep refusing after the repair has run, over an event the repair could not fix, until
you fix that event by hand or turn the setting off. It is off by default on all three MongoDB stores, so upgrading on its own changes
nothing here. It also covers the third message below, the store that turns position off and would otherwise run no
damage check at all.

An event whose position was dropped rather than turned into a string has no `position` field at all. Your store
already warns about events without a position, but that warning names the position backfill, which is the wrong
remedy here and will not fix it. If you see it and you have also called `updateEvent`, run the second query in the
[repair runbook](../runbooks/update-event-repair.md) before assuming your history predates position.

There is a third message worth knowing about. If stream position is on only by default, and the oldest event in the
collection has no `position`, the store turns position off for itself and warns instead of checking anything further.
That one event is enough, so a single event whose position `updateEvent` dropped puts an otherwise healthy store on
that path, where it is the only warning you get. Every one of these messages names the repair runbook for that
reason.

### What to do about it

Run the `occurrent-eventstore-mongodb-update-event-repair` module. The
[runbook](../runbooks/update-event-repair.md) has the full sequence, and the
[module README](../../eventstore/migration/update-event-repair/README.md) covers the options.

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
UpdateEventRepair repair = new UpdateEventRepair(database, "events", UpdateEventRepairOptions.defaults());
UpdateEventRepairReport report = repair.report();   // counts the damage, writes nothing
UpdateEventRepairResult result = repair.run();      // repairs it
```

The repair only touches events that still look damaged, so running it twice is safe, and it resumes from a
checkpoint if it is killed part way.

### What it will not fix, and you should know before you run it

The repair rebuilds an event from what its document still holds. Where the old write-back destroyed the only copy
of a value, the tool reports the event by `_id` rather than inventing one. Six cases end up there: a position that
was never stored, a position another event already holds, a `position` string that is not a number, one holding zero
or a negative number, one above the store's position counter, and a document whose `dcbtags` cannot be read back into
a tag set. The zero-or-negative and above-the-counter cases are values no store ever assigns. The runbook says what
to do about each.

A position the tool does restore is the value the document holds, not one it can check. The old write-back kept
whatever position the update function returned, so a function that set `position` itself left that number behind as a
string like any other. A forged value another event already holds is refused by the unique index, and a forged zero,
negative, or above-the-counter one is reported, but a positive value inside the assigned range that happens to be free
is indistinguishable from the event's own.
The tool restores it and counts a repair. If your update functions set `position`, a clean repair is not the same as
the positions being right, and you need an external record. If they left `position` alone, every restored position
came from the event itself.

One case is invisible even to the tool. If an update function returned a replacement event built from scratch,
without the `dcbtags` extension, the document no longer looks like a DCB event and nothing distinguishes it from an
ordinary stream event. If the extension was replaced rather than dropped, the repair rebuilds the tag array from
the replacement tags, since that is all the document has left. If you know you ran an update function that built
replacement events from scratch over DCB events, you need an external record of what those events should be.

## 11. A subscription handler Spring cannot invoke now fails startup, and a live subscription can miss a startup write

Two behavior changes ship together with the transactional-advice fix for [#837](https://github.com/johanhaleby/occurrent/issues/837)
and [#965](https://github.com/johanhaleby/occurrent/issues/965).

A `@Subscription`, `@StreamSubscription`, `@DcbSubscription` or `@SynchronousSubscription` handler method Spring's
proxy cannot invoke now fails Spring Boot startup with `SubscriptionHandlerNotInvocableException`. Before this release
it ran on the raw bean instead, with no advice applied, silently skipping `@Transactional` or any other aspect on
every delivery, which is the bug #837 and #965 report. A method declared only on the concrete class while the bean
is a JDK interface proxy, a private method a CGLIB proxy cannot override, and a final method a CGLIB proxy cannot
override either, all hit the new check, but only once the bean actually ends up behind such a proxy. An unproxied
bean has no proxy to lose advice through in the first place, so a private or final handler there is unaffected and
still registers normally. A static method hits the same check unconditionally, proxied or not. `Method.invoke`
dispatches a static method on its declaring class alone, regardless of the target object passed to it, so invoking
one never goes through a proxy at all, whether or not the bean has one. Make the method non-private, expose it on an interface the
proxy implements, drop `final` or `static`, or set `spring.aop.proxy-target-class=true` so a CGLIB proxy is used
instead of a JDK interface proxy.

Registration for all four annotations also moves to the phase `@Projection`, `@Snapshot` and `@Saga` already use,
once every singleton in the application is instantiated. Before this release each handler registered during its own
bean's creation, so whichever bean the container happened to construct first could already be live while a later
bean was still writing an event from its own `@PostConstruct`. Whether that write reached the handler depended on
bean creation order, an accident of the bean graph rather than something you configured. Every handler now registers
only once singleton construction has finished, so a live-only subscription, `@Subscription`, `@StreamSubscription`
or `@DcbSubscription` at `StartPosition.NOW`, and every `@SynchronousSubscription`, never sees an event written
during singleton construction, `@PostConstruct` included, regardless of creation order. `StartPosition.NOW`'s own
contract was already "events written after the subscription starts", so this does not break a documented promise,
it closes a gap the old, order-dependent timing sometimes closed by accident and sometimes did not.
`StartPosition.DEFAULT` is unaffected on a restart, since a durable subscription then resumes from its stored
checkpoint and can still replay such an event, so only `NOW` is the position this section's guarantee actually
covers, and that guarantee itself only reaches singleton construction. `afterSingletonsInstantiated`, the callback
every one of these now registers from, runs before a later startup phase such as an `ApplicationRunner` and before
a bean the container creates after registration, one marked `@Lazy` for example, so a write from either of those is
delivered normally, not missed the way a singleton construction write is. If your startup code writes an event
during singleton construction that a live subscription needs to see, move that write into an `ApplicationRunner` or
a similar later phase, or start the subscription at `StartPosition.BEGINNING` and let it catch up explicitly.
[#979](https://github.com/johanhaleby/occurrent/issues/979) tracks recording an early position marker during startup
so a future release can close this without giving up the deferred proxy resolution this section's first half
depends on.

There is no recipe for either change. A proxy-invocability failure and a startup ordering dependency are both
runtime behavior, not a call site a rewrite could search for.

## 12. A projection feed's `accept(..)` waits until the event is applied, and fails when it is not

This covers `CatchupProjectionFeed.accept(..)` and `DomainEventFeed.accept(..)` on both stacks, fixed for
[#1135](https://github.com/johanhaleby/occurrent/issues/1135). The reactor stack changes in one case only, covered
after the blocking cases. In 0.33.0 an event fed before the blocking feed went live was
put in a buffer and `accept(..)` returned straight away. A listener acknowledges the message once `accept(..)`
returns, so the broker discarded an event that was only held in memory, and a stop or a crash before the catch-up
finished lost it.

Before the feed goes live, and while a catch-up runs on a feed that already went live, `accept(..)` now waits until
the catch-up has applied the event. In 0.33.0 a catch-up on a live feed did not hold live events back, so
`accept(..)` applied the event on the calling thread and returned. When the feed is live and no catch-up runs, the
event is still applied on the calling thread.

It's important to keep in mind that you must never call `accept(..)` on the thread that later calls `catchUp()`,
`catchUpAll()` or `goLive()`. That thread waits for a catch-up it never gets to start, until another thread runs the
catch-up, takes the feed live, calls `stopCatchUp()` or interrupts it. A test or a startup routine that does this on
one thread now hangs:

```java
feed.accept(event);
feed.catchUp();
```

Run the catch-up on a thread of its own, or feed the event once the catch-up has returned.

`accept(..)` now throws an `IllegalStateException` whenever the event was not applied. It throws in these cases:

- the catch-up was stopped before the feed went live, or the feed was stopped before any catch-up started
- the catch-up failed
- the waiting thread was interrupted
- it was called while the feed was not live from inside the projection, a view or another callback of the same
  feed, where the thread would wait for work it holds up itself
- another delivery of the same event is still running on another thread

In 0.33.0 a stopped feed and a second delivery of an event still being applied both returned normally, so the
listener acknowledged an event that nothing had applied. Do not acknowledge the message when `accept(..)` throws,
and the broker delivers it again. A listener that acknowledges only after `accept(..)` returns, and lets an
exception reach the broker client, needs no change.

In 0.33.0 an event fed while the feed was not live from inside the projection, a view or another callback of the
same feed was buffered and applied once a catch-up took the feed live. Now it is refused. Thrown while the catch-up replays history into the projection or a view,
that refusal fails the catch-up, and the feed refuses every event until you build a new one. A caller that catches
the refusal and continues drops the nested event.

A long replay keeps the listener thread waiting. A Kafka consumer that waits past its `max.poll.interval.ms`, five
minutes by default, is taken out of its consumer group, and the record is delivered again once the partition is
reassigned. RabbitMQ closes the channel of a consumer that holds on to a message without acknowledging it for longer
than `consumer_timeout`, 30 minutes by default, and delivers the message again. Both cost a redelivery, not the event.

On the reactor stack, `CatchupProjectionFeed.accept(..)` and `DomainEventFeed.accept(..)` already returned a `Mono`
that completes once the event is applied. That `Mono` now errors with an `IllegalStateException` for an event fed
while the feed is stopped, after a catch-up was stopped before the feed went live and before the next one starts.
0.33.0 completed it without applying the event, so the listener acknowledged an event nothing had applied. Do not
acknowledge the message when the `Mono` errors.

`DomainEventFeed.acceptCloudEvent(..)`, which the Kafka and RabbitMQ bridges call, is unchanged.

There is no recipe for this change. Which thread calls `accept(..)` and what a listener does when it throws are
runtime behavior that a rewrite of the source cannot see.

## 13. Only the in-memory event store's write path may feed a push model's `accept(..)`

Up to 0.33.0 the javadoc of both `PushSubscriptionModel` classes named a RabbitMQ or Kafka listener, a Spring
application event and an HTTP endpoint as sources that hand events to `accept(..)`, and it called the write path one
where the event is already durably stored. In 0.34.0 `accept(..)` is supported only for the listener of an
`InMemoryEventStore`. Neither push model records which events a subscription has handled, so an event fed through
`accept(..)` from a source that cannot deliver it again is lost when the application crashes before the handler has
run. The in-memory event store loses the event in that crash too, which is why it is the one exception. See
[#1140](https://github.com/johanhaleby/occurrent/issues/1140) and the amendment to
[ADR 133](../architecture/decisions/0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md).

What to use instead depends on where the events come from:

- A RabbitMQ or Kafka listener calls `acceptRedeliverable(CloudEvent)` and acknowledges the message only when the
  outcome is `DELIVERED` or `FILTERED`. `RabbitMqCloudEventBridge` and `KafkaCloudEventBridge` already call it.
  Under `DeliveryFailurePolicy.PARK` they also acknowledge a failed message, once its republish to the parking
  destination is confirmed.
- An HTTP endpoint whose caller retries a failed request calls `acceptRedeliverable(CloudEvent)` too, and answers
  by the outcome's `disposition()`. `ACKNOWLEDGE`, for `DELIVERED` and `FILTERED`, is a success. `HOLD`, for
  `DEFERRED` and `UNAVAILABLE`, is an error the caller retries later, a 503 say. `FAIL`, for `NOT_DELIVERABLE`, and
  an exception out of the call go to the endpoint's own failure policy. `STOP`, for `REFUSED`, is an error the
  caller must not retry, because the same event gets the same answer until the subscription is cancelled and
  subscribed again.
- A listener on the write path of a durable event store, such as MongoDB, is replaced by a durable subscription,
  which records the position it has handled and resumes from it after a restart. Forwarding the events to a broker
  whose listener calls `acceptRedeliverable(CloudEvent)` works as well.
- A Spring application event is delivered once, in memory, so it cannot deliver an event again after a crash either.
  If the application publishes it for an event a durable event store has already committed, subscribe to that store
  with a durable subscription instead. If the event is in an `InMemoryEventStore`, feed `accept(..)` from that
  store's listener instead.

Two sources have no supported replacement, an HTTP endpoint whose caller does not retry and a Spring
application event for an event that no event store holds. Neither can deliver an event again after a crash, so
Occurrent supports no way to feed a push model from either.

An `InMemoryEventStore` listener needs no change on the blocking stack. On the reactor stack the listener has to
subscribe to the `Mono` that `accept(..)` returns and wait for it, `events -> pushModel.accept(events).block()` for
example, on a thread that may block.

There is no recipe for this change. Where a listener's events come from is not something a rewrite of the source can
see.

## 14. A projection feed's `goLive()` waits for a running catch-up

This covers `CatchupProjectionFeed.goLive()` and `DomainEventFeed.goLive(id)` on both stacks. In 0.33.0 a `goLive()`
called while a catch-up of the same projection was still replaying returned without waiting for that replay, and on
the reactor stack its `Mono` completed without waiting. Now it returns, or its `Mono` completes, only once that replay
has ended.

It can now also fail because of a catch-up. When a catch-up of the same projection failed while it waited, that
replay or another one, the blocking feeds throw an `IllegalStateException` and the reactor feeds' `Mono` errors with
one. Its cause is a catch-up failure recorded while it waited.

A call the view makes while the feed is calling it, from the code that applies an event to the view or from a
callback such as `replayStarted()`, still returns without waiting for the replay, since that replay cannot end before
the call does.

You are affected in these cases:

- Startup code that runs `catchUp()`, `catchUp(id)` or `catchUpAll()` on a background thread and calls `goLive()` while
  it replays now waits for that replay.
- Code that calls `goLive()` next to a catch-up and does not expect it to throw can now see the catch-up's failure.
- On the blocking stack, a view that hands a `goLive()` call to another thread during a replay and waits for it there
  waits for a replay that cannot end while the view waits. On the reactor stack the same holds for a view that blocks
  on the `Mono` from a thread it switched to while a replay holds live delivery back.

What to do:

- A catch-up takes the feed live when its replay completes. A `goLive()` called while it replays also takes the feed
  live when that replay is stopped, so keep the `goLive()` if you rely on that. If the thread calling it must not
  wait for the replay, call `goLive()` on a thread that can, or call it after `stopCatchUp()`.
- Call `goLive()` from the view's own thread rather than from one it hands the call to.
- Treat the `IllegalStateException` like a failed catch-up, and build a new feed.
- On the blocking stack an interrupt ends the wait early, and `goLive()` returns with the interrupt still set on the
  thread.

There is no recipe for this change. Which thread calls `goLive()`, and whether a catch-up runs next to it, are runtime
behavior that a rewrite of the source cannot see.

## 15. Feeding a reactor catch-up from its own handler no longer waits forever

This covers the reactor `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` and `DomainEventFeed`. Up to
0.33.0 a subscription handler that fed an event back into its own subscription waited for that event, and the event
waited for the handler to return, so both waited forever. The same held for the code that applies an event to a
projection feed, the `fold` you pass to `CatchupProjectionFeed.create(..)` or `DomainEventFeed.register(..)`, feeding
the same feed. It happened live and during a replay, whether the handler blocked on the call or returned it as part of
its `Mono`.

Now the call completes once the event is queued. The event is applied once the handler or fold has returned, in the
order it was fed, and an event fed during the replay once the subscription or feed has gone live. A replay that ends
before that writes no catch-up marker, so the next replay hands the handler or fold the same history again. That holds
for `PushSubscriptionModel.accept(..)` and `acceptRedeliverable(..)` called from the handler, and for
`CatchupProjectionFeed.accept(..)`, `DomainEventFeed.accept(..)` and `DomainEventFeed.acceptCloudEvent(..)` called from
the fold.

`acceptRedeliverable(..)` and `acceptCloudEvent(..)` decide between `DELIVERED` and `DEFERRED` the same way they do
for any other caller. During a replay that is `DEFERRED`. When live they report `DELIVERED` once the event is queued.
A handler or fold that used the call's completion, its error or its `DELIVERED` to learn that the event was applied
now learns only that it was queued.

When applying one of these events fails, nobody is waiting for it any more. The subscription or feed then starts
failing. It deletes its catch-up marker, refuses every later event from anywhere else with that failure, applies the
events it has already taken in and those its handler or fold feeds it meanwhile, and then fails for good. In 0.33.0 the
failure went only to the call, so a handler that did not wait for it lost the event, and the next event was applied.

A failed catch-up starts the subscription or feed failing the same way, whether its replay, its marker read or its
marker write failed, so an event its handler or fold queued before that failure is still applied. An event from anywhere
else that is still waiting gets the failure instead and is not applied. The marker is deleted in that case too. So when
the marker read fails on a subscription that had already caught up, the next catch-up replays the whole history, where
0.33.0 skipped it.

What to do:

- After a failure, fix its cause, then cancel the subscription and subscribe again, or build a new feed. Once the
  catch-up marker is gone, its catch-up replays the history, the failed event and the events after it included. An
  event that no replay can bring back is lost only when applying it failed, as in 0.33.0.
- When deleting the marker still fails after 3 retries, the subscription or feed logs an error naming its id. Delete
  the marker stored under that id in the `CheckpointStorage` you passed for catch-up markers before you subscribe again
  or build the new feed, since a marker left in place makes that catch-up skip the replay.
- To have the call recognized, subscribe it on the thread the handler or fold was called on, by blocking on it for
  example, or return it as part of the `Mono` the handler returns. A handler that blocks on the call from a thread it
  switched to still waits forever.

There is no recipe for this change. Whether a call runs inside a handler of the subscription it feeds is runtime
behavior that a rewrite of the source cannot see.

## 16. A native MongoDB subscription made while the model is stopped starts with `start()`

This covers `NativeMongoSubscriptionModel`. In 0.33.0 `subscribe(..)` opened a change stream even when the model was
stopped. That change stream delivered events while `isPaused(..)` returned `true`, and `start()` opened a second one
next to it, so every event written after `start()` arrived twice.

Now a subscription made while the model is stopped opens no change stream until `start()` or `resumeSubscription(..)`
starts it. That changes when a wait for it returns.

### A wait for the subscription to start waits for `start()`

`waitUntilStarted()` on the `Subscription` that `subscribe(..)` returns now returns once `start()` or
`resumeSubscription(..)` has opened its change stream. In 0.33.0 it returned once the change stream that `subscribe(..)`
opened was open. `SpringMongoSubscriptionModel` already waited this way in 0.33.0.

So any call that waits for the subscription to start hangs when it runs between `stop()` and `start()` on the thread
that later calls `start()`. That thread waits until it is interrupted. Many DSL and runner calls wait by default, for
example the Kotlin subscription DSL's `subscribe(..)`, `ProjectionRunner.project(..)` and `SagaRunner.run(..)`.

What to do:

- Call `start()` before anything waits for the subscription to start, or subscribe from another thread.
- Where a call takes a `waitUntilStarted` flag, passing `false` makes it return without waiting. Call
  `waitUntilStarted()` on the `Subscription` it returns once `start()` has returned.
- `waitUntilStarted(Duration)` returns `false` once the timeout has passed, so a wait with a timeout ends on its own.

With `StartAt.now()`, without a `StartAt`, or with a dynamic `StartAt` answering the present, the subscription starts
at MongoDB's operation time, which `subscribe(..)` asks for on the model's executor without waiting for the answer. So
where it starts is fixed when MongoDB answers, shortly after `subscribe(..)` returns, and an event written before then
isn't delivered to it. To be sure an event is delivered, call `start()` and wait for `waitUntilStarted()` on the
subscription before writing it. `start()` waits for the answer when it opens the change stream at it. While MongoDB
can't be reached, the question is retried with the model's `RetryStrategy`, as opening the change stream is. When the
strategy gives up, the give-up can keep the change stream from opening, and pausing and resuming the subscription after
that starts it again.

There is no recipe for this change. Whether the model is stopped when `subscribe(..)` runs is runtime behavior that a
rewrite of the source cannot see.

## 17. `StartAtTime.offsetDateTime(..)` includes the events stored at that time

This covers the blocking `CatchupSubscriptionModel` and `StreamCatchupSubscriptionModel`. In 0.33.0 a subscription
from `StartAtTime.offsetDateTime(time)` replayed the events stored after `time`. Now it replays the events stored at
`time` as well.

The same goes for the time position a replay stores. Several events can share that time down to the millisecond, and in
0.33.0 a restart read only the events after it, so the other events stored in that millisecond were never delivered.

What to do:

- If you pass the time of the last event you handled, to resume where you left off, that event is now delivered again.
  Make the handler safe to run twice on it, or skip it by its id.
- Adding a millisecond to that time instead skips every other event stored in the same millisecond, which is the loss
  this change fixes.

There is no recipe for this change. The time you pass is a runtime value that a rewrite of the source cannot see.

## 18. A competing consumer's `start(..)` and `resumeSubscription(..)` log a failure and return

This covers `CompetingConsumerSubscriptionModel`. In 0.33.0 `start(..)` and `resumeSubscription(..)` threw when the
lease strategy or the wrapped model threw for a competing subscription, for example when MongoDB could not be reached
while `start(..)` registered a subscription for its lease. The subscription then stayed where the failure left it until
you called again.

1. Both now log the failure as a warning and return. A thread of its own tries the subscription again, with the backoff
   the MongoDB lease strategies use by default, until it is registered for its lease and runs only while this node
   holds it. Every fifth try that fails is logged as a warning.
2. `start(..)` still throws the first failure of a subscription that does not compete. When another call for that
   subscription is under way, `start(..)` returns instead, and a thread of its own tries the subscription again once
   that call has returned, until it succeeds. A pause, resume or cancel of the subscription made while it still fails
   ends those tries and is made all the same. `stop()`, `pauseSubscription(..)` and `cancelSubscription(..)` still
   throw what failed in their own call.
3. Remove code that caught the exception from `start(..)` or `resumeSubscription(..)` to call again. The thread does
   that now.
4. To find out whether a subscription runs, call `isRunning(id)`. It asks the wrapped model, which runs the
   subscription only on the node that holds its lease. `isPaused(id)` returns `true` for a subscription that
   `pauseSubscription(..)`, `stop()` or the loss of its lease paused. A subscription for which both return `false` is
   waiting for its lease, or is still being tried again.

There is no recipe for this change. Whether the lease strategy or the wrapped model throws is runtime behavior that a
rewrite of the source cannot see.

## 19. `SpringMongoSubscriptionModel` reads its own cursor, and a quiet durable subscription saves its position

`SpringMongoSubscriptionModel` now reads each change stream with the cursor loop `NativeMongoSubscriptionModel` uses,
instead of Spring Data's `MessageListenerContainer`. Both models report the position a subscription has read to when
no event matched its filter, and `DurableSubscriptionModel` saves it. Go through the list below, since most of it needs
nothing from you.

### An event whose action keeps failing is no longer skipped

In 0.33.0, when the action still threw after the `RetryStrategy` gave up, `SpringMongoSubscriptionModel` went on to the
next event, and a `DurableSubscriptionModel` over it then stored a position past the event that failed. The event was
lost.

Now the model restarts the change stream from the event before it and delivers the failing event again. The default
`RetryStrategy` never gives up, so this only concerns `RetryStrategy.none()`, which gives up at the first failure, or
a strategy with `maxAttempts(..)` or a `retryIf(..)` predicate. With such a strategy the restart gives up too, the
give-up is logged as an error, and the subscription delivers nothing more until you pause and resume it.

If you relied on the skip to get past an event the action cannot handle, catch the exception in the action and decide
there what to do with the event.

### After lost history with restarting turned off, the subscription is gone

With `restartSubscriptionsOnChangeStreamHistoryLost(false)`, a subscription whose position the oplog no longer holds
is now removed from the model. `isRunning(id)` and `isPaused(id)` return `false`. In 0.33.0 it still counted as
running.

To start it again, call `subscribe(..)` with the same id and a position the oplog still holds. Code that called
`pauseSubscription(id)` and `resumeSubscription(id)` for this now gets `UnknownSubscriptionException`.

### `SpringMongoSubscription` has no `protected` constructor

The constructor took a Spring Data `Subscription`, which the model no longer has. A subclass of
`SpringMongoSubscription`, or code that created one, stops compiling. Use the `Subscription` that `subscribe(..)`
returns. `SpringMongoSubscription` and `SpringMongoSubscriptionModel` also no longer override `equals` and `hashCode`,
so two instances are equal only when they are the same object.

### The default executor belongs to the model

Each `SpringMongoSubscriptionModel` now makes its own executor when you pass none, also for `useVirtualThreads()`, and
shuts it down in `shutdown()`. An action that is running then gets five seconds to return before it is interrupted.
In 0.33.0 the default executor was never shut down. An executor you pass with
`SpringMongoSubscriptionModelConfig.executor(..)` is still yours to shut down.

`subscribe(..)` on a model that is shut down throws `IllegalStateException`.

### A subscription made before `start()` receives what was written before `start()`

A subscription at `StartAt.now()`, or with the model default, made while the model is stopped or on a model created
with `autoStartup(false)`, now starts at the operation time MongoDB answers with when `subscribe(..)` asks. In 0.33.0
it started where the change stream was when `start()` opened it, so the events written in between were skipped. They
are delivered now, which is more than the action received before.

### A pause waits for an action that is running, and the model checks before each attempt

In 0.33.0, `pauseSubscription(..)` and `stop()` did not wait for an action that was running, and the model could hand
the action an event the change stream had already read after `pauseSubscription(..)` or `cancelSubscription(..)` had
returned. Now the model checks right before each attempt of the action, a retry included, and makes no attempt once
they have closed the subscription, so that event is delivered after the resume instead. A cancel doesn't wait for an
attempt that passed the check, so that attempt can still start just after `cancelSubscription(..)` has returned. The
`RetryStrategy`'s `onError` isn't called for a retry that was skipped this way, since the action didn't fail.

A `DurableSubscriptionModel` reads the version to write the checkpoint with after that check and before it calls your
action. So through it your action can still be called once after `cancelSubscription(..)` has returned, or after
`pauseSubscription(..)` has stopped waiting for that read. No event is lost this way, and after a cancel the checkpoint
of that call is not saved.

`pauseSubscription(..)` waits up to a second for an action that is running, and `stop()` waits one second for all of
them together. An action that takes longer can still be running when they return. A pause called from inside the
action does not wait. An interrupt doesn't end the wait, so the subscription is paused when they return, and the
interrupt is set on the thread again. While a pause waits, a call for another subscription doesn't wait for it.

Neither waits for a read that is waiting on the server, in `SpringMongoSubscriptionModel` or in
`NativeMongoSubscriptionModel`, so the thread of a paused subscription can stay busy for up to `maxAwaitTime` after
they return, and for as long as an action still runs once the pause has stopped waiting for it. So the model needs a
thread for each running subscription, and one more for each closed run that is still reading or still running its
action. Every pause and resume, and every `stop()` and `start(true)`, can add such a run, so no fixed number of threads
is always enough.

If you pass an executor with a fixed number of threads and it has no thread free for a resume, the model hands the
subscription to it again, 100 ms and then up to 2 seconds apart, until it takes it, the subscription is paused or
cancelled, or the model shuts down. The subscription counts as running meanwhile. So a smaller executor delays the
resume rather than leaving the subscription paused. A `subscribe(..)` the executor has no thread for still throws.

### A quiet subscription's checkpoint is written once a minute

A `DurableSubscriptionModel` over `SpringMongoSubscriptionModel` or `NativeMongoSubscriptionModel` now saves the
position of a subscription that has had no checkpoint saved for a minute. It uses the same `CheckpointStorage` and the
same write condition as for an event. A subscription that stores a checkpoint for an event at least once a minute
gets no extra write.

The save follows your persist predicate. Nothing is saved while the event the running subscription most recently gave
your action is one the predicate declined to store, since the saved position would come after that event. Nothing is
saved while an event is being delivered either. After a pause and a resume, an action of the paused run that is still
running keeps the save off until it returns, for as long as that takes. Before the first event after a subscribe, the
position is saved whatever the predicate is.

So with a predicate that declines some events, such as `EveryN` with `n` above 1, a subscription that goes quiet right
after a declined event gets no position saved until the predicate stores one. If it stays quiet for longer than the
oplog window, a restart still ends in lost history.

If you widen the filter of a durable subscription and keep its id, it now resumes from the last quiet position it
saved, so the events before that position that the old filter didn't match are not delivered. Before, it resumed after
the last event the old filter matched, and received them. When you widen a filter, use a new subscription id, or
subscribe once with a `StartAt` for the position to start from.

Change the interval with `saveQuietPositionEvery(Duration)` on `DurableSubscriptionModelConfig`, and keep it well
below the oplog window. `neverSaveQuietPosition()` turns the save off, and the stored checkpoint of a subscription
that matches nothing for longer than the oplog window is then a position MongoDB can no longer start from. The Spring
Boot starter has no property for the interval, so define your own `SubscriptionModel` bean to change it there.

There is no recipe for these changes. The removed constructor has no replacement to rewrite to, and the rest is runtime
behavior that a rewrite of the source cannot see.
