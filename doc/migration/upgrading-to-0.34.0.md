# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

The guide has twenty-three sections, five of them about compile-time breaks. At compile time, if you use the flow saga's
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
Then `SpringMongoSubscriptionModel` no longer skips an event whose action keeps failing, forgets a subscription
whose history was lost when it is told not to restart it, and no longer builds on Spring Data's
`MessageListenerContainer`, which removes the `protected` constructor of `SpringMongoSubscription`. A
`DurableSubscriptionModel` over a MongoDB model also writes a checkpoint once a minute for a subscription that receives
no events. `ReactorMongoSubscriptionModel` reads the driver's change stream cursor itself for a subscription with an
id, which changes what a test that mocks `ReactiveMongoOperations` has to stub. Read
[section 19](#19-springmongosubscriptionmodel-reads-its-own-cursor-and-a-quiet-durable-subscription-saves-its-position).
Then `CompetingConsumerSubscriptionModel.isRunning()` now says whether the model is started, and no longer returns
what the wrapped model returns, which after `stop()` and a `start(..)` that won no lease was `false`, unless the model
had a subscription that doesn't compete. A competing subscription you paused directly on the wrapped model also runs
again on the competing consumer model's next `start(..)`, a grant of its lease or a resume of it. Read
[section 20](#20-a-competing-consumers-isrunning-says-whether-the-model-is-started).
Then a `ReactorMongoSubscriptionModel` subscription started at the present now starts from the moment
`subscribe(..)` is called. It can receive events written up to 16 seconds before the call, and one whose change stream
first opens after its history is gone stops, unless you configure the model to restart it. A
`ReactorDurableSubscriptionModel` subscription at `StartAt.now()` over a model of your own that is not a
`SubscriptionModel` now starts from the `subscribe(..)` call too. Read
[section 21](#21-a-reactive-mongodb-subscription-started-at-the-present-starts-from-the-subscribe-call).
A new `CompetingConsumerSubscriptionModel` over a wrapped model that is not running runs a competing
subscription only once you call its own `start(..)`, and calling `start()` on the wrapped model instead never gets one
running. Read [section 22](#22-a-competing-consumer-over-a-wrapped-model-that-is-not-running-waits-for-its-own-start).
Finally, the reactor `cancelSubscription(..)` returns a `Mono<Void>`, which is a fifth compile-time break for a class
that implements it. Read
[section 23](#23-a-reactor-cancelsubscription-returns-a-mono-that-completes-once-the-stored-state-is-deleted).

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
2. `start(..)` still throws the first failure of a subscription that does not compete, and a failure to start the
   wrapped model, see [section 20](#20-a-competing-consumers-isrunning-says-whether-the-model-is-started). When another
   call for a subscription that does not compete is under way, `start(..)` returns instead of throwing what fails for
   it, and a thread of its own tries the subscription again once that call has returned, until it succeeds. A pause,
   resume or cancel of the subscription made while it still fails ends those tries and is made all the same. It doesn't
   end the tries to start the wrapped model, which go on until one succeeds, or until `stop()` or `shutdown()`.
   `pauseSubscription(..)` and `cancelSubscription(..)` still throw what failed in their own call, and item 3 says what
   `stop()` throws. A pause, resume or cancel first applies each `start(..)` and `stop()` that began before it and is
   still waiting to be applied, also one that has not got to that subscription yet. What fails there for a competing
   subscription is left to the thread from item 1, as it is when the `start(..)` or `stop()` gets there itself. One that
   fails for a subscription that does not compete, or throws an `Error`, is given up. That `start(..)` or `stop()`
   doesn't throw the failure, which is logged as a warning. When the failure is an `Error`, the pause, resume or cancel
   throws it once its own call is made. A pause, resume or cancel made from inside a call this model makes to the lease
   strategy or the wrapped model for that subscription applies none of them first. `stop()` gives up no other call.
3. For a competing subscription, remove code that caught the exception from `start(..)` or `resumeSubscription(..)` to
   call again. Neither throws what the lease strategy or the wrapped model threw for that subscription now, unless it is
   an `Error`, and the thread from item 1 makes the call again. Keep that code for a subscription that does not compete.
   `resumeSubscription(..)` still throws its failure for it, and so does `start(..)` when no other call for that
   subscription is under way, and this model doesn't make the call that failed again. `stop()` throws what failed for a
   competing subscription when it gets to that subscription itself, and when another call applied it there first. The
   exception is a failure to pause the subscription in the wrapped model that a pause, resume or cancel met before
   `stop()` was done stopping the wrapped model. `stop()` throws that one only when, once `stop()` gets to the
   subscription, the wrapped model runs it or can't say whether it does, and no resume that began after `stop()` has let
   it run. Otherwise it is logged as a warning, and unless such a resume has let the subscription run, `stop()` then
   stops it as it stops any other and throws what fails there. When another call holds the subscription by then, that
   call, or at the latest the thread from item 1 once it gets to the subscription, tries to unregister it, with the same
   exception. When the thread from item 1 fails to unregister it, the thread gives up the lease instead, if the lease
   strategy still reports it held. When the MongoDB lease strategies fail to remove a lease, they report it as not
   held, stop refreshing it and remove it again on their next refresh round. Another node can take the subscription
   over once that removal succeeds, or at the latest once the lease has expired. The thread from
   item 1 tries the subscription again either way. When `stop()` finds a
   subscription held by a call that has not applied it there, it is applied once that call returns, by a thread of its
   own or by the next call made
   for that subscription, and `stop()` doesn't throw what fails there. A `stop()` that a `start(..)` waiting behind it
   took back doesn't throw what failed for a subscription.
4. To find out whether a subscription runs, call `isRunning(id)`. It asks the wrapped model, which runs a competing
   subscription only on the node that holds its lease. `isPaused(id)` returns `true` both for a subscription that waits
   for you and for one that comes back without a call, and nothing it returns tells the two apart.
   - A subscription that `pauseSubscription(..)` or `stop()` paused waits for `resumeSubscription(..)` or
     `start(true)`. On a model built over a wrapped model that is not running, `start(false)` also brings back one that
     `stop()` paused, until a `start(..)` has returned without throwing, see
     [section 22](#22-a-competing-consumer-over-a-wrapped-model-that-is-not-running-waits-for-its-own-start).
   - The other competing subscriptions come back once this node holds their lease. That is one that lost its lease,
     one whose `resumeSubscription(..)` didn't win the lease, one made while the model was stopped, and one whose lease
     went to another node while the wrapped model made it. It is also one whose `resumeSubscription(..)` failed. The
     model counts that one as paused by itself, also when you had paused it, and the thread from item 1 tries it again.
     While the model is stopped, these also wait for `start(..)`, except one you resumed after `stop()`.
   - A competing subscription you paused directly on the wrapped model, and not through the competing consumer model,
     comes back on the next `start(..)`, with either flag, on a grant of its lease and on `resumeSubscription(..)`, see
     [section 20](#pause-a-competing-subscription-through-the-competing-consumer-model).
   - A subscription that doesn't compete has no lease, so a lease this node wins doesn't bring it back. One made after
     `stop()` while the wrapped model was not running, such as before any resume, waits for `resumeSubscription(..)` or
     `start(true)` as one you paused does, and `start(false)` keeps it paused. One made before the first `start(..)` of
     a model built over a wrapped model that is not running is resumed by that `start(..)`, with either flag, unless you
     paused it and the flag is `false`, see
     [section 22](#22-a-competing-consumer-over-a-wrapped-model-that-is-not-running-waits-for-its-own-start). One made
     while the model is started runs once `subscribe(..)` returns, unless you called `stop()` on the wrapped model
     itself.

   A subscription for which both return `false` is still being tried again, or, when it competes, is waiting for its
   lease.

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
running keeps the save off until it returns, for as long as that takes.

Before the first event after a subscribe, the position is saved whatever the predicate is, but only for a subscription
that starts from a stored position, the one in the checkpoint store or the one recorded for it when it subscribes from
the subscription-model default. A subscription from a `StartAt` of your own gets none saved until the predicate stores
the position of an event, so with a predicate that always returns `false` it gets no position stored for an event or a
quiet read. The position it restarts from when its checkpoint is no longer in the oplog, with
`restartSubscriptionsOnChangeStreamHistoryLost` turned on, is still stored.

So with a predicate that declines some events, such as `EveryN` with `n` above 1, a subscription that goes quiet right
after a declined event gets no position saved until the predicate stores one. If it stays quiet for longer than the
oplog window, a restart still ends in lost history.

If you widen the filter of a durable subscription and keep its id, it now resumes from the last quiet position it
saved, so the events before that position that the old filter didn't match are not delivered. Before, it resumed after
the last event the old filter matched, and received them. When you widen a filter, use a new subscription id, or
subscribe once with a `StartAt` for the position to start from.

Change the interval with `saveQuietPositionEvery(Duration)` on `DurableSubscriptionModelConfig`, and keep it well
below the oplog window. `neverSaveQuietPosition()` turns the save off. A subscription that then matches nothing for
longer than the oplog window ends in lost history when it next starts from its stored checkpoint. With
`restartSubscriptionsOnChangeStreamHistoryLost` turned on the model restarts it, and the position it restarts from is
still stored. The Spring Boot starter has no property for the interval, so define your own `SubscriptionModel` bean to change it there.

### `ReactorMongoSubscriptionModel` reads the driver's change stream cursor

For a subscription with an id, `ReactorMongoSubscriptionModel` opens the change stream from
`ReactiveMongoOperations.getCollection(..)` and reads the driver's change stream cursor one batch at a time, so that it
can move the position of a subscription that matches nothing. The `Flux` that `subscribe(filter, startAt)` returns
still reads through `ReactiveMongoTemplate.changeStream(..)`.

The model asks MongoDB for the next batch only once your action's `Mono` has completed for every event of the batch
before. A slow action therefore holds back the next `getMore`, where in 0.33.0 the driver fetched the next batch while
the action ran.

The resume token MongoDB sends with a batch that has no event isn't in the driver's public API, so the model reads it
through private fields of the driver. When the driver in use lacks one of those fields, declares one as neither final
nor volatile, or fails to hand over the token, the model logs a warning with the reason and reads the change stream as
before, and a quiet subscription keeps the position of its last event.

A test that makes the model fail by stubbing `changeStream(..)` on a mocked `ReactiveMongoOperations` no longer
reaches a subscription with an id. Stub `getCollection(..)` as well.

### A quiet reactor subscription's checkpoint is written once a minute too

A `ReactorDurableSubscriptionModel` that wraps `ReactorMongoSubscriptionModel` now saves the quiet position that model
reports, at most once a minute, with the same `CheckpointStorage` and write condition as for an event. What the
blocking subsection above says about your persist predicate, a subscription from a `StartAt` of your own and widening a
filter applies to it too, except the position it restarts from after the oplog dropped its checkpoint, which the reactor
model doesn't store. When you subscribe a subscription from a `StartAt` of your own while a delete that a cancel of
the same id started is still running, the model writes back any checkpoint that delete read, whatever your persist
predicate is. Where `ReactorDurableSubscriptionModel` drives the subscription itself and is stopped at the subscribe,
or a pause of the subscription or a `stop()` comes before the subscribe has taken that delete over, the `start(..)` or
`resumeSubscription(..)` that runs the subscription writes it back instead, if that delete is still running then.

Nothing is saved while an event is being delivered either, but a pause cancels the delivery that is under way, so the
save doesn't wait for it after a resume. The save then stays off until an event is stored again, which is normally the
event delivered again after the resume. A cancel waits for a quiet save that is under way before it deletes the
checkpoint, so a quiet save doesn't write the checkpoint back after the cancel.

When a quiet save fails, the `Mono` that `ReactorMongoSubscriptionModel` waits for fails, and the model reads again
from the subscription's position after its backoff. The save is tried again at the next quiet position without waiting
for the interval.

Change the interval with `saveQuietPositionEvery(Duration)` on `ReactorDurableSubscriptionModelConfig`, and turn the
save off with `neverSaveQuietPosition()`.

The save works when `ReactorDurableSubscriptionModel` wraps `ReactorMongoSubscriptionModel` directly, and also with a
`ReactorCatchupSubscriptionModel` or `ReactorStreamCatchupSubscriptionModel` between the two, as the reactive Spring Boot
starter sets it up when the event store supports catch-up. Both answer `capability(QuietPositionReportingSubscriptions.class)`
with the capability of the model they wrap. `ReactorMongoSubscriptionModel` learns about a subscription only when the
catch-up model has replayed its history, so it reports no quiet position during the replay.

If you put a subscription model of your own between the two, nothing saves the quiet position unless your model answers
the capability the same way, and a quiet subscription keeps the checkpoint of its last event, as in 0.33.0. Only answer it
that way if every event your model delivers by itself, such as a replay, reaches your action before the wrapped model
reads anything for the subscription. The saved quiet position is a position in what the wrapped model reads, so a
restart from it skips whatever your model hadn't delivered yet.

```java
@Override
public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
    if (type == QuietPositionReportingSubscriptions.class) {
        return wrappedSubscriptionModel.capability(type);
    }
    return SubscriptionModel.super.capability(type);
}
```

There is no recipe for these changes. The removed constructor has no replacement to rewrite to, and the rest is runtime
behavior that a rewrite of the source cannot see.

## 20. A competing consumer's `isRunning()` says whether the model is started

This covers `isRunning()` on `CompetingConsumerSubscriptionModel`, the one that takes no subscription id. In 0.33.0 it
returned what the wrapped model returned, so the answer depended on what had started the wrapped model.

- A new model returned `true` over a wrapped model that runs, as a `SpringMongoSubscriptionModel` does by default, and
  `false` over one built with `autoStartup(false)`, which it still does.
- After `stop()` and `start(..)`, it returned `false` until this node won a lease, unless the model had a subscription
  that doesn't compete. `start(..)` started the wrapped model again only for such a subscription, and a lease this node
  won started it for the subscription the lease belongs to.
- After `stop()`, a `resumeSubscription(..)` that won its lease made it return `true`.
- After `shutdown()`, it returned `true` for as long as the wrapped model did.

Now it returns `true` for a new model over a wrapped model that runs, and after `start(..)`, also while this node holds
no lease. A new model over a wrapped model that is not running is stopped until its first `start(..)`, see [section
22](#22-a-competing-consumer-over-a-wrapped-model-that-is-not-running-waits-for-its-own-start). It returns `false` after
`stop()` until the next `start(..)`, also while a subscription you resumed after `stop()` runs on this node. After a
`start(..)` that throws, it returns `false` until a later `start(..)` returns without throwing. Once `shutdown()` has
begun, it returns `false` for good.

A `ManualStartSubscriptionModel` that wraps it, which the Spring Boot starter makes when the subscription mode is
`MANUAL`, returns `true` when it is started itself and the competing consumer model returns `true`. Over the starter's
`SpringMongoSubscriptionModel`, which runs from the start, it answers as in 0.33.0 until its `stop()`. After that, it
returns `true` after a `start()` that wins no lease, where 0.33.0 returned `false` unless the model had a subscription
that doesn't compete. It returns `false` after a resume, until the next `start()`.

Code that calls `start()` only when `isRunning()` returns `false` keeps working. Over a wrapped model that is not
running, `isRunning()` returns `false` until the first `start(..)`, so that code calls it. When a `start(..)` throws,
`isRunning()` returns `false` again, so that code tries again. Every `start(..)` starts the
wrapped model when it is not running, without resuming what the wrapped model holds paused, so a subscription that
doesn't compete, made on a started model, runs once `subscribe(..)` returns. That holds unless you called `stop()` on
the wrapped model itself. The subscription is then made the way that model makes one while it is stopped. Item 4 of
[section 18](#18-a-competing-consumers-start-and-resumesubscription-log-a-failure-and-return) lists what brings each
kind of paused subscription back.

When the wrapped model was stopped, such as by calling `stop()` on it yourself, each competing subscription this node
holds the lease for runs there again once a call through the competing consumer model starts it, also when that start
throws after it took effect. Among the calls that do this are `start(..)`, a lease granted for another subscription, a
`subscribe(..)` that wins its lease and any `resumeSubscription(..)`.

`start(..)` returns without waiting for such a subscription to run, also for one you paused directly on the wrapped
model. Each one is resumed on another thread, and tried again until the subscription runs, loses its lease, or is
paused or cancelled through the competing consumer model. A resume that fails logs a warning. So a resume of such a
subscription that blocks in the wrapped model holds up neither `start(..)`, `stop()` nor any other subscription. If
your code expects a subscription to run the moment `start(..)` returns, wait for `isRunning(id)` to return `true`
instead.

`start(..)` still waits while it makes a competing subscription that waits for its lease compete, and while it resumes
one that lost its lease or, as `start(true)`, one paused by the competing consumer model's `pauseSubscription(..)` or
`stop()`. A `start(true)` in 0.33.0 waited for both too. A start or resume that blocks in the wrapped model there holds
up that `start(..)`, and a `stop()` that waits for it.

A competing subscription you paused directly on the wrapped model runs again too, see [Pause a competing subscription
through the competing consumer model](#pause-a-competing-subscription-through-the-competing-consumer-model) below.
Calling `stop()` on the wrapped model while a call through the competing consumer model is under way isn't supported,
and can leave a subscription whose lease this node holds paused.

When the wrapped model fails to start, `start(..)` throws what failed once every subscription has had its turn, also
when the model has only competing subscriptions.

`stop()` waits for a `start(..)` under way to return, and `shutdown()` waits while that `start(..)` starts the wrapped
model. So when the wrapped model's own `start` doesn't return, neither do `stop()` and `shutdown()`. 0.33.0 did the same
for a model with a subscription that doesn't compete, since only then did its `start(..)` start the wrapped model. Now
it happens for any model whose wrapped model is not running when `start(..)` is called.

If you called `isRunning()` to find out whether this node delivers events, call `isRunning(id)` for each subscription
instead. It asks the wrapped model, which runs a competing subscription only on the node that holds its lease.

There is no recipe for this change. The call compiles as before, and what it returns is runtime behavior that a rewrite
of the source cannot see.

### Pause a competing subscription through the competing consumer model

The competing consumer model now decides whether a competing subscription runs. Its next `start(..)`, with either flag,
a grant of the subscription's lease and a `resumeSubscription(..)` of it run the subscription again when this node holds
its lease and you didn't pause it through the competing consumer model's own `pauseSubscription(..)`. That undoes a
pause you called directly on the wrapped model, and a `stop()` and `start(..)` you called on the wrapped model itself.

In 0.33.0 a pause called directly on the wrapped model survived `start(..)`, with either flag, and a grant of the lease,
and only `resumeSubscription(..)` undid it. The pause that a `stop()` and `start(..)` called on the wrapped model itself
left survived the same way.

Pause a competing subscription with `pauseSubscription(..)` on the `CompetingConsumerSubscriptionModel`, not on the
wrapped model, and resume it with `resumeSubscription(..)` on the competing consumer model. A pause through the
competing consumer model also gives up this node's lease, so another node can take the subscription over.

There is no recipe for this part either. Whether the model you pause a subscription on is the wrapped model of a
competing consumer model is decided at runtime, which a rewrite of the source cannot see.

## 21. A reactive MongoDB subscription started at the present starts from the `subscribe(..)` call

This covers `ReactorMongoSubscriptionModel`. In 0.33.0 a subscription started with `StartAt.now()` or the model default
started at the present of the moment its change stream opened, after `subscribe(..)` had returned, so an event written
in between was never delivered.

Now the model notes the moment `subscribe(..)` is called, or the moment the `Flux` from `subscribe(filter, startAt)` is
subscribed to, and the newest cluster time the MongoDB driver has seen on that client. It opens the change stream just
after that cluster time, or at the start of the second the server's clock showed at the moment if that is earlier.
When that cluster time is at most 15 seconds older than the server's clock, every event written through the same
`MongoClient` after the call is delivered. On a replica set so is every event another client writes, unless another
member becomes primary in between. The `ReactorMongoSubscriptionModel` javadoc says what happens otherwise. Three
things change with it.

- A new subscription can receive events written up to 16 seconds before `subscribe(..)` was called, plus the time the
  reply to the model's `hello` took to reach the client. Events written through the same `MongoClient` reach back at
  most a second, plus that time.
- A subscription made while the model is stopped receives the events written between `subscribe(..)` and `start()`. In
  0.33.0 it started at the present of `start()`.
- A subscription whose change stream first opens longer after `subscribe(..)` than the oplog keeps history, because the
  model was stopped or the subscription paused until then, gets the handling `restartSubscriptionsOnChangeStreamHistoryLost`
  configures. With the model's default, `false`, the subscription stops, the model logs an error, and `isRunning(id)`
  returns `false`. `waitUntilStarted()` has already reported it started by then. In 0.33.0 it opened at the present and
  skipped the events written in between.

What to do:

- Make sure your handlers can receive an event they have already handled. Delivery is at least once, which already
  allowed repeats, so a handler written for that needs no change.
- To have a subscription whose history is gone restart at the present, skipping the events in between as in 0.33.0,
  create the model with `ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true)`.
  The reactive Spring Boot starter turns it on unless you set `occurrent.subscription.mongodb.restart-on-change-stream-history-lost`
  to `false`.

`ReactorDurableSubscriptionModel` over a model of your own that is not a `SubscriptionModel` drives the feed itself, and
a subscription there from `StartAt.now()` now starts from the moment `subscribe(..)` is called too. In 0.33.0 it opened
its feed at the present each time it started. Now the durable model asks your model's `globalCheckpointAsOfNow()` at the
call. So a subscription registered while the durable model is stopped, or paused before it started, receives the events
written between `subscribe(..)` and the start. A subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()`
starts from such a read too, of the call that first starts it, where in 0.33.0 it opened its feed at the present.

When `globalCheckpointAsOfNow()` answers nothing, the subscription opens its feed at the present each time it starts, as
in 0.33.0. When it fails, it is asked again after a delay that about doubles, with a warning for each attempt, until it
answers or the subscription is cancelled, paused, stopped or shut down, and the subscription does not start before it
answers. When it is slow to answer, the subscription waits for it however long it takes, with a warning every 10 seconds
that it still waits. For a subscription from `StartAt.now()`, the durable model has at most one such read of your model
running, and a resume after a pause or a stop waits for that read instead of asking again. One from
`StartAt.dynamic(..)` can read your model twice, once at `subscribe(..)` for the subscription-model default in case the
function answers it, and once at its first start when the function answers `StartAt.now()`. A cancel of the subscription
or a shutdown cancels the read. Your model answers what its `globalCheckpoint()` answers unless it overrides
`globalCheckpointAsOfNow()`, so a model whose `globalCheckpoint()` always fails or never answers keeps such a
subscription from starting, where 0.33.0 started it. Override `globalCheckpointAsOfNow()` to answer `Mono.empty()` for
the 0.33.0 start. A subscription from the subscription-model default refuses that answer, as it refuses the failure. To
also deliver what is written between the call and the read, override `globalCheckpointAsOfNow()` to answer with where
your feed was when it was called.

Where `ReactorDurableSubscriptionModel` wraps a model that manages named subscriptions, such as
`ReactorMongoSubscriptionModel`, it hands the subscription to that model. A `subscribe(..)` from `StartAt.dynamic(..)`
that comes after a `cancelSubscription(..)` of the same id found a stored checkpoint to delete, and before the durable
model has written that checkpoint back, asks the function only once it is written back. When the function then answers
`StartAt.now()`, the subscription starts from where the feed was at `subscribe(..)`, where in 0.33.0 the wrapped model
opened its feed at the present. That read is asked again and warned about the same way, and the wrapped model does not
get the subscription before it answers. A `stop()` or a pause does not end its retries, only an answer, a cancel or a
shutdown does.

Until the wrapped model has the subscription, a pause or a resume of it is kept and given to that model once it has it,
so a subscription paused meanwhile delivers nothing until it is resumed. A `stop()` or a `start(..)` reaches the wrapped
model at once, and only its effect on the waiting subscription is kept, a pause for `stop()` and a resume for
`start(true)`. `start(false)` records no resume, so a subscription that waits while the model is stopped stays paused
after it.

Until a call asks for a state, the waiting subscription is paused while the wrapped model is stopped and running
otherwise, as `ReactorMongoSubscriptionModel` registers a subscription made while it is stopped. A pause of a paused
subscription throws `SubscriptionNotRunningException`, and a resume of one that isn't paused throws
`SubscriptionAlreadyRunningException`. The durable model cannot ask a model that doesn't know the id how it will
register it.

A resume of the waiting subscription while `ReactorMongoSubscriptionModel` is stopped is kept, and doesn't start that
model, where in 0.33.0 its `resumeSubscription(..)` started it. So a subscription of another id that you make after the
resume, and before the waiting subscription is handed over, is registered paused and stays paused. Call `start(..)` on
the model before that subscribe. A subscription of another id that is already paused this way starts with `start()` or
a resume of it.

A wrapped model of your own that registers a subscription made while it is stopped as running differs from 0.33.0 while
the subscription waits. `isPaused(..)` answers `true` for it and a pause of it throws `SubscriptionNotRunningException`,
where that model would answer `false` and pause it. A `start(false)` while that model is stopped keeps the subscription
paused once that model has it, where in 0.33.0 it ran. Resume it to start it. Without a pause, a `stop()` or such a
`start(false)` meanwhile, it runs once that model has it.

A `subscribe(..)` of the id throws `DuplicateSubscriptionIdException` at the call from the moment the subscription
starts to wait until the wrapped model has it and the state kept for it, or until the subscription ends before then.
That includes the time your function runs, and when it answers `StartAt.now()`, the read of where the feed was, however
long that read takes. The durable model keeps a pause, a resume, a `stop()` or a `start(..)` for the waiting
subscription, and such a call would not reach a second subscription of the id.

A `subscribe(..)` of the id that doesn't wait also throws at the call while the wrapped model has a subscription of the
id or is taking one, as in 0.33.0. One that waits throws at the call while the durable model still records a
subscription of the id that it handed over or is handing over. The record ends when the durable model cancels that
subscription, when it can't record where that subscription starts, when its hand-over or a start again of it fails,
when a later subscription of the id replaces it, and at `shutdown()`. It stays when the wrapped model fails or drops the
subscription by itself, as it does after an error there, also when that error fails `waitUntilStarted()`. A
`subscribe(..)` of the id throws while the durable model starts a subscription of the id there again, and while a
cancel, a pause or a resume that the durable model sent the wrapped model for the id is still under way.
[Section 23](#23-a-reactor-cancelsubscription-returns-a-mono-that-completes-once-the-stored-state-is-deleted) describes
the last two.

A `subscribe(..)` that waits is checked again before the wrapped model gets it, and the wrapped model checks it then too.
A refusal there fails its `waitUntilStarted()` with `DuplicateSubscriptionIdException` and is logged as an error, since
the call has returned. That can happen when the wrapped model has a subscription of the id that the durable model
doesn't record, as after a cancel that failed there.

In every other case the durable model accepts the `subscribe(..)`, also while another `subscribe(..)` of the id still
reads where to start, and hands both to the wrapped model. `ReactorMongoSubscriptionModel` refuses a `subscribe(..)` of
an id it has, so it throws `DuplicateSubscriptionIdException` for the one of the two that reaches it second. One that
fails before then, in its own start position for instance, never reaches it.

There is no recipe for this change. Where a subscription starts is runtime behavior that a rewrite of the source cannot
see.

## 22. A competing consumer over a wrapped model that is not running waits for its own `start()`

A new `CompetingConsumerSubscriptionModel` over a wrapped model that is not running, such as a
`SpringMongoSubscriptionModel` built with `autoStartup(false)`, is stopped until you call its own `start(..)`, as if
`stop()` had been called on it. A subscription made before then is held paused in the wrapped model, and a competing one
doesn't compete for its lease. That `start(..)` starts the wrapped model, resumes every subscription that doesn't
compete, also as `start(false)`, and makes every competing subscription compete for its lease. With `start(false)`, a
subscription you paused before then stays paused. A `start(..)` that throws doesn't count as the first. Until one has
returned without throwing, each `start(..)`, also one after `stop()`, resumes every subscription that doesn't compete and
makes every competing one compete for its lease, also one that `stop()` paused. The exception is one you paused and
haven't resumed since, which `start(false)` keeps paused. Once a `start(true)` has resumed one you paused, a later
`start(false)` resumes it too, also when that `start(true)` threw for another subscription. A `start(false)` returns
without waiting for a competing subscription that `stop()` paused to run again, as in [section
20](#20-a-competing-consumers-isrunning-says-whether-the-model-is-started).

A wrapped model of your own that refuses `subscribePaused(..)` with `UnsupportedOperationException` is the exception.
A competing subscription made before the first `start(..)` registers for its lease straight away. When this node wins
it, the subscription is subscribed in the wrapped model, which holds it paused since it is not running, so this node
gives the lease back, and the subscription competes for its lease from that `start(..)` on.

In 0.33.0 nothing stopped such a model, although `isRunning()` returned `false` for it. A competing subscription
competed for its lease as soon as it was made. A `start()` on the wrapped model got it running once this node held the
lease, and one that won the lease after waiting for it ran with no `start()` at all. Now neither runs before `start(..)`
on the competing consumer model.

To tell whether the wrapped model runs, the constructor of `CompetingConsumerSubscriptionModel` now calls `isRunning()`
on the wrapped model, which 0.33.0 never did. When that call throws, the constructor throws the same exception.

Call `start()` on the `CompetingConsumerSubscriptionModel`. Code that also calls `start()` on the wrapped model, before
or after, keeps working, and a competing subscription runs once this node holds its lease. Calling `start()` on the
wrapped model instead runs the subscriptions that don't compete, but no competing subscription competes for its lease, so every
event the wrapped model hands one waits. A warning that names the subscription and this step is logged the first time
that happens for each such subscription. A `stop()` or `shutdown()` on the competing consumer model hands the events
that wait to the handler. Code that started neither model now delivers nothing and logs nothing, since the wrapped model
hands no event over.

Over a wrapped model that runs as the competing consumer model is built, such as a `SpringMongoSubscriptionModel` with
the default configuration, nothing changes.

There is no recipe for this change. Which model your code starts, and whether it was running as the competing consumer
model was built, is runtime behavior that a rewrite of the source cannot see.

## 23. A reactor `cancelSubscription(..)` returns a `Mono` that completes once the stored state is deleted

This covers the reactor `CancellableSubscriptions`, which every reactor `SubscriptionModel` extends, the reactor
`DcbSubscriptionModel`, and the reactor `DcbSubscriptions.cancel(..)`. In 0.33.0 `cancelSubscription(..)` returned
nothing, and a model that stores a checkpoint or a catch-up marker deleted it in the background. A process that ended
before that delete succeeded kept the checkpoint or the marker. After a restart the same id then resumed from the
cancelled subscription's position, or skipped its history.

Now it returns `Mono<Void>`. The cancel still takes effect when you call the method, whether or not anything subscribes
to the `Mono`. The `Mono` completes once the state stored for that id is deleted, in the model you called and in every
model it wraps, and it fails when a delete fails. Neither the method nor the `Mono` has to wait for a call of the
subscription's action that is already running, so that call may still be running after the `Mono` completes. Waiting
for it would let one action that never ends hold up the cancel.

What to do:

1. A call that ignores the result compiles. Recompile code built against 0.33.0, since the return type is part of the
   method's compiled signature. It behaves as before, except on a `ReactorDurableSubscriptionModel` that wraps a model
   that manages named subscriptions, `ReactorMongoSubscriptionModel` for one. That wrapped model cancels, pauses and
   resumes by id. While a cancel, a pause or a resume that `ReactorDurableSubscriptionModel` sent it for the id has not
   ended, your own pause and resume included, a subscribe of the id throws `DuplicateSubscriptionIdException`, also
   after you cancel the id. The cancel's `Mono` completes only once none of them is under way, so wait for it before
   you subscribe the id again, as step 2 describes.
2. When a later subscribe with the same id has to start from its own `StartAt`, also after a restart, wait for the
   `Mono`, for example with `cancelSubscription(id).block()` or by chaining on it. When it fails, or the process ended
   before it completed, call `cancelSubscription(id)` again. That works in a new process that never subscribed the id.
3. A class that implements either interface stops compiling. Return `Mono<Void>`. An implementation that cancels
   synchronously and stores nothing returns `Mono.empty()`.
4. An implementation that deletes stored state asynchronously starts the delete when the method is called, returns a
   `Mono` that completes once the delete has, and caches it, so a second subscriber does not delete again.
5. A model that wraps another returns a `Mono` that also waits for the `Mono` from the wrapped model's
   `cancelSubscription(..)`.
6. A class that implements both the blocking and the reactor `CancellableSubscriptions` with one
   `void cancelSubscription(String)` cannot compile against 0.34.0 in any form, since the two methods now differ only
   by return type. Split it into a blocking adapter and a reactor adapter, each implementing one of the two, and have
   both call the code that cancels.

`UpgradeToOccurrent_0_34` changes a Java implementation of either interface that returns `void` to return `Mono<Void>`.
A declaration with no body, an abstract one or one in an interface that extends either interface, gets the new return
type and nothing else. What the recipe does with a body depends on how the body ends:

| The body | What the recipe does |
|---|---|
| ends by calling `cancelSubscription(..)` on the model it wraps, or on its superclass | returns that call |
| is empty, or ends in a statement such as a method call or an assignment | adds `return Mono.empty()` at the end |
| ends in a `return` or a `throw` | keeps the body in the method |
| ends in anything else, an `if`, a loop, a `try` or a `switch` for example | moves the body unchanged into a new private `void` method named `doCancelSubscription`, then calls it and returns `Mono.empty()`. The name becomes `doCancelSubscription2`, and so on, when the class can already call a method with that name without a qualifier, one it has or inherits, one of an enclosing class, or a statically imported one. A class with a supertype the recipe cannot see gets `cancelSubscriptionBodyBeforeOccurrent0340` instead, numbered the same way, since that supertype can have a public `doCancelSubscription(String)` the class never calls, and a private method with the same name and parameters does not compile |

Where the body stays in the method, each `return` without a value becomes `return Mono.empty()`. That is the whole
change for step 3, and for step 5 when the wrapped model's cancel is the last statement.

A body that calls a wrapped model's `cancelSubscription(..)` anywhere else, inside an `if` or before other statements,
gets a `TODO` comment, since the `Mono` it returns does not wait for that call. Return that call's `Mono`, as step 5
describes. Step 4 stays by hand, since the recipe cannot tell a delete that runs in the background from code that
finishes before the method returns. The recipe does not change a Kotlin implementation, or a lambda or method reference
that implements `CancellableSubscriptions`. It does not change the method of a class that also implements the blocking
`CancellableSubscriptions`, apart from a `TODO` comment that points here, since changing the return type would only swap
which of the two the class fails to implement. Step 6 stays by hand. The recipe finds the blocking interface only when
it can see it among the class's supertypes, so check by hand a class that reaches it through a type the recipe cannot
see.

`ReactorDurableSubscriptionModel` now deletes the checkpoint only after every checkpoint write the cancelled
subscription had already started has ended, and a write it had not started by then never runs.

A subscribe of the id in the same process that comes before the delete is taken out, which happens before the cancel's
`Mono` completes, takes the delete over. Where `ReactorDurableSubscriptionModel` drives the subscription itself and is
stopped at the subscribe, or a pause of the subscription or a `stop()` comes before the subscribe has taken the delete
over, the `start(..)` or `resumeSubscription(..)` that runs the subscription takes it over instead, if the delete has
not been taken out by then. The delete makes no further try, and the call that took it over writes back the checkpoint
that a try of the delete read. That includes a try that already deleted it, and a try that failed after the store
applied it, so the store holds what it held before the cancel. The cancel's `Mono` then completes. A subscription from
the subscription-model default resumes from the cancelled subscription's checkpoint.

While a checkpoint waits to be written back, for this cancel or for an earlier subscribe of the id, the subscribe
returns without calling your `StartAt.dynamic(..)` function, on any thread. `ReactorDurableSubscriptionModel` calls it
on a thread of its own once the store holds that checkpoint again, so `ResumeStartPositions.replayThenResume(..)` and
the Spring Boot starter's `BEGINNING` start with the default `ResumeBehavior` resume from it too. When
`ReactorDurableSubscriptionModel` drives the subscription itself, a resume or a `start(..)` does the same. With no
checkpoint waiting to be written back, your function is called at the call, as with no delete running.

While your function waits, a cancel of the id or a `shutdown()` ends the wait without calling it, and the subscription
doesn't start. When `ReactorDurableSubscriptionModel` drives the subscription itself, a pause of the id and a `stop()`
end it the same way. A write back that fails is tried again, as described below, and your function is called once one
succeeds. An exception your function throws then is logged as an error. It fails `waitUntilStarted()` of the
subscription the subscribe returned, and keeps a subscription that a resume or a `start(..)` began paused, so you can
resume it. Wait for `waitUntilStarted()` if your code needs to know that such a subscription started.

Your function runs on a thread that may block whenever it waits for a write back, also when you subscribe on a thread
where Reactor refuses to block, a WebFlux request thread for example. With nothing to wait for it runs on your thread,
as with no delete running.

A first checkpoint that fails or is refused, for instance because another node stored one for the id meanwhile, ends the
subscription as it does with no delete running. On a model that wraps one that manages named subscriptions, that failure
can reach `waitUntilStarted()` instead of the subscribe, as described below.

When a subscribe fails once it has taken the delete over, as one that Reactor refuses on a thread that may not block
does, the delete still removes the checkpoint, as in 0.33.0, unless another subscription of the id is starting or
registered by then. The same goes for a subscription that took the delete over and ends before it started without
writing a checkpoint, for instance because its start position cannot be read. A subscription that a pause ended before
it started keeps the delete taken over until you resume it. When that subscribe fails, or that subscription ends,
before the delete has ended, the cancel's `Mono` ends only once the delete that goes ahead has ended. To start the id
clean, wait for the `Mono` before you subscribe it again, as step 2 describes.

Apart from a subscribe whose `StartAt.dynamic(..)` function waits for a write back, the subscribe reads the store at the
call and waits neither for the delete nor for the checkpoint writes the delete runs after. When the store holds nothing,
the subscription starts from the checkpoint that the newest of those writes is writing, and with none of them from where
the feed is at the call, as with no delete running. A write that reaches the store after that read makes the
subscription start from an earlier checkpoint than the last one the cancelled subscription wrote, so your action can see
those events again. The subscription writes a checkpoint only once those writes have ended.

When they have not ended at the call, or on a storage of your own the write back described below has not, a subscription
handed to a wrapped model that manages named subscriptions is handed its checkpoint at the call, and an event that model
delivers before the checkpoint is recorded waits for it. When the store records an earlier checkpoint for the id than
the one read, as `resolveFirstCheckpointRace(..)` can answer, `ReactorDurableSubscriptionModel` cancels the
subscription in the wrapped model and subscribes it there again from that earlier checkpoint, so your action sees the
events between the two.

A pause, a resume, a `stop()` or a `start(..)` made while `ReactorDurableSubscriptionModel` starts the subscription
there again is kept and put in place once the subscription is there again. A pause of a subscription that is already
paused throws `SubscriptionNotRunningException`, and a resume of one that isn't paused throws
`SubscriptionAlreadyRunningException`. The subscription has the state you last asked for before your action sees an
event from the earlier checkpoint, and `isPaused(id)` and `isRunning(id)` answer that state meanwhile. A subscribe of
the same id meanwhile is refused with `DuplicateSubscriptionIdException`, as the wrapped model refuses it while it has
the subscription. `ReactorDurableSubscriptionModel` cancels the subscription in the wrapped model before its second
subscribe, and pauses or resumes it there afterwards to give it the state you asked for. The wrapped model takes each of
these calls by id, so each would reach a later subscription of the id. A subscribe is therefore refused until each has
ended, also after you cancel the id. Your cancel's `Mono` completes only after that, so a subscribe made once it has
completed is not refused for this reason. A wrapped model of your own whose cancel never completes keeps the subscribe
of the id refused and your cancel's `Mono` from completing.

When the checkpoint cannot be recorded, or that second subscribe fails, `ReactorDurableSubscriptionModel` cancels the
subscription in the wrapped model and its `waitUntilStarted()` fails with that error, since the subscribe has returned
by then. With no delete running that failure is thrown from the subscribe, as in 0.33.0. Wait for `waitUntilStarted()`
if your code needs to know that such a subscription started.

That cancel is not made when a later subscribe of the id has put a subscription in the wrapped model by then. A
subscribe of the id that comes once `ReactorDurableSubscriptionModel` has decided to make it is refused with
`DuplicateSubscriptionIdException` until the cancel has ended.

The reactor `CheckpointStorage` gains two methods with defaults, `delete(subscriptionId, condition)` and
`evaluatesDeleteConditions()`. The in-memory and MongoDB reactor storages implement both. On them each try of the delete
is conditional on the version it read just before, and the checkpoint goes back at once at a higher version. The
subscription writes its own checkpoints at once too, at a version above that, so neither the try nor the write back
removes or replaces them, and neither the delivery of an event, a read of the store nor a checkpoint write waits for the
try or the write back. Neither does the subscribe.

On those storages a subscription from the subscription-model default writes the checkpoint it read, or the one written
back when it read nothing, at that version before it starts. One registered while the model was stopped writes it before
`resolveFirstCheckpointRace(..)` compares it with where the feed was at registration. A refused write makes the
subscription read the store again, and a write that fails fails the subscription to start. A write back that fails is
logged as a warning and tried again after a delay that about doubles, as a try of the delete is. The tries end once one
succeeds, once every subscribe that took the delete over has given it back, which a cancel of the id does, or once the
model is shut down. A pause or a `stop()` does not end them, since a resume or a start begins from what they write. A
subscription handed to a wrapped model that manages named subscriptions writes that checkpoint after the subscribe has
returned, and an event the wrapped model delivers before then waits for it. When that write is refused, the subscription
keeps the checkpoint it was handed, so your action can see events again that the stored checkpoint already covers.

A `CheckpointStorage` of your own keeps compiling and keeps working, and on it the write back of a subscribe that takes
the delete over waits for the try under way to end. The subscribe doesn't wait for it, and when a try has already
deleted the checkpoint, the subscription starts from the checkpoint that try read. Its checkpoint writes wait for the
write back, and a resume or a start of the id that comes before then takes the delete over too. When
`ReactorDurableSubscriptionModel` drives the feed itself, a subscription that starts from a checkpoint opens the feed
once the write back has ended, and one that starts from where the feed is opens it at the call. A write back that fails
is tried again as above, so a subscription that starts from a checkpoint, and the first checkpoint write of any other,
wait until one succeeds. On any storage, the delete of a later cancel of the id runs only once the write back has ended.
A process that ends after a try deleted the checkpoint and before the write back reached such a store keeps no
checkpoint, also for a subscription with a `StartAt` of its own that handled events by then, since its checkpoint writes
wait for the write back. The next subscribe of the id then starts as a new subscription, as described below. To remove
that case and the wait for the try, implement both:

1. `delete(subscriptionId, condition)` evaluates the condition against the stored version exactly as
   `save(subscriptionId, checkpoint, condition)` does, in the same atomic step as the delete. `notOlderThan(v)` deletes
   unless the stored version is above `v`, and deletes a checkpoint stored without a version. `ifAbsent()` deletes
   nothing, and is refused when a checkpoint is stored. Nothing stored completes the `Mono` without deleting. A refusal
   signals `CheckpointWriteConditionNotFulfilledException`.
2. `evaluatesDeleteConditions()` answers true.

The reactor `CheckpointStorageConformance` in `occurrent-tck-subscription-reactor` covers both methods. For a storage
that answers false it checks that a conditional delete fails with `UnsupportedOperationException` and deletes nothing.

Unless you configure `startWhenNoStartPositionCanBeRecorded(true)`, a subscription from the subscription-model default
stores its start position before it handles its first event, on any storage, and it is durable from then on. A process
that ends before then ends as if the subscribe never ran, so the next subscribe of the id starts as a new subscription
would, from where the feed is then. A subscribe of the id that comes once the cancel's `Mono` has completed starts as a
new subscription too.

The cancel also ends a subscribe of the id that has not started yet. The subscribe writes no checkpoint, and its
`waitUntilStarted()` fails with `CancellationException`. A `StartAt.dynamic(..)` function that was about to run as the
cancel came can still run after the cancel returned, since the cancel does not wait for your function, and the model
discards what it answers. When the durable model drives a subscription itself, a pause or a `stop()` before it has
started ends it the same way, except that its checkpoint stays stored and the subscription stays paused. Its
`waitUntilStarted()` fails with `CancellationException` too. In 0.33.0 that wait never ended. If you wait for
`waitUntilStarted()` on a subscription that another part of your application may cancel, pause or stop, handle that
error. A registration made while the model was stopped is the one exception. The handle it returned keeps waiting once a
resume or `start(true)` has taken the registration over and the subscription has started, or a pause or a `stop()` has
ended it, as in 0.33.0. A cancel or a refusal that comes before the subscription has started now ends that wait as well.

On a `ReactorDurableSubscriptionModel` that wraps a model that manages named subscriptions, the cancel also ends a
subscribe that is still reading its start position or that the wrapped model is still taking when the cancel comes. The
cancel does not wait for that wrapped model. The durable model cancels the subscription in the wrapped model once the
wrapped model has taken it, and the cancel's `Mono` completes only after that. When that cancel fails, the subscription
can still be in the wrapped model, so the cancel's `Mono` fails with that error. A `shutdown()` does not wait for the
wrapped model either, and logs such a failure as an error. After a `shutdown()` no subscription starts a checkpoint
write, including one the wrapped model is still running an event through. Once a cancel or a `shutdown()` has returned,
the durable model doesn't run the action of a subscription it handed to the wrapped model and ended, even when the
wrapped model still delivers an event to it.

Neither a cancel nor a `shutdown()` of `ReactorDurableSubscriptionModel` waits for a call of the action that is already
running, so that call can still be running after the cancel's `Mono` completes or `shutdown()` returns. The durable
model writes no checkpoint for the event that call handles. After a `shutdown()` the checkpoint stored before that
event stays, so the next subscribe of the id that resumes from it delivers the event again, in a new model or after a
restart. After a cancel the checkpoint is deleted, so a later subscribe of the id delivers the event again only when
its own `StartAt` begins before it. If that call must have ended before you go on, have the action signal it.

None of these waits has a time limit, because a store can still apply a write after the model stopped waiting for it. A
save could then bring the cancelled checkpoint back after the delete, and a delete could remove the checkpoint the next
subscription wrote. On a `CheckpointStorage` of your own that does not implement `delete(subscriptionId, condition)` and
whose deletes hang, a subscription of the id made right after the cancel therefore writes no checkpoint until the store
answers, and one that starts from a stored checkpoint handles no event before then. No call for another id waits for it.

A delete that fails is tried again until it succeeds, a subscribe of the id takes it over, or the model is shut down,
and each failure is logged as a warning.
The wait before a try starts at 100 milliseconds and about doubles after each failure, never past 5 seconds, with some
randomness so that deletes failing together are not tried again together. Until a try succeeds or a subscribe takes
the delete over, the cancel's `Mono` neither completes nor fails. A `shutdown()` stops the
tries, also one waiting to be tried again. A try already under way runs to its end, and otherwise the `Mono` fails with
the error of the last try. A store can retry within one try, as `ReactorCheckpointStorage` for MongoDB does by default,
so such a try can still reach the store after the `shutdown()`. When the checkpoint stays stored, call
`cancelSubscription(id)` again once a model runs, as step 2 describes.

When `ReactorDurableSubscriptionModel` drives the subscription itself, no call waits for where the feed is, on any
thread. A `subscribe(..)` from the subscription-model default, or from `StartAt.now()`, asks the wrapped model's
`globalCheckpointAsOfNow()` at the call, which answers with where the feed was at that call however late it answers. So
the subscription delivers what you write after the call returned, also when it was registered on a stopped model or
paused before it started, see section 21 for `StartAt.now()`. A `subscribe(..)` on a running model asks storage first
and asks the wrapped model only when no checkpoint is stored. `start(..)` waits for no read of where the feed is, so a
read that never answers holds up neither the start nor the other subscriptions it starts. A subscription from the
subscription-model default, or from `StartAt.now()`, whose read never answers does not start, and logs a warning every
10 seconds that it still waits, see section 21. A `StartAt.dynamic(..)` function on a stopped model runs only once the
model is started. When it answers the subscription-model default, the subscription starts from where the feed was at the
`subscribe(..)`, and when it answers `StartAt.now()`, from where the feed was at the start.

A wrapped model of your own that does not override `globalCheckpointAsOfNow()` answers with where its feed is when the
read runs, so a subscription over it can still skip what you write between the call and the read, as in 0.33.0. Override
it to answer with where the feed was when it was called.

When `ReactorDurableSubscriptionModel` wraps a model that manages named subscriptions, a `subscribe(..)` from the
subscription-model default, or from a `StartAt.dynamic(..)` that answers it, reads where the feed is whether that model
runs or not, and waits for that read however long it takes. Returning before the read answered could start the
subscription from a position after events written once the call had returned, and skip them. A cancel of that id or a
`shutdown()` ends the wait, and calls for other ids don't wait for it. When the subscribe reads on a thread where
Reactor refuses to block, Reactor refuses that read and `subscribe(..)` throws, as in 0.33.0, so subscribe from a thread
that may block.

A function that runs at the call and throws makes `subscribe(..)` throw, as in 0.33.0. One that runs later, because a
checkpoint waits to be written back, fails `waitUntilStarted()` instead, as described above. A `shutdown()` ends
`waitUntilStarted()` with `SubscriptionModelShutdownException` for a subscription it ends before the subscription
starts, such as one registered while the model was stopped, where in 0.33.0 that wait never ended when
`ReactorDurableSubscriptionModel` drove the subscription itself.

A reactor catch-up model cancelled before its replay handed the subscription over to the wrapped model now passes the
cancel on to the wrapped model too, the way the blocking `StreamCatchupSubscriptionModel` always has. A wrapped model
you wrote yourself can therefore get `cancelSubscription(..)` for an id it was never given in this process. It stops
nothing then, and deletes what it stores for that id, as `CancellableSubscriptions` describes.
