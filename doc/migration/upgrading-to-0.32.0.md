# Upgrading to Occurrent 0.32.0

Five things break, and only if you use the features they belong to.

**At compile time**, if you write reactive subscriptions, one type was renamed. The reactor `SubscriptionModel` is now
`FluxSubscriptionModel`. The recipe below rewrites it for you. Read
[section 5](#5-the-reactor-subscriptionmodel-is-now-fluxsubscriptionmodel).

**Also at compile time**, if you implement `SynchronousEventDispatcher` or `ReactiveSynchronousEventDispatcher`
yourself, one method gained a parameter. Almost nobody does, since the model Occurrent ships implements it for you. Read
[section 4](#4-a-synchronous-subscription-no-longer-stops-at-the-first-failing-handler).

**Also at compile time**, if you use `NativeMongoLeaseCompetingConsumerStrategy`, it moved packages. The same recipe
rewrites it. Read
[section 9](#9-nativemongoleasecompetingconsumerstrategy-moves-to-the-native-driver-package).

**At startup**, a push sink now feeds exactly one projection or saga, so an application that shared one between several
refuses to start. If you use a push source, read
[section 3](#3-a-push-sink-feeds-exactly-one-projection-or-saga) first.

**At runtime**, if you feed a `DomainEventFeed` from your own listener, `accept(..)` now throws when no projection is
registered instead of returning normally, so the message goes unacknowledged rather than being discarded. Read
[section 13](#13-domaineventfeed-refuses-an-event-when-no-projection-is-registered).

**Also at runtime**, if a `CatchupThenPushSubscriptionModel` or a `@Projection(source = PUSH)`/`@Saga(source = PUSH)`
(blocking stack) is fed from more than one thread once it is live, your handler now sees genuinely concurrent calls
instead of calls serialised behind a lock. Read
[section 14](#14-a-live-push-handler-can-now-be-called-concurrently).

Fourteen things are worth reading. One configuration property is deprecated and has a recipe that rewrites it for you, the
MongoDB event stores changed how they persist the CloudEvent `time` attribute under
`TimeRepresentation.RFC_3339_STRING`, a push sink feeds one consumer, a synchronous subscription no longer stops at the
first failing handler, the reactor subscription primitive was renamed, a durable reactor model refuses a composition it
used to accept, a paused MongoDB subscription now delivers what was written while it was paused, a push catch-up
replays on its own thread and no longer discards events after a failed replay,
`NativeMongoLeaseCompetingConsumerStrategy` moved to the package every other native-driver subscription type uses,
`CompetingConsumerSubscriptionModel` refuses two calls it used to accept, a second `start()` is allowed while
`waitUntilStarted` stops saying yes when it means no, a domain-event feed refuses an event when no projection is
registered, every way a subscription model can refuse a call now has its own exception type, and a live push handler
can be called concurrently instead of queueing behind one lock.

## 1. `occurrent.subscription.enabled` becomes `occurrent.subscription.mode`

`occurrent.subscription.enabled` was a boolean. Its replacement is an enum with three values, because there is now a
third thing you can ask for:

| Old | New | What it means |
|---|---|---|
| `occurrent.subscription.enabled=false` | `occurrent.subscription.mode=disabled` | No subscription beans at all |
| `occurrent.subscription.enabled=true` | `occurrent.subscription.mode=auto` | Subscriptions are created and started, the default |
| no equivalent | `occurrent.subscription.mode=manual` | Every subscription is registered, none of them runs until you start it |

The old property still works and is deprecated, so nothing breaks if you upgrade without touching your configuration.
It is removed in the release after next. Setting both is allowed while they agree, which is deliberate. A rewritten
configuration file plus a leftover environment variable should not fail your application. Setting both so they
contradict each other fails at startup, naming both values.

### Run the recipe

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_32</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.32.0</version>
        </dependency>
    </dependencies>
</plugin>
```

```bash
mvn rewrite:run
```

It rewrites `.properties` and `.yaml` alike, and it is deliberately not restricted to `application.properties` or
`application.yml`, so it also reaches a profile file, a `config/` directory, and anything you pull in with
`spring.config.import`. Expect the diff to cover every configuration file that sets the key, wherever it lives.

### What the recipe leaves for you

Three cases, all of which it steps around on purpose rather than guessing:

- **A value it cannot read as a boolean**, `occurrent.subscription.enabled=${SUBSCRIPTIONS_ON}` for example. It leaves
  the whole entry alone, deprecated key included. Renaming the key there would leave
  `occurrent.subscription.mode=${SUBSCRIPTIONS_ON}` resolving to `true`, which is not a mode and fails to bind. Change
  the property and whatever supplies the value together.
- **An environment variable or anything outside your configuration files.** `OCCURRENT_SUBSCRIPTION_ENABLED` is
  invisible to a source rewrite. Search your deployment configuration for it by hand. This is exactly why setting both
  properties is tolerated while they agree.
- **A file that already sets both keys.** The recipe drops the deprecated one and keeps `occurrent.subscription.mode`,
  on the assumption that the key you migrated to is the one you meant.

## 2. The CloudEvent `time` attribute

The rest of this page is the `time` change. Nothing has to be migrated for the fix to work, and there is one optional
cleanup if you want the fix to cover events you have already stored. If you use `TimeRepresentation.DATE`, or you never
filter on `time`, you can stop reading here.

### What changed

The `time` attribute used to be written with `OffsetDateTime.toString()`, which omits parts of the value when they are
zero. The same instant could therefore be stored in several shapes:

```
2026-07-28T12:00Z                    the seconds and the fraction are both zero
2026-07-28T12:00:30Z                 the fraction is zero
2026-07-28T12:00:30.123456789Z       nothing is omitted
```

It is now always written in one shape, with seconds and with nine fractional digits:

```
2026-07-28T12:00:00.000000000Z
2026-07-28T12:00:30.000000000Z
2026-07-28T12:00:30.123456789Z
```

Nanosecond precision is unaffected. The only difference is that parts which used to be omitted are now written out.

That representation stores `time` as a string, so MongoDB compares it character by character. A single shape is what
makes those comparisons behave, which fixes two things. An exact filter such as `Filter.time(instant)` now matches an
event written at that instant even when its timestamp falls on a whole minute, and range filters now order correctly.
Both applied to MongoDB subscriptions as well, since they convert their filters through the same code.

### The one thing to know

Events already in your database keep the shape they were written with. Queries over events written by 0.32.0 and later
are correct, and a query whose boundary lands exactly on an older event can still miss it, because the filter is
rendered in the new shape and the stored value is in the old one.

If that matters to you, rewrite the field once. This is optional.

### Optional: rewrite the stored values

Run this against your event collection, with the server on MongoDB 4.2 or later so `$dateToString` is available in an
aggregation pipeline update. Replace `events` with your collection name, and take a backup first.

```js
db.events.updateMany(
  { time: { $type: "string" } },
  [
    {
      $set: {
        time: {
          $dateToString: {
            date: { $dateFromString: { dateString: "$time" } },
            format: "%Y-%m-%dT%H:%M:%S.%LZ"
          }
        }
      }
    }
  ]
)
```

Two caveats worth reading before you run it.

`$dateFromString` parses into a BSON date, which holds milliseconds, so this loses any precision finer than a
millisecond that was in the stored string. If your events carry microseconds or nanoseconds and you need them, do the
rewrite in application code instead, reading each `time` with `OffsetDateTime.parse` and writing it back formatted with
nine fractional digits.

`%L` emits three fractional digits, not nine, so the rewritten values are canonical in shape but not identical to what
0.32.0 writes for new events. Comparisons still behave, because all rewritten values share one shape and sort against
each other correctly, but an exact filter built from a nanosecond instant will not match a rewritten value. Use the
application-code route if you need exact matching against these older events.

### Time range queries and UTC offsets

One limit did not change. Chronological ordering of a string comparison also depends on every value carrying the same
UTC offset. These are the same instant and do not sort the same way:

```
2026-07-28T14:00:00.000000000+02:00
2026-07-28T12:00:00.000000000Z
```

Occurrent does not normalise the offset away, because preserving the timezone is the reason to choose
`RFC_3339_STRING` in the first place. So range queries are sound for a collection whose events carry a consistent
offset, which is the case if you store UTC. If your events span several offsets and you need range queries over them,
use `TimeRepresentation.DATE`, which compares numerically, or keep a separate attribute holding the instant as a
`Date`.

Rationale in [ADR 79](../architecture/decisions/0079-canonical-fixed-width-time-for-rfc3339-storage.md).

## 3. A push sink feeds exactly one projection or saga

There is no recipe for this one, because it is a change to a bean topology rather than a rename a recipe could apply.

`PushSubscriptionModel` and `DomainEventFeed` used to fan one received message out to every consumer registered on
them. A broker message carries one acknowledgement decision, so those consumers shared it. A consumer that kept failing
held up every consumer behind it on every redelivery, and once the broker gave up and dead-lettered the message they
never saw it at all. That breaks the isolation Occurrent now holds itself to, so the shared configuration is no longer
expressible. [ADR 90](../architecture/decisions/0090-a-push-sink-feeds-one-consumer.md) has the full reasoning.

You are affected only if two or more consumers register on the same sink instance. If each of your push projections
already has its own bean, nothing changes.

The failure is at startup, not at runtime, and it names both consumers:

```
This DomainEventFeed already feeds projection 'orders', so 'shipments' was refused: a push sink feeds exactly one
consumer. Declare one sink per projection or saga, each fed by its own queue. ...
```

### Before

```java
@Bean
PushSubscriptionModel pushModel() {
    return new PushSubscriptionModel();
}

@Projection(id = "orders", source = PUSH)
Projection<OrderView, DomainEvent, String> orders() { ... }

@Projection(id = "shipments", source = PUSH)
Projection<ShipmentView, DomainEvent, String> shipments() { ... }
```

### After

```java
@Bean
PushSubscriptionModel ordersFeed() {
    return new PushSubscriptionModel();
}

@Bean
PushSubscriptionModel shipmentsFeed() {
    return new PushSubscriptionModel();
}

@Projection(id = "orders", source = PUSH, subscriptionModelName = "ordersFeed")
Projection<OrderView, DomainEvent, String> orders() { ... }

@Projection(id = "shipments", source = PUSH, subscriptionModelName = "shipmentsFeed")
Projection<ShipmentView, DomainEvent, String> shipments() { ... }
```

A `DomainEventFeed` migrates the same way, with one bean per projection.

Then give each sink its own queue on the broker, so that a message one projection cannot handle stops only that
projection. A single queue behind two sinks still couples them, just at the transport rather than in Occurrent. Only
one of the two consumers would receive any given message. Whether that means a queue per consumer on a fanout exchange,
a consumer group per projection, or something else is your broker's vocabulary rather than Occurrent's.

If you drive the sinks yourself rather than through `@Projection`, the same applies. Construct one per consumer and
call `accept(...)` on each from your listener.

## 4. A synchronous subscription no longer stops at the first failing handler

Only relevant if you register more than one synchronous subscription and you do **not** run them in a transaction. If
you use the Spring Boot starter without replacing its `TransactionExecutor` bean, nothing here changes for you.

Handlers used to run in registration order until one threw, and that exception ended the dispatch. With a transaction
that is harmless, because the write rolls back, so no handler is left having acted on an event that no longer exists.
Without one the write has already committed by the time handlers run, so the handlers behind the failure never received
that event, and a synchronous subscription has no replay to catch them up. They never would.
[ADR 57](../architecture/decisions/0057-synchronous-subscriptions.md) has the reasoning, in the amendment at the end.

There is no recipe, because nothing here is a rename a recipe could apply.

### What changes

Every handler is now offered every event, and the failures are reported once the batch has been dispatched:

- **One handler failed.** Identical to before. That exception reaches you exactly as it was.
- **Several handlers failed.** The first one reaches you, and the rest arrive in its `getSuppressed()`. If you log or
  match on the exception from `execute`, read `getSuppressed()` too, or you will see only the first failure.
- **A handler that failed is skipped for the rest of that write's events.** Handing it the later events would update its
  read model from them without the one it failed on, leaving a gap in the middle rather than at the end.
- **An `Error` still stops the dispatch,** rather than being collected like an exception.

### If you would rather keep the old behaviour

Wire a transaction. That was always the way to make synchronous handlers atomic with the write, and it now also selects
the stop-at-the-first-failure dispatch:

```java
GenericApplicationService.builder(eventStore, cloudEventConverter)
        .synchronousSubscriptions(synchronousSubscriptionModel)
        .transactionExecutor(new SpringTransactionExecutor(transactionManager))
        .build();
```

### If you implement the dispatcher yourself, this one does not compile

**`SynchronousEventDispatcher.dispatch(List)` and `ReactiveSynchronousEventDispatcher.dispatch(List)` are gone,
replaced by `dispatch(List, boolean transactional)`.** This is the only source break in 0.32.0, and it is deliberate:
your implementation owns the handler loop, so if the flag were optional you would keep stranding handlers with nothing
to tell you. A compile error asks the question instead.

The migration is to add the parameter and act on it:

```java
@Override
public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
    for (CloudEvent cloudEvent : writtenCloudEvents) {
        if (transactional) {
            // A failure rolls the write back, so stopping here loses nothing.
            handlers.forEach(handler -> handler.accept(cloudEvent));
        } else {
            // The write has committed, so give every handler the event and report the failures afterwards.
            isolate(handlers, handler -> handler.accept(cloudEvent));
        }
    }
}
```

If you would rather keep exactly your old behaviour for now, ignore the parameter. That compiles and behaves as before,
and it is a choice you have made rather than one made for you.

`SynchronousSubscriptionModel` keeps a one-argument `dispatch(List)` of its own, so code driving the model directly, for
example a test or an in-memory write listener, is unaffected.

### If you implement a transaction executor yourself

`TransactionExecutor` gained `isTransactional()`, and `ReactiveTransactionExecutor` gained `isTransactional()` returning
`Mono<Boolean>`. Both default to reporting no transaction, which selects the isolating dispatch, because that is the
answer that cannot lose a reaction if you forget. Override it if you open one.

**Answer for the moment of the call, not for your executor as a whole.** The application service asks during dispatch,
which runs inside your `inTransaction`, so if whether you open a transaction depends on configuration or on what the
caller already opened, read the live state. That is what the two Spring executors Occurrent ships now do, since a
`TransactionTemplate` configured with `PROPAGATION_NOT_SUPPORTED` or `PROPAGATION_NEVER` opens nothing and a fixed
`true` would be a lie that costs the handlers behind a failure their event.


## 5. The reactor `SubscriptionModel` is now `FluxSubscriptionModel`

Only relevant if you write reactive subscriptions. Nothing on the blocking stack moved.

`org.occurrent.subscription.api.reactor.SubscriptionModel` is renamed to `FluxSubscriptionModel`. It is the same
interface with the same single method, the one that returns a `Flux<CloudEvent>` you subscribe to and dispose
yourself.

The old name is taken over by a new interface that means what the blocking `SubscriptionModel` has always meant, a
named subscription model you can pause, resume and cancel by id. It is `Subscribable` plus
`SubscriptionModelLifeCycle` and adds no methods, so nothing you already implement has to grow.

### Run the recipe

The same `org.occurrent.UpgradeToOccurrent_0_32` recipe from
[section 1](#1-occurrentsubscriptionenabled-becomes-occurrentsubscriptionmode) does this rename too, in Java and
Kotlin alike. If you already ran it, you already have both changes.

Run it once, as part of the upgrade. It rewrites every reference to the old name, and it cannot tell code you wrote
against the new `SubscriptionModel` afterwards from code that predates the rename, so running it a second time later
would rename the new references too.

### By hand

Change the import and the type name:

```java
// Before
import org.occurrent.subscription.api.reactor.SubscriptionModel;

SubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(mongo, "events", timeRepresentation);
Flux<CloudEvent> events = subscriptionModel.subscribe();
```

```java
// After
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;

FluxSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(mongo, "events", timeRepresentation);
Flux<CloudEvent> events = subscriptionModel.subscribe();
```

Watch the wildcard case. If you imported `org.occurrent.subscription.api.reactor.*` and referred to
`SubscriptionModel` unqualified, that name still compiles, because the new interface has it. Your code will then be
typed on the named lifecycle-managed interface rather than the `Flux`-returning one, and the `subscribe()` call
above stops compiling. The recipe handles this correctly, so it is the safer route.

### Why it was renamed

The two stacks used one name for two different things, which meant a reactive subscription model had no type naming
what a blocking one names with `SubscriptionModel`. The subscription TCK needs that type, and inventing a third name
for a concept the blocking stack already names would leave a reader asking what the difference is. There is none.
[ADR 98](../architecture/decisions/0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md)
has the full reasoning.


## 6. A durable reactor model over a catch-up model over a cold-only model

`ReactorDurableSubscriptionModel` now hands its subscriptions to the model it wraps whenever that model manages named
subscriptions itself, and the three reactor catch-up models now do. If your composition is
`Durable(Catchup(customModel))` where `customModel` implements only the cold `FluxSubscriptionModel` primitive, the
catch-up model has nothing underneath to delegate the live half to, and refuses with:

> `ReactorStreamCatchupSubscriptionModel can only manage named subscriptions when the model it wraps manages them
> itself (implements SubscriptionModel). The wrapped <your class> only offers the plain (cold) subscribe(filter,
> startAt) primitive, so use that primitive directly, or wrap a model that manages named subscriptions.`

Before this release the same composition ran through the durable model's own delivery loop, which retried nothing and
validated nothing, the gap [#547](https://github.com/johanhaleby/occurrent/issues/547) records. The remediation is in
the message. Implement the reactor `SubscriptionModel` on your model, the way every model shipped by Occurrent now
does, and the composition inherits its retry and validation. If you cannot, subscribe to the catch-up model's cold
`Flux` directly and manage the delivery yourself, which is what the old path silently did for you without the
resilience you probably assumed it had.

Only the named `subscribe(..)` paths refuse. The model-wide life-cycle methods stay safe on such a composition:
`shutdown()` (a Spring context close calls it through `destroyMethod`) and `stop()` are no-ops, `isRunning()` answers
`false`, and cancelling an id the composition never knew is ignored, so an application that keeps the cold-only
composition but never subscribes by name still starts, health-checks, and shuts down cleanly.

## 7. A paused MongoDB subscription delivers what was written while it was paused

Nothing to change in your code, but the behaviour under you moved, so check your handlers before you upgrade.

`SpringMongoSubscriptionModel` used to rebuild its change stream from the `StartAt` the subscription was created with.
For the default that resolves to the present all over again, so an event written while a subscription was paused sat
behind where the resumed stream started and was never delivered. It now resumes from the change-stream position it had
read to, which is what `NativeMongoSubscriptionModel` and `ReactorMongoSubscriptionModel` already did, so the paused
window arrives.

The same position is used when a subscription restarts after a change-stream error, where the model also used to
reconnect at the present. A subscription whose change stream history is no longer in the oplog still restarts at the
present, and still only if you asked for that with `restartSubscriptionsOnChangeStreamHistoryLost`. Without it, such a
subscription stops and says so rather than silently skipping ahead, which is what that setting has always promised.

### What this asks of you

**Handlers must be idempotent.** Delivery across a pause is at least once, and `stop()` on the model pauses every
subscription it holds, so `stop()` followed by `start()` is the same case. Three things produce a repeat:

- An event whose handler had not finished when the subscription was paused. The position advances after the handler
  returns, so pausing mid-handler means that event is handed over again on resume.
- An event whose handler threw. The position does not advance past an event nobody processed, so the subscription
  restarts on it rather than moving past it. This one is not new in spirit, since the checkpoint never advanced past
  a failed event either, but it is new in the moment it happens. The retry used to wait for the next application
  restart, and now it happens straight away. With `RetryStrategy.none()` the handler therefore sees the event a
  second time before the subscription gives up.
- Every event another consumer of the same subscription id handled while this one was paused. A competing consumer is
  paused precisely because a rival holds the lease, and the rival has been delivering in the meantime. When the lease
  comes back, this consumer resumes from where *it* left off, not from where the rival got to.

If your handler writes to a read model with an upsert keyed on the event, or checks the `streamid`/`streamversion` or
`position` extension before acting, you already have what this needs. If it appends to a list or increments a counter
without looking, it will double-count now where a pause used to hide the problem by dropping the event instead.

**A long pause can now outlive the oplog, and the default is to stop rather than skip.** Resuming at the present could
never fail, because the present is always in the oplog. Resuming from a position can, and MongoDB answers with change
stream history lost (error 286) when that position has rolled off. A competing consumer on standby for longer than your
oplog window is the case to think about, and so is any outage that outlasts it. With
`restartSubscriptionsOnChangeStreamHistoryLost` left at its default of `false` the subscription logs the error and stops
delivering, and `isRunning(id)` keeps answering `true`, so nothing in the API tells you it has gone quiet. Three ways
out, and the right one depends on what the subscription is for:

- Size the oplog for the longest standby you expect. This is the only one that keeps every event.
- Set `restartSubscriptionsOnChangeStreamHistoryLost(true)` if you would rather the subscription restart at the present
  and lose the window it could not reach. That is the old behaviour, now something you ask for rather than something
  that happens quietly.
- Put a catch-up model in front of a subscription that may stand by for a long time, so it replays from the event store
  instead of depending on the oplog at all.

### Why the trade goes this way

Both answers cost something. Resuming at the present loses events, and resuming from the position read repeats them.
They are not equally bad. A lost event is unrecoverable and violates the isolation rule Occurrent is designed around, while
a repeat is absorbed by an idempotent handler, and every wrapper above these models already delivers at least once.
[ADR 94](../architecture/decisions/0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md)
records the measurement this was decided against, including the competing-consumer case where it costs the most.

## 8. A push catch-up replays on its own thread

Only relevant if you call `subscribe(...)` on `CatchupThenPushSubscriptionModel` yourself, on either stack. Nothing
changes if the model is bound for you by `@Projection(source = PUSH)` or `@Saga(source = PUSH)`, or if you go through
the projection DSL: the registrars and runners call `waitUntilStarted` on your behalf, so under the default
`startupMode` a replay failure still fails your application's startup exactly as it did before.

The model replays a projection's history from the event store before handing over to the live push feed. That replay
used to run before `subscribe(...)` returned: on the blocking stack it ran on the calling thread, and on the reactor
stack the model subscribed its own replay pipeline inline, so with a synchronous reader the history had been applied by
the time you held the handle. It now runs off that thread on both stacks, on a virtual thread of its own on the
blocking one and on `boundedElastic` on the reactor one, and `waitUntilStarted()` on the returned subscription is the
only thing that joins it. That is what lets `startupMode = BACKGROUND` keep the largest replay Occurrent runs off the
startup path, which is the reason for the change.
[ADR 91](../architecture/decisions/0091-a-push-catch-up-replays-off-the-startup-path.md) has the full reasoning.

Two things follow for a direct caller.

**On the blocking stack, a replay failure moves.** It used to be thrown out of `subscribe(...)`. It is now rethrown
from `waitUntilStarted()`, so a `try`/`catch` around `subscribe(...)` alone no longer catches anything, and the
projection behind it starts silently empty. Move the handling to the wait:

```java
// Before
try {
    catchupModel.subscribe("orders", this::updateOrderView);
} catch (RuntimeException e) {
    // react to the replay failure
}
```

```java
// After
Subscription subscription = catchupModel.subscribe("orders", this::updateOrderView);
try {
    subscription.waitUntilStarted();
} catch (RuntimeException e) {
    // react to the replay failure
}
```

The wait tells the outcomes apart. An exception means the replay failed, `false` means the model was stopped (or that
the timeout expired, if you passed one with `waitUntilStarted(Duration)`), and `true` means the projection is caught up
and live.

**After a failure the subscription keeps its registration and refuses every event fed to the live feed.** It used to
release the registration instead, which freed the subscription id but meant your listener acknowledged every later
event into a projection that received nothing, and your broker then discarded them. Now the acknowledgement fails, so
the source holds the backlog. Fix the cause, call `cancelSubscription(..)` to release the id, and subscribe again. The
fresh `subscribe(...)` replays the whole history, because nothing was recorded as caught up.

Two things follow. The subscription id stays taken until you cancel it, so a second `subscribe(...)` under the same id
is refused rather than quietly replacing the failed one. And `isRunning(id)` now answers `true` for a subscription
whose catch-up failed, because it is registered and refusing, where it used to answer `false`.

**A stop is reversible, and a failure is not.** If you stop the model while a replay is running, `start(true)` replays
that history again from the beginning, and `start(false)` leaves it for `resumeSubscription(..)`. A replay that
*failed* is never restarted by `start(..)`, since that would turn the refusal into a restart loop. Note that the
`Subscription` handle you already hold tracks the replay it was created for, so after a stop it keeps reporting
`false`. Ask `isRunning(id)`, or `isCatchingUp(id)` on the blocking model, or take the handle `resumeSubscription(..)`
hands back.

**On both stacks, the state is not there yet when `subscribe(...)` returns.** Code that read the projected state
straight after subscribing was reading a finished replay before and is racing one now. Call `waitUntilStarted()` first
on the blocking stack, and on the reactor stack compose the returned subscription's `waitUntilStarted()` `Mono` before
whatever reads the state. The reactor failure path is not new, it always arrived through that `Mono`, but the inline
replay meant the state happened to be complete by the time you could ask, which is exactly the kind of accident this
change removes.

## 9. `NativeMongoLeaseCompetingConsumerStrategy` moves to the native-driver package

`org.occurrent.subscription.mongodb.spring.blocking.NativeMongoLeaseCompetingConsumerStrategy` is renamed to
`org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy`. Nothing about the
class changes, only where it lives.

It took a `MongoCollection<BsonDocument>` from the native Java driver and never touched Spring, but shipped under the
`spring.blocking` package that the three Spring competing-consumer artifacts own for real. Every other native-driver
subscription type, `NativeMongoSubscriptionModel` and `NativeMongoCheckpointStorage` among them, lives under
`nativedriver.blocking`, so this class was the one outlier, and its import line read as though an application that
deliberately avoided Spring depended on it anyway. It also meant `occurrent-subscription-mongodb-native-blocking-competing-consumer-strategy`
shipped a `package-info.class` identical to the one the three Spring artifacts ship for the package they actually own,
so an application using all four modules together carried four copies of the same class file. Resolves
[#534](https://github.com/johanhaleby/occurrent/issues/534).

### Run the recipe

The same `org.occurrent.UpgradeToOccurrent_0_32` recipe from
[section 1](#1-occurrentsubscriptionenabled-becomes-occurrentsubscriptionmode) rewrites this too, including the
qualified `NativeMongoLeaseCompetingConsumerStrategy.Builder` construction and the `withDefaults(..)` static factory
call. If you already ran it for section 1 or section 5, you already have this change.

### By hand

Change the import:

```java
// Before
import org.occurrent.subscription.mongodb.spring.blocking.NativeMongoLeaseCompetingConsumerStrategy;
```

```java
// After
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
```

The class name, its `Builder`, and every method are unchanged, so nothing else in your code needs to move.

## 10. `CompetingConsumerSubscriptionModel` refuses two calls it used to accept

Nothing to change in your code unless you make one of these two calls, and both of them were already doing something
other than what they looked like they were doing. There is no recipe, because nothing about the call shape changes,
only what it does at runtime.

**First, read this part if nothing else: running the same subscription id on several nodes is untouched.** That is the
whole point of the pattern and it works exactly as before. The refusals below are scoped to one
`CompetingConsumerSubscriptionModel` instance, and a node has one of those: the Spring starter registers it as a
singleton bean, so three nodes means three instances, each subscribing to the same subscription id and competing for
it through the lease.

### Subscribing twice to one subscription id on one instance now throws

```java
model.subscribe("my-subscription", event -> ...);
model.subscribe("my-subscription", event -> ...); // IllegalArgumentException: Subscription my-subscription is already defined.
```

The same applies to the overload that takes an explicit subscriber id, so two subscriber ids for one subscription id on
one instance are refused too.

This is what every other subscription model has always done, and this one is the odd one out rather than the other way
round. The second call used to register a second competing consumer, which read like a working standby and was not one:
`cancelSubscription`, `pauseSubscription` and `resumeSubscription` all find a consumer by subscription id alone, so the
second registration was unreachable through all three, and both shared the one wrapped model, which refuses a duplicate
id itself. If you were doing this to get a standby consumer, what you actually need is a second instance, which is what
a second node gives you.

One related fix comes with it: cancelling a subscription that opted out of competing consumption (a `StartAt` resolving
to `null` for `CompetingConsumerSubscriptionModel`) now frees its subscription id, where the model used to remember it
forever. If you cancel and re-subscribe such a subscription, that used to leave the model trying to resume it on the
next `start()`.

### Pausing a subscription the model does not have now throws

```java
model.pauseSubscription("never-subscribed"); // IllegalArgumentException: Subscription never-subscribed is not running
```

`SubscriptionModelLifeCycle.pauseSubscription` has always documented `@throws IllegalArgumentException If subscription
is not running`, and the wrapped model has always thrown it. The wrapper looked for a competing consumer of its own,
found none, and returned quietly, so the call reported success and did nothing. If you pause subscriptions by iterating
over ids from somewhere other than the model itself, use `subscriptionIds()` or guard with `isRunning(id)`.

One case is deliberately left alone and is worth knowing about if you pause across a cluster: pausing a subscription on
a node that is currently *waiting* for the lease still does nothing and still does not throw. Closing that properly
means the model reporting a waiting consumer through `isPaused`/`isRunning`, which is a larger change than this one and
is tracked as [#565](https://github.com/johanhaleby/occurrent/issues/565).

### Why

The reasoning, including why uniqueness is scoped to the instance rather than to the subscription id everywhere, is in
[ADR 102](../architecture/decisions/0102-a-subscription-id-is-unique-per-subscription-model-instance.md). Both
behaviours were found by the subscription TCK, whose general subscription-model suite now runs against this model for
the first time.

## 11. `start()` twice is allowed, and `waitUntilStarted` stops saying yes when it means no

No call shape changes here either, so there is no recipe. What changes is the answer you get back.

### A second `start()` no longer throws

```java
model.start();
model.start(); // used to be IllegalStateException on CompetingConsumerSubscriptionModel, now accepted
```

`CompetingConsumerSubscriptionModel` was the only subscription model on either stack that refused this, and it is the
one the Spring Boot starter gives you by default, so the model most applications hold was the one with the odd answer.
If you wrote a guard to work around it, you can drop it.

```java
if (!model.isRunning()) { // no longer needed
    model.start();
}
```

Starting a model is now a goal rather than a transition. `start(true)` leaves the model running with every subscription
it can bring up brought up, including one you paused yourself with `pauseSubscription(id)`. If you were relying on a
paused subscription surviving a `start()`, pause it again afterwards or use `start(false)` and resume by id.

### `waitUntilStarted` answers `false` for a subscription that has not started

Three released cases change, and all three used to answer `true` or hide a failure.

```java
pushModel.stop();
Subscription subscription = pushModel.subscribe("my-projection", event -> ...);
subscription.waitUntilStarted(ofSeconds(5)); // was true, now false
```

A `PushSubscriptionModel` or `SynchronousSubscriptionModel` that is stopped drops what it is handed rather than holding
it, so a subscription registered on one has not started. Start the model, or take the handle `resumeSubscription(id)`
gives you, which is the started one.

```java
Subscription subscription = catchupModel.subscribe("my-projection", event -> ...);
subscription.waitUntilStarted(ofSeconds(5)); // a failed replay now throws instead of returning false
```

The blocking stream and DCB catch-up models used to log a failed replay at WARN and answer `false`, which meant an
application that ignored the return value carried on with an empty read model and no trace of why. The failure now
reaches you, the same way the push catch-up already reported it. If you call `waitUntilStarted` on one of those models,
put it in a `try`/`catch` or let it stop your startup. The same handle also returns its delegate's answer now instead of
a fixed `true`, so a delegate that did not start within your timeout is reported as `false`.

A catch-up cancelled or shut down before it went live answers `false` rather than `true`, and on the reactive stack the
`Mono` fails instead of completing.

### What did not change

A subscription that has started keeps answering `true` afterwards, even once it is paused, or stopped, or waiting for
another node to release its competing consumer lock. That last one matters if you run competing consumers with
`@Subscription(startupMode = WAIT_UNTIL_STARTED)`, because a node that has not won the lock still starts up rather than
blocking. Use `isRunning(id)` and `isPaused(id)` when you want to know what is happening right now.

### Why

[ADR 105](../architecture/decisions/0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md)
has the reasoning, including why a lock-waiting competing consumer counts as started and a registration you still have
to start yourself does not. `SubscriptionModelConformance` asserts the `start()` half, so every model has to agree on it
now.

## 12. Every subscription refusal has its own exception type

A subscription model used to throw a bare `IllegalArgumentException` whichever way you got it wrong, and the only
thing telling the cases apart was a message the conformance suite says is not part of the contract. Each condition
now has a type of its own.

| You did this | You now get |
|---|---|
| `subscribe(..)` with an id this model instance already has | `DuplicateSubscriptionIdException` |
| `subscribe(..)` with a filter shape the model cannot apply | `UnsupportedSubscriptionFilterException` |
| `subscribe(..)` with a start position the model does not accept | `UnsupportedStartAtException` |
| `pauseSubscription(..)` on a subscription that is not running | `SubscriptionNotRunningException` |
| `resumeSubscription(..)` on a subscription that is running | `SubscriptionAlreadyRunningException` |
| `pauseSubscription(..)` or `resumeSubscription(..)` with an id the model has never seen | `UnknownSubscriptionException` |

### Nothing you wrote stops compiling

All six extend `IllegalArgumentException`, which is what every one of these calls threw before, so an existing
`catch (IllegalArgumentException e)` still catches all of them and no call site changes shape. There is no recipe to
run. What changes is the class you get and the message it carries, so a test asserting
`isExactlyInstanceOf(IllegalArgumentException.class)` or a specific message needs updating, and a test asserting
`isInstanceOf(IllegalArgumentException.class)` does not.

The types are sealed under `SubscriptionRefusedException`, so you can catch the whole set at once, or `switch` over
them exhaustively.

```java
try {
    subscriptionModel.resumeSubscription(id);
} catch (UnknownSubscriptionException e) {
    // no subscription with that id here, so try the next model
} catch (SubscriptionAlreadyRunningException e) {
    // this model owns the id and it is already running
}
```

### One behaviour really did change

`UnknownSubscriptionException` is new, and not only as a name. Pausing or resuming an id no model had ever seen used
to report that the subscription was not running or not paused, which claimed something about a subscription that did
not exist. If you have code reading those messages to work out whether an id exists, you can ask for the type
instead. `subscriptionId()` on any of the four id-scoped types tells you which id the refusal was about.

### A missing payload reader is a different kind of refusal

A model or store built without a `DataFieldReader`, asked to filter on a field inside the `data` payload, now throws
`UnsupportedOperationException` instead of `IllegalArgumentException`. No filter you pass instead makes a payload
readable, so it is the same kind of answer an event store already gives for a capability it was not built with. Build
the store or the model with a reader, for example the Jackson-backed one in
`occurrent-common-inmemory-filter-matching-jackson`.

### Why

The reasoning, including where the line between an argument exception and an unsupported operation falls and why a
failed catch-up or a competing consumer's lock stays an `IllegalStateException`, is in
[ADR 106](../architecture/decisions/0106-a-refused-subscription-call-says-which-condition-it-hit.md).

## 13. `DomainEventFeed` refuses an event when no projection is registered

Only relevant if you declare a `DomainEventFeed` bean and feed it from a listener yourself, on either stack.

`accept(..)` used to return normally when nothing was registered on the feed. Its own documentation tells you to
acknowledge the broker message once `accept` returns, so a listener wired before the projection was registered
acknowledged events that no projection ever received, and the broker then discarded them. It now throws an
`IllegalStateException` instead, and on the reactor stack the returned `Mono` fails with one, so the message goes
unacknowledged and your source redelivers it.

This is easiest to reach under `occurrent.subscription.mode=manual`, where the registration is deferred until you call
`ManualStartPushSources.startAll()`. Anything your listener consumed before that point was lost. Refusing is what makes
the manual mode mean what [ADR 86](../architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md)
says it means, that a subscription registered but not started withholds events rather than losing them, because the
broker is the only thing holding a backlog and it only holds one while nobody acknowledges.

If your listener starts consuming before registration and you would rather check than catch, the feed now answers
`hasProjection()`:

```java
@RabbitListener(queues = "orders")
public void onOrderEvent(OrderEvent event) {
    if (!orderFeed.hasProjection()) {
        throw new AmqpRejectAndDontRequeueException("Projection not registered yet");
    }
    orderFeed.accept(event);
}
```

`catchUpAll()` changes the same way. It used to do nothing on a feed with no projection and report success, so a
misconfigured feed looked caught up and then fed nothing. It now throws, matching `catchUp(String)`, which always did.
`stopCatchUp()` is unchanged and stays a no-op, because it runs on a shutdown path where throwing helps nobody.

`PushSubscriptionModel.accept(..)` is deliberately **not** changed. It is also fed from the write path, for example as
an `InMemoryEventStore` listener, and there the event is already stored and a later catch-up replays it, so refusing
would fail the write while protecting nothing. Ask its `hasSubscriptions()` when you drive it from a broker.

### Why

[ADR 104](../architecture/decisions/0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) records the whole
contract, including why a stopped model still drops events while an unregistered or failed one refuses them.

## 14. A live push handler can now be called concurrently

Only relevant on the blocking stack, and only if a `CatchupThenPushSubscriptionModel` or a `CatchupProjectionFeed` (or
the `@Projection(source = PUSH)` / `@Saga(source = PUSH)` annotations built on them) is fed by more than one thread
once it has gone live — a listener container configured with concurrency greater than one, say.

Once such a subscription or feed is live, every payload used to be folded while holding the handover's own internal
lock, so concurrent broker threads queued on it and your handler ran one call at a time no matter how much
concurrency the listener container was configured for. That was never a documented guarantee, and it was costing real
throughput: a benchmarked handler doing a small synchronous I/O call plateaued at roughly one payload per handler
duration regardless of thread count. The handler call now runs outside that lock, so a concurrently-fed subscription
gets concurrent handler calls.

**If your handler is not thread-safe** — it mutates a field, a non-concurrent collection, or anything else without its
own synchronization — and you configure more than one delivering thread, it can now see real races it could not see
before. Either make the handler safe for concurrent invocation (an idempotent write to a database is usually already
safe; an in-memory accumulator usually is not), or keep the delivering side single-threaded, which is the default
unless you explicitly configure more.

Delivery order across concurrently-delivering threads is not guaranteed either, where the lock used to impose one as a
side effect. A single-threaded caller sees no change at all: delivery is still synchronous on the calling thread, in
the order `accept` was called.

### Why

[ADR 107](../architecture/decisions/0107-a-live-push-handler-runs-outside-the-handover-lock.md) has the reasoning,
including the benchmark that measured the win before this was decided.
