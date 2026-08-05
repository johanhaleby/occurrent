# Upgrading to Occurrent 0.32.0

Three things break, and only if you use the features they belong to.

**At compile time**, if you write reactive subscriptions, one type was renamed: the reactor `SubscriptionModel` is now
`FluxSubscriptionModel`. The recipe below rewrites it for you. Read
[section 5](#5-the-reactor-subscriptionmodel-is-now-fluxsubscriptionmodel).

**Also at compile time**, if you implement `SynchronousEventDispatcher` or `ReactiveSynchronousEventDispatcher`
yourself, one method gained a parameter. Almost nobody does, since the model Occurrent ships implements it for you. Read
[section 4](#4-a-synchronous-subscription-no-longer-stops-at-the-first-failing-handler).

**At startup**, a push sink now feeds exactly one projection or saga, so an application that shared one between several
refuses to start. If you use a push source, read
[section 3](#3-a-push-sink-feeds-exactly-one-projection-or-saga) first.

Five things are worth reading. One configuration property is deprecated and has a recipe that rewrites it for you, the
MongoDB event stores changed how they persist the CloudEvent `time` attribute under
`TimeRepresentation.RFC_3339_STRING`, a push sink feeds one consumer, a synchronous subscription no longer stops at the
first failing handler, and the reactor subscription primitive was renamed.

## 1. `occurrent.subscription.enabled` becomes `occurrent.subscription.mode`

`occurrent.subscription.enabled` was a boolean. Its replacement is an enum with three values, because there is now a
third thing you can ask for:

| Old | New | What it means |
|---|---|---|
| `occurrent.subscription.enabled=false` | `occurrent.subscription.mode=disabled` | No subscription beans at all |
| `occurrent.subscription.enabled=true` | `occurrent.subscription.mode=auto` | Subscriptions are created and started, the default |
| no equivalent | `occurrent.subscription.mode=manual` | Every subscription is registered, none of them runs until you start it |

The old property still works and is deprecated, so nothing breaks if you upgrade without touching your configuration.
It is removed in the release after next. Setting both is allowed while they agree, which is deliberate: a rewritten
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
makes those comparisons behave, which fixes two things: an exact filter such as `Filter.time(instant)` now matches an
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
them. A broker message carries one acknowledgement decision, so those consumers shared it: a consumer that kept failing
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
projection. A single queue behind two sinks still couples them, just at the transport rather than in Occurrent: only
one of the two consumers would receive any given message. Whether that means a queue per consumer on a fanout exchange,
a consumer group per projection, or something else is your broker's vocabulary rather than Occurrent's.

If you drive the sinks yourself rather than through `@Projection`, the same applies: construct one per consumer and
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
the message: implement the reactor `SubscriptionModel` on your model, the way every model shipped by Occurrent now
does, and the composition inherits its retry and validation. If you cannot, subscribe to the catch-up model's cold
`Flux` directly and manage the delivery yourself, which is what the old path silently did for you without the
resilience you probably assumed it had.
