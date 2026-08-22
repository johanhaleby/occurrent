# 133. A broker is a transport for the push feed, and never a subscription model

Date: 2026-08-18

## Status

Accepted. Decides [#412](https://github.com/johanhaleby/occurrent/issues/412), the design layer for the broker work
tracked by [#388](https://github.com/johanhaleby/occurrent/issues/388). This ADR decides a design and writes no code.
The modules it names are built by #413 through #417.

**Scope.** Blocking only. The reactor variants are [#418](https://github.com/johanhaleby/occurrent/issues/418) and are
not decided here, beyond the requirement that nothing below prevents them. Spring Boot auto-configuration is
[#846](https://github.com/johanhaleby/occurrent/issues/846) and is not in scope either, for the reason given in
decision 9. The `@RabbitSaga` and `@KafkaSaga` sketches on
[#415](https://github.com/johanhaleby/occurrent/issues/415) and
[#417](https://github.com/johanhaleby/occurrent/issues/417) are ideas recorded for a future Spring Boot layer, not
part of this design.

## Context

A production deployment often forwards Occurrent cloud events to RabbitMQ or Kafka and consumes them there instead of
reading a MongoDB change stream. [ADR 62](0062-pluggable-projection-event-source.md) made that possible without
Occurrent depending on any broker. `PushSubscriptionModel` is a register-only `Subscribable` driven by an external
`accept(CloudEvent)` call, `Pushable` is the capability a listener depends on, and `DomainEventFeed` is the same idea
one level up, for an application whose own message converter already produced a domain event.

Everything on either side of `accept(...)` was left to the application. It writes the publisher, decides the exchange
or the topic, decides what goes in the message body and what goes in the message headers, writes the listener, and
works out how to put the Occurrent extensions back on the event it rebuilds.

Two applications doing that work arrive at two different message shapes. That on its own would only be a duplication
problem. The real problem is that one application's publisher and its own consumer, written months apart by different
people, can disagree about the same mapping, and nothing in the type system or the tests says so. The event just stops
matching the filter it was supposed to match.

### Why the consume side is not a new subscription model

The design that suggests itself first is a `RabbitMqSubscriptionModel` and a `KafkaSubscriptionModel` sitting next to
the MongoDB ones. Neither can honestly implement `Subscribable.subscribe(id, filter, startAt, action)`.

`StartAt` names a place in the event store's history. A broker holds the live tail plus whatever its retention keeps,
which is neither the store's history nor addressable by an Occurrent position. A broker-backed model handed
`StartAt.subscriptionModelDefault()` could only begin from wherever the queue or the consumer group happens to be, and
then report success. A caller asking for a replay would get a live-only subscription and no error.

ADR 62 already answered this the other way round, and the answer still holds. Replay is the event store's job, so
`CatchupThenPushSubscriptionModel` replays through `PositionOrderedReader` and then hands over to the live feed, while
live-resume is the broker's job, because the broker already redelivers what was not acknowledged. A broker-specific
subscription model would have to reimplement the first half worse and duplicate the second.

### What is actually missing

Two things, and both are about agreement rather than about delivery.

The publisher and the consumer have to agree on where an event goes. Today they agree by two people writing the same
string twice, once in an exchange-and-routing-key expression and once in a queue binding.

The publisher and the consumer also have to agree on what a message looks like once it arrives. The Occurrent
extensions are the part that matters. `streamid`, `streamversion` and `position` come from
`OccurrentCloudEventExtension`, and `dcbtags` comes from `EventStoreCloudEventExtensions`. `EventMetadata` reads them
off the CloudEvent, and every DSL that keys on stream or position reads them through it.

A message that arrives without them produces an empty `EventMetadata`. A projection keyed by metadata now fails loudly
on that, because ADR 62's 2026-07-26 amendment added the delivery-time guard and `ProjectionKeys.failIfKeyNeededMetadata`
throws rather than resolving the key to `null` and skipping the event. So the worst case is no longer silent, and this
ADR does not need to argue as if it were.

What stays quiet is a consumer that is not keyed by metadata and reads `EventMetadata.getPosition()` or `get(key)`
for its own purposes. Those still answer `null` on empty metadata, and nothing guards a caller that treats the answer
as real. That is the case the header rule in decision 4 protects.

## Decision

### 1. Each transport module contributes a bridge into the push feed that already ships

A transport module consumes a broker message, rebuilds the event, and hands it to `Pushable.accept(CloudEvent)` at the
CloudEvent level or to `DomainEventFeed.accept(EventMetadata, E)` at the domain level.

No new `SubscriptionModel` is added, for the reason above. `PushSubscriptionModel`, `Pushable`, `DomainEventFeed` and
`CatchupThenPushSubscriptionModel` keep the shapes they have. The one exception is `PushObserver` and the outcome
`routeReportingMatch` reports to it, which this decision changes for the reason given below.

Two existing rules constrain what a bridge may do, and both are there to stop events being lost.

[ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) decided that an `accept(...)` which returns
normally without delivering is an acknowledgement of an event nothing consumed. So a bridge never acknowledges when
`accept(...)` throws.

A normal return is not on its own proof of delivery, and the CloudEvent bridge needs one more check because of it.
ADR 104 made `DomainEventFeed` refuse when nothing is registered, so the domain bridge can read a normal return as
delivery. It deliberately did not make `PushSubscriptionModel` refuse, because that model is also fed from the write
path by `new InMemoryEventStore(pushModel::accept)`, where refusing would fail a write to protect nothing. A stopped
model also drops live events and returns normally, which ADR 85 decided and ADR 104 kept.

So a CloudEvent bridge that only looked at whether `accept(...)` threw would acknowledge and lose events in three
states, before anything is registered, while the model is stopped, and while its subscription is paused. `route`
returns normally in all three.

**The acknowledgement decision comes from a `PushObserver`, not from a check taken before or after the push.**
`PushSubscriptionModel` reports through `routeReportingMatch`, which evaluates the sole registration's eligibility
once and tells the observer what it decided. (The 2026-08-21 amendment below moves that report to after the matched
action runs, replacing the mechanism this paragraph originally described. The decision itself, one evaluation
shared between the match and the report, is unchanged.)

**The bridge cannot attach that observer, so the wiring order is part of this decision.** The observer is a
constructor argument on `PushSubscriptionModel` and there is no method to set one afterwards, while the bridge is
constructed from a model that already exists. So the application creates the outcome channel first, passes it to the
model's constructor, and passes the same channel to the bridge, which reads its outcomes. That channel delegates to
whatever `PushObserver` the application already wanted, so a deployment with its own diagnostics keeps them instead of
having to choose. A check taken separately
would be two steps, so a `stop()`, a `pauseSubscription` or a `cancelSubscription` landing between them would
acknowledge into a model that then drops the event.

**`PushObserver` has to report why, not just whether, and that is a prerequisite this ADR creates.** Today it is told
a boolean, and `routeReportingMatch` reports `false` for four different situations, the filter not matching, the model
not running, the subscription being paused, and nothing being registered. Only the first of those may be acknowledged.
The other three mean the event was not delivered, so acknowledging discards it.

A lifecycle check afterwards cannot separate them. A pause reports `false`, a concurrent resume then makes
`isRunning(subscriptionId)` true, and the bridge acknowledges a message nothing consumed. That is the same two-step
race one paragraph up, just moved after the push, so it has to be answered in the routing evaluation itself.

So the push module gains an outcome reported from that one evaluation, `DELIVERED`, `FILTERED` and `NOT_DELIVERABLE`
at the time this decision was written (`DEFERRED` was added by the 2026-08-21 amendment below, and `UNAVAILABLE`
and `REFUSED` by the 2026-08-22 one, which also narrows what `NOT_DELIVERABLE` means), and
the bridge acknowledges on `DELIVERED`, and on `FILTERED`, where redelivering would loop forever because the event
is simply not this consumer's. It does not acknowledge on `NOT_DELIVERABLE`, on `DEFERRED`, or when the matched
action throws. (Originally worded around `accept(...)` returning normally. The amendment below changes which method
a bridge calls and when the report happens, not this acknowledgement rule.)

This refines a shape that has not shipped rather than breaking a released one, since `PushObserver` is still in the
unreleased section of `changelog.md`, so its existing entry absorbs the outcome contract instead of a migration note
being needed. [#848](https://github.com/johanhaleby/occurrent/issues/848) owns the change and has to be done before
the consume bridges in #415 and #417, which are what need the contract.

#848 owns a second prerequisite too, the live match decision 5 needs on `DomainEventFeed`. Those two are the only
changes this design needs outside the three broker modules, and both are prerequisites rather than improvements, so
neither belongs at the end of an implementation plan.

**The bridge therefore holds the `PushSubscriptionModel` rather than a bare `Pushable`.** The observer is a
constructor argument on the model, and the readiness accessors are not reachable through `Pushable` either. The bridge
uses `isRunning(subscriptionId)` to decide when to start and stop consuming, which is a coarse lifecycle question
where a small delay is harmless, and never to decide a single message. `hasSubscriptions()` would be the wrong
question even there, because it stays true for a paused subscription while `route` skips exactly that registration.
The bridge knows which id to ask about because ADR 90 gives it exactly one.

What remains is that a stopped model drops live events, which ADR 85 decided and ADR 104 deliberately kept, on the
grounds that a stop is an operator act with a `start()` on the other side. A bridge stops consuming when its
subscription stops, checked on a coarse poll rather than before every message, so it mostly does not feed a stopped
model, not never: a `stop()` or `pauseSubscription(...)` called from inside a handler can still hand a bridge an
`UNAVAILABLE` for a message already in flight before the next poll notices. The 2026-08-21 amendment's
follow-up note below covers what a bridge does with that message. This ADR inherits ADR 85 and ADR 104's decision
rather than accepting a new loss of its own.

The bridge feeds the live `PushSubscriptionModel`. When a subscription needs history first, `CatchupThenPushSubscriptionModel`
composes in front of it and takes that model as a constructor argument, so it is not itself the push target and a
bridge does not hand events to it.

[ADR 90](0090-a-push-sink-feeds-one-consumer.md) decided that a push sink takes exactly one consumer, because one
received message has one acknowledgement decision. So one bridge feeds one `PushSubscriptionModel` or one
`DomainEventFeed`, and a deployment with three projections has three queues on RabbitMQ or three consumer groups on
Kafka.

### 2. Three interface families, in an api module with no transport dependency

**`EventDestination`** says where an event goes. It is an interface in the api module, and each transport module
contributes one record implementing it, because an exchange and a routing key are not a topic and a partition key and
pretending otherwise buys nothing. `RabbitMqDestination` has an exchange, a routing key and headers.
`KafkaDestination` has a topic, a nullable message key and headers.

Headers are a component of the record from the start rather than a later addition. A header added by the sink instead
would be the same for every event the sink publishes, and the whole reason to want application headers is that they
vary with the event.

**A destination means slightly different things in the two directions, and both are fixed here** because decision 5
returns the same record type to a consumer that is declaring bindings. Publishing uses every component. A binding uses
only the routing ones, the exchange and routing key on RabbitMQ and the topic on Kafka, and the resolver leaves the
per-message components empty in what `destinationsFor` returns, so a Kafka message key is `null` and the headers map
is empty there. A routing key in that direction is read as the binding pattern rather than as one message's exact key.
The queue or the consumer group is not a component at all, because it belongs to the consumer rather than to the
mapping, which is also why two projections reading the same event type get two queues without the resolver knowing.

**`DestinationResolver<D extends EventDestination>`** derives the destination, and it answers in both directions:

```java
D destinationFor(CloudEvent cloudEvent);

Optional<Set<D>> destinationsFor(SubscriptionFilter filter);

D catchAllDestination();
```

The reverse method returns an `Optional` because a resolver often cannot answer it. `CloudEventTypeMapper` translates
a known class to a type and a known type back to a class, and it cannot list every event type an application has, so
there is no set of destinations that means "everything". An empty result therefore means the resolver could not narrow
this filter, and decision 5 says what a consumer does with that.

The shipped implementations derive the destination from the cloud event type through `CloudEventTypeMapper`. That is
the point of putting the mapping here. A publisher and a consumer agree because they read one mapping, the same one
the application already uses to convert between `com.acme.OrderPlaced` and its domain class, instead of two hand
written strings that nothing compares.

The reverse method is what lets a consumer declare only the bindings it needs, and decision 5 says what it may and may
not be used for.

**`CloudEventSink` and `DomainEventSink<E>`** publish. This is the one type an application swaps when it already has
its own publisher wrapper, which is why routing is behind the sink rather than beside it. A wrapper that publishes
already decides both where the event goes and how it gets there, so an application replacing one of these replaces
both decisions together and nothing is left half-overridden.

```java
void publish(CloudEvent cloudEvent);

void publish(Iterable<CloudEvent> cloudEvents);
```

`DomainEventSink<E>` has the same two methods over `E`, plus a `publish(EventMetadata, E)` overload that decision 4
explains.

**A fourth type drives the publish side, and without it nothing calls a sink.** `CloudEventForwarder` runs one durable
subscription out of the event store and hands each event to a `CloudEventSink`. `DomainEventForwarder<E>` uses the
same subscription, decodes each CloudEvent once with the `CloudEventConverter` and hands the domain event to a
`DomainEventSink<E>`, so an application plugging in its own publisher wrapper receives domain events and never touches
CloudEvents. The forwarder decodes rather than the sink, which is what keeps that true.

Publication is at-least-once and needs no new mechanism to be. `DurableSubscriptionModel` already advances the
checkpoint only after the action returns, so a sink that throws leaves the checkpoint where it was and the event is
published again on the next run. That is the guarantee to document rather than one to try to improve, and it is the
same contract the consume side already works under.

All of these live in `occurrent-broker-api-blocking`. Its dependencies follow from the types above rather than from a
wish to keep the list short, so it takes `occurrent-subscription-core` for `SubscriptionFilter`, the durable
subscription module for `DurableSubscriptionModel`, the CloudEvent converter api for `CloudEventConverter`, the
cloudevents extension module for `EventMetadata`, and the CloudEvents SDK. It depends on no broker client, which is
the constraint that matters and the only one this list is really asserting.

### 3. Both directions work at the CloudEvent level and at the domain level

The CloudEvent level is the base. The domain level exists so an application whose message converter already produces
domain events does not convert twice, which is the same reason ADR 62 added `DomainEventFeed` beside
`PushSubscriptionModel`.

The domain-level types live in the same artifact as their transport, in a separate package
(`org.occurrent.broker.rabbitmq.blocking.domain` beside `org.occurrent.broker.rabbitmq.blocking`). A separate artifact
would depend on the transport artifact and add two classes to it, and a user would then have to know which of two
dependencies to declare for one broker.

The two levels are not symmetric about where the conversion happens, and the asymmetry is deliberate.

On the publish side the shipped domain sinks, `RabbitMqDomainEventSink` and `KafkaDomainEventSink`, are each built
from a `CloudEventSink` and a `CloudEventConverter<E>`. They convert and delegate rather than talking to the broker
client themselves. `DomainEventSink<E>` stays an interface with no such requirement, so an application is free to
implement it directly.

**Those shipped sinks are for a caller that starts with a domain event, and pairing them with `DomainEventForwarder`
is the one combination this design tells you not to build.** A forwarder starts from a stored CloudEvent, so decoding
it to a domain event and handing it to a sink that converts straight back means one decode and one re-encode per
event, which is exactly the double conversion ADR 62 added the domain feed to avoid. It is also lossy, because
`toCloudEvent` builds a fresh event and only the extensions are recoverable from `EventMetadata`, so the id, source,
subject and time the store recorded are regenerated rather than preserved.

So forwarding stored events out of the event store uses `CloudEventForwarder` with a `CloudEventSink`, which is the
ordinary publish path and converts nothing. `DomainEventForwarder<E>` is for a `DomainEventSink<E>` the application
implements itself, one that genuinely publishes domain events through its own converter, and there the decode happens
once and nothing re-encodes it.

**`DomainEventForwarder` still cannot preserve the stored event's core attributes, and that limit is stated rather
than left to be discovered.** It hands the sink a domain event and an `EventMetadata`, and `EventMetadata` holds
extensions, so the id, source, subject and time the store recorded do not reach the sink and whatever it publishes has
new ones. The Occurrent extensions do survive, which is what the consuming side needs to build `EventMetadata`, so
this path is sound for a consumer that keys on stream, version or position.

The restriction is easier to state by what does survive, because that is the shorter list. The Occurrent extensions
survive, because the forwarder puts them on as headers rather than deriving them.

The event type usually survives but is not promised to. `DomainEventSink<E>` does not require the sink to use the
forwarder's `CloudEventTypeMapper`, and even the same mapper need not map an aliased or renamed type back to the
string the stored event was written with. So a type filter can disagree between the broker path and a catch-up too,
just less often, and a deployment relying on it should treat this path the same way as one filtering on the
attributes below. **Everything else a
`Filter` can match on does not.** `EventMetadata.from` reads only `getExtensionNames()`, so `id`, `time`, `source`,
`subject`, `dataschema` and `datacontenttype` never reach the sink and whatever it publishes has new ones. Data
survives only where the sink's encoding is the one the store holds, which is the application's to know rather than
this design's to promise.

So a consumer filtering on any of those can decide differently on the live path than the same filter does during a
catch-up replay over the stored events, and a deployment with one uses `CloudEventForwarder`, where nothing is
regenerated because nothing is converted.

On the consume side there is no such delegation. The domain bridge reads the message body with the application's
converter and the extension headers into an `EventMetadata`, then calls `DomainEventFeed.accept(metadata, event)`.
Routing it through the CloudEvent bridge instead would decode the body into a CloudEvent and then decode it again into
a domain event, which is exactly the double conversion ADR 62 added the domain feed to avoid.

### 4. The Occurrent extensions are written as message headers, at both levels and in both directions

Whatever extensions an event has are written as message headers, never only inside the body. `streamid`,
`streamversion`, `position` and `dcbtags` are the ones that matter, and the rule is stated over whatever the event
actually has rather than over those four, because no event has all four. `dcbtags` is stamped only by a DCB append and
a stream-written event never has it, and `position` is legitimately absent when stream positions are switched off.

This is an invariant rather than a convenience. `EventMetadata` is how the subscription, saga, projection, view and
DCB DSLs all read stream identity and position, so a message that leaves them in the body produces an empty
`EventMetadata` on a consumer that reads them off the headers.

At the CloudEvent level every extension attribute on the event becomes a message header. **Only one of the two
transports gets that from the CloudEvents SDK, and the ADR says which**, because assuming both do would leave the
RabbitMQ module with no mapping at all.

Kafka uses `cloudevents-kafka`, whose binary message writer produces the headers the Kafka binding specification
defines. RabbitMQ has no such writer. The SDK's AMQP module is built on Qpid Proton and targets AMQP 1.0, while this
design uses the AMQP 0-9-1 client, which is where `basicNack`, exchanges and routing keys come from, and nothing in
the SDK writes a `BasicProperties`. There is no official CloudEvents binding for 0-9-1 either.

So the RabbitMQ module defines that mapping, and it is fixed here rather than per implementer, modelled on the AMQP
1.0 binding so it is recognisable. Each CloudEvent attribute becomes an entry in `BasicProperties.headers` under its
name prefixed with `cloudEvents_`, which is the separator the AMQP binding recommends and the one that avoids the
colon form's awkwardness in a header name. `datacontenttype` becomes `BasicProperties.contentType`. The event data is
the message body unchanged. One class owns this mapping in both directions, so the bridge that reads it and the sink
that writes it cannot drift apart.

Decision 8 covers the binding mode itself.

At the domain level the extensions come from wherever the caller got them, and the API says so rather than pretending
otherwise. `publish(EventMetadata, E)` converts the domain event and then stamps the supplied metadata onto the
resulting CloudEvent before handing it to the `CloudEventSink`, which is a second place extensions are written and is
called out here so no implementer has to guess. Everything `EventMetadata` holds is stamped, not only the four named
above, since `EventMetadata.from` reads every extension off the event and dropping the rest would mean the metadata
does not survive the round trip. Where the converter already set an extension the supplied metadata wins, because the
caller reading it off a stored event is the one with the store's answer.

`publish(E)` writes whatever the `CloudEventConverter` produced. For an event that has never been through the event
store that is no stream identity at all, because a stream version and a position are properties of a stored event and
Occurrent cannot derive them. A consumer of such a message sees an empty `EventMetadata`, and a projection keyed by
metadata refuses it at delivery.

### 5. A binding derived from a filter narrows what arrives, and never decides what is handled

`destinationsFor(SubscriptionFilter)` exists so a consumer can bind only the routing keys it wants, or subscribe to
only the topics it wants, instead of taking everything and discarding most of it.

It works for the event-type part of a filter and for nothing else, because the event type is the only part of a filter
the destination mapping knows about. A stream id, a data field, a time range and a DCB criteria are all invisible to
the broker.

Where that final decision is made differs between the two levels, and the difference is stated here because it is not
obvious and getting it wrong delivers events a filter excluded.

**At the CloudEvent level the bridge does not evaluate a filter of its own.** `RegisteringSubscribable.subscribe`
already builds the predicate from the filter it was given and `route` applies it before calling the handler, so a
bridge that filtered as well would need a second copy of the filter, configured separately from the subscription's.
Two copies that nothing compares is the problem this ADR opened by describing.

**At the domain level the bridge is the only thing that can decide, so it applies the filter itself.**
`DomainEventFeed.accept(metadata, event)` hands the event to the registered projection unconditionally, and the
`Filter` given to `register` reaches `CatchupProjectionFeed` as the replay filter, so it selects what the catch-up
reads out of the event store and has no effect on the live path. A domain bridge that trusted a coarse binding would
therefore deliver events the subscription excluded. This is not the duplicate filter refused above, because at this
level there is no other evaluation to disagree with.

**Giving the bridge its own filter would lose events, so the feed does the matching and the domain bridge does not
ship until it can.** A bridge filter narrower than the one the registration was made with makes the bridge treat an
event as not matching and acknowledge it, while the projection's replay contract says that event was one of its own.
That is a loss rather than two rules disagreeing, so `AGENTS.md` settles it and no judgement about how likely the
misconfiguration is comes into it.

It cannot be closed from inside a broker module. `RegisteringSubscribable` keeps only the derived
`Predicate<CloudEvent>` and not the `SubscriptionFilter` it was built from, `DomainEventFeed` exposes neither its
filter nor a live match, and `ProjectionAnnotationRegistrar` derives the filter and calls `register` itself for
`@Projection(source = PUSH)`, which decision 9 relies on continuing to work. Making the bridge own registration would
break that annotation path, so this ADR does not do it.

So [#848](https://github.com/johanhaleby/occurrent/issues/848) is a **prerequisite** for the domain bridge rather than
an improvement to it, on the same footing as the outcome decision 1 needs.

**What that gives the feed is the same shape `routeReportingMatch` already has one level up.** The bridge rebuilds a
CloudEvent from the message and hands it over, and the feed matches, decodes with its own converter, delivers, and
reports which outcome happened. The bridge acknowledges on that outcome exactly as the CloudEvent bridge
does, and the two levels become one mechanism described twice rather than two designs. (`DomainEventFeed.acceptCloudEvent`
never actually returns `NOT_DELIVERABLE` in what shipped: only `FILTERED`, `DELIVERED` and `DEFERRED`, or a thrown
exception for an unregistered feed or a permanently failed catch-up. See changelog.md's own correction and #893.)

Handing over a CloudEvent rather than a domain event is what makes the match answerable at all, since a decoded domain
event plus an `EventMetadata` of extensions cannot answer a condition on subject, source, time or raw data. Decision 8
is what makes the rebuild cheap, because binary mode puts every attribute in the headers, so nothing is decoded to
find out whether it matches and a non-matching event is never decoded at all.

**Everything about building that matcher belongs to the feed, not to the bridge**, since the feed is what owns the
filter now. That covers three things the bridge would otherwise have had to hold. `register` takes a plain `Filter`
while `matcherFor` takes a `SubscriptionFilter`, so the wrapping happens there. A `DcbSubscriptionFilter` holds a
`DcbCriteria` rather than a `Filter`, so it cannot drive this path and is refused, which ADR 62 already does for a
catch-up replay on the same grounds. And a filter with a payload condition needs a `DataFieldReader`, because the
one-argument `matcherFor` builds with `DataFieldReader.refusing()` and throws while constructing the matcher, so the
reader is supplied to the feed alongside the filter and a payload filter without one is refused at startup, which is
what the subscribe path already does.

**Bindings default to `catchAllDestination()`, and narrowing them is something an application asks for.** At the
CloudEvent level a bridge has no way to read the filter its subscription was registered with. `ProjectionRunner`
derives that filter internally for `@Projection(source = PUSH)`, `PushSubscriptionModel` exposes neither it nor its
matcher, and `PushObserver` reports per-event outcomes rather than the filter behind them. So a bridge that claimed to
derive bindings from the subscription's filter would be describing something it cannot reach.

Taking the catch-all as the default is the right answer rather than a concession. Bindings are a topology decision and
the filter is a delivery decision, and this ADR only ever promised that the first narrows and the second decides.
Binding everything narrows nothing, which is always safe. An application that wants the narrowing hands the bridge a
filter for that purpose, and then `destinationsFor` derives the bindings from it.

**A binding filter supplied that way has to be at least as inclusive as the subscription's**, since a narrower one
stops events reaching a matcher that would have accepted them, and that is a loss rather than a tuning mistake.
Nothing checks it, which is the same limitation as at the domain level and has the same fix behind it.

A filter whose type part cannot be derived returns an empty `Optional`, and the consumer then binds
`catchAllDestination()` too. Guessing narrower would drop an event the filter would have matched, so the imprecise
answer has to be the inclusive one.

A CloudEvent bridge that wants to see what arrived and whether it matched reads its `PushObserver`, which decision 1
already requires it to have.

**The catch-all is a third method on the resolver rather than something the bridge works out**, because an empty
`Optional` says only that the filter could not be narrowed and says nothing about where to listen instead. RabbitMQ
needs the exchange to put a `#` binding on, Kafka needs a pattern narrow enough to avoid consuming unrelated topics,
and a custom resolver knows both while the bridge cannot ask it by downcasting. Since the resolver is already the one
place that knows this deployment's topology, it is also the only honest place to answer this. A resolver that routes
everything through one exchange returns that exchange with `#`, and one that spreads events over a topic per context
returns the pattern covering them.

A deployment whose platform team owns the topology declares its queues and bindings itself and ignores this method
entirely, which #415 already says has to stay possible.

### 6. Three modules, named after the existing convention

| Directory | Artifact | Contents |
|---|---|---|
| `broker/api/blocking` | `occurrent-broker-api-blocking` | `EventDestination`, `DestinationResolver`, `CloudEventSink`, `DomainEventSink`, `CloudEventForwarder`, `DomainEventForwarder`, the shared settings |
| `broker/rabbitmq/blocking` | `occurrent-broker-rabbitmq-blocking` | `RabbitMqDestination`, resolver, sinks, bridges, both levels |
| `broker/kafka/blocking` | `occurrent-broker-kafka-blocking` | `KafkaDestination`, resolver, sinks, bridges, both levels |

`occurrent-subscription-api-blocking` is the template for the naming, and `broker/api/blocking` mirrors
`subscription/api/blocking` in layout as well as in name.

### 7. Cross-transport conventions

The RabbitMQ and Kafka modules are built partly in parallel from this ADR, so the shape they share is fixed here
rather than settled twice. Where a rule below is broken by a transport, the transport says why at the declaration.

**Headers are `Map<String, String>`, never null, empty when unset.** The record copies what it is given and returns it
unmodifiable. `String` values are what both transports convert from without the caller having to know which one they
are on, since RabbitMQ AMQP headers are `Map<String, Object>` and Kafka headers are `byte[]`. Kafka encodes as UTF-8.
CloudEvent attributes are strings in both bindings anyway, so a richer value type would only be usable for application
headers and would then differ per transport.

**An application header may not use the prefix the binding reserves, and a colliding key is refused when the
destination is built.** Decision 8 writes every CloudEvent attribute into the same header namespace, so an application
header using that prefix could overwrite `streamid` and break decision 4's invariant without anything failing.
Refusing at construction makes that a startup error in the code that named the header rather than a wrong value on a
consumer much later. Kafka takes the prefix from `cloudevents-kafka`, and RabbitMQ takes it from the `cloudEvents_`
mapping decision 4 defines for it, since there is no SDK constant to take it from there.

A prefix is not the whole reservation on either transport. The Kafka binary binding also puts `datacontenttype` in an
unprefixed `content-type` header, so an application header of that name would silently change a message's media type
while passing a prefix check, and `content-type` is reserved there alongside the `ce_` prefix. RabbitMQ has the same
attribute in `BasicProperties.contentType`, which is a field rather than a header entry and so cannot be collided with
in the first place.

**The publish acknowledgement setting is named the same on both sinks, and it is a timeout rather than a switch.**
RabbitMQ calls the mechanism publisher confirms and Kafka calls it acks plus waiting on the send future, and neither
name is usable on the other transport. Both builders take `acknowledgementTimeout(Duration)`, defaulting to 5 seconds,
and the transport javadoc names the underlying mechanism.

There is deliberately no `waitForAcknowledgement(false)`. A publish that returns before the broker accepted the
message reports success for an event the broker may never have received, so turning the wait off is a documented loss
window, and `AGENTS.md` says a loss window that is narrow, documented and warn-logged is still a loss. Offering the
switch would put that choice in the API and then have to defend it. An application that genuinely wants to publish
without waiting implements `CloudEventSink`, the same way out as structured mode in decision 8.

**An expired acknowledgement timeout throws.** This is the branch the setting most needs decided, because the message
may or may not have reached the broker and returning normally would report a success nobody established. Throwing
hands the caller a decision it can act on, and the caller republishing after a timeout may produce a duplicate, which
is the same at-least-once contract every consumer here already works under. The `RetryStrategy` does not cover this
case, which is why the two are described separately below.

**A publisher confirm says the broker took the message, not that it routed it, so the RabbitMQ sink publishes with
`mandatory = true` and treats a returned message as a failure.** RabbitMQ confirms a publish to an exchange that has
no binding matching the routing key and then discards the message, so a sink that reads the confirm alone reports
success for an event nobody will ever receive. A typo in a routing key would look exactly like a working deployment.
So the sink sets `mandatory`, correlates any `basic.return` with the publish it belongs to, and treats a return as a
failed publish even though a confirm follows it. This applies to every publish the RabbitMQ side makes, the ordinary
forwarded event and the parked one alike, and for the parked one it is what stops the original being acknowledged
after a park that went nowhere.

**Waiting is only worth anything if the producer is configured to make the broker answer, so the Kafka sink requires
`acks=all` and refuses to start below it.** Under `acks=0` a send future completes once the record reaches the socket
buffer, and Kafka promises nothing about the broker having it, so the sink would wait, succeed, and let
`CloudEventForwarder` advance its checkpoint past an event no broker ever stored. That is the timeout doing the
opposite of its job. Refusing at startup is the only place this can be caught, since afterwards the failure looks
exactly like success. RabbitMQ needs no equivalent, because publisher confirms have no setting that weakens them this
way.

**A destination is a record with static factories, a sink is a builder.** A destination has three components and no
optional wiring, so it gets a canonical constructor, an `of(...)` factory for the common case, and a
`withHeaders(Map<String, String>)` copy method. Both transports use the same factory name for the same idea.

A sink takes a client connection, a resolver, an acknowledgement timeout and a `RetryStrategy`, so it gets
a builder reached through `RabbitMqCloudEventSink.builder(...)` and `KafkaCloudEventSink.builder(...)`. Every builder
method that is not transport-specific has the same name on both.

**A sink accepts a `RetryStrategy` and its default applies one.** A broker is an external store under the rule in
`AGENTS.md`, with `NativeMongoCheckpointStorage` as the template, so the default is exponential backoff from 100 ms up
to 2 seconds and the builder takes `retryStrategy(RetryStrategy)`. The retry guards a publish against a transient
failure. It does not replace the acknowledgement wait, because a publish that was never acknowledged is not known to
have failed.

**A domain sink is built from a CloudEvent sink and a converter, through the same factory name on both transports.**
`RabbitMqDomainEventSink.using(cloudEventSink, cloudEventConverter)` and its Kafka twin.

**What a bridge does after a failed `accept(...)` is fixed in one respect and configurable in another.** The fixed
part is that it never plainly acknowledges the message, because that is the loss ADR 104 named. Everything else is the
application's to configure, and #415 already requires it, since always requeueing keeps a message that fails every
time in a redelivery loop forever and takes the operator's own policy away from them.

So each bridge takes a `DeliveryFailurePolicy`, a shared enum in `occurrent-broker-api-blocking` rather than two
transport enums that happen to line up, with two constants. `REDELIVER` puts the message back for another attempt.
`PARK` routes it to a holding destination nobody consumes from, so an operator can look at what failed. Both bridges
take it through a builder method named `onDeliveryFailure(DeliveryFailurePolicy)`, and `REDELIVER` is the default
because it is the choice that cannot lose a message on a transient failure.

`PARK` also needs somewhere to park, so choosing it requires a parking destination of that transport's own
`EventDestination` type, a `RabbitMqDestination` or a `KafkaDestination`, given through
`parkingDestination(D)` on the same builder. A bridge configured with `PARK` and no parking destination refuses to
start. Without that the two modules would each invent a default, and a default holding destination is precisely the
thing an operator has to know the name of.

`REDELIVER` is where the two transports genuinely differ in mechanism, and it is written down here so the Kafka one
does not invent a third behaviour. RabbitMQ has a per-message negative acknowledgement, so it calls `basicNack` with
requeue and the broker redelivers. Kafka has none, so it declines to commit the offset and seeks back to it.

**Seeking only works if nothing else commits, so the Kafka bridge requires `enable.auto.commit=false` and refuses to
start otherwise.** Auto-commit is Kafka's default, and it advances the offset on a timer regardless of what the bridge
decided, so a bridge that seeks back while auto-commit is on will still commit past the failed record. This is the
consume-side twin of the `acks=all` rule above, and it fails at startup for the same reason, that afterwards the
mistake is invisible.

**After a seek the bridge stops processing that partition's remaining polled records.** A poll returns a batch, so
continuing through it after a failure means committing a higher offset for the same partition and skipping the record
that failed, which is the same loss by a slower route. Other partitions in the same poll are unaffected, since their
offsets are independent.

**The bridge commits an explicit offset per partition and never the no-argument form.** `poll()` advances the
consumer's own position across the whole returned batch, so `commitSync()` with no arguments after the first
successful `accept(...)` commits every record the poll returned, including ones nothing has processed yet. The bridge
therefore commits `record.offset() + 1` for that record's partition, and only once that record has succeeded. Turning
auto-commit off does not cover this on its own, which is why both are written down.

**`PARK` is a publish the bridge does itself, waits for, and only then acknowledges the original.** It is the same
sequence on both transports, which is worth stating because the shortcut differs on each and both shortcuts lose
messages.

Handing the message to RabbitMQ's own dead-lettering is the shortcut there, and it is not enough. Dead-lettering is
the broker feature where a rejected message goes to a dead-letter exchange named on the queue, which then routes it
onward. `basicNack(requeue = false)` throws the message away outright when no such exchange is configured, and even
with one the broker republishes the message without publisher confirms, so an unavailable or unbound target can still
drop it. So the bridge publishes to the parking exchange itself, with confirms, and acknowledges the original
only once that confirm arrives. Kafka's shortcut is committing the offset before the parking record is acknowledged,
which loses the original the same way, so it waits for that acknowledgement first too.

In both cases a failed parking publish leaves the original unacknowledged, which means it is redelivered rather than
lost. That is the rule the whole ADR runs on, that nothing is acknowledged until the event is somewhere it can be read
from again. What neither bridge may do is acknowledge or commit after logging the failure.

**The Kafka resolver uses the stream id as the message key by default, and a null key when there is no stream id.**
Kafka only orders within a partition, and a projection reading one stream needs that stream's events in order, so
keying by stream id puts them on one partition. An event published through `publish(E)` has no stream identity at all,
which decision 4 explains, so the default resolver reads the extension rather than demanding it and leaves the key
null when it is absent. `KafkaDestination` declares the key nullable for exactly this, and the alternative would be
the metadata-free overload throwing on a resolver the caller never chose.
An application that wants a different partitioning replaces the resolver, which is one of the two things a resolver is
for.

### 8. Binary content mode, and structured mode is not offered

The sinks write messages in the CloudEvents binary content mode. The event data is the message body and every
CloudEvent attribute, the four Occurrent extensions included, is a message header.

Binary mode is what makes decision 4's invariant the natural outcome instead of extra work. Structured mode puts the
whole CloudEvent in the body as JSON, so honouring the header invariant on top of it would mean writing the same four
values twice, in the body and in the headers, and accepting that a consumer reading one of the two copies may read a
different answer than a consumer reading the other. Binary mode also lets a broker route on the event type without
opening the body, which is what decision 5's bindings depend on.

`AGENTS.md` is what settles this, rather than a preference for the smaller API. It allows the easier solution when it
yields roughly the same result and refuses it when the difference is isolation or correctness, and two copies of the
same four values that can disagree is a correctness difference.

Structured mode is therefore not offered, rather than offered and discouraged. An application that needs it for a
consumer outside Occurrent implements `CloudEventSink`, which is what that interface is for. Refusing it now also
takes nothing back later, because adding a structured writer changes neither `EventDestination`, nor
`DestinationResolver`, nor either sink interface.

### 9. Spring Boot auto-configuration is #846, and it comes after both consume bridges

No `occurrent-spring-boot-starter-broker-*` module is built as part of this work.
[#846](https://github.com/johanhaleby/occurrent/issues/846) builds it, in the same 0.34.0 milestone but after both
transport modules and both consume bridges exist.

Auto-configuration fixes property names and bean names, and doing that before anyone has wired these three interface
families by hand fixes them against a guess. The manual wiring is also small already, since `@Projection(source =
PUSH)` resolves a push feed bean without any broker knowledge, so what an application declares by hand is a sink bean
and a bridge bean per consumer.

The `@RabbitSaga` and `@KafkaSaga` ideas on #415 and #417 are not part of #846 either. They are the shape a later
Spring Boot layer may take, and putting them into the first auto-configuration would decide that layer before either
transport module has been used by anyone.

### 10. The RabbitMQ publishing module ships as a module, not as a documented example

#414 asks whether the RabbitMQ publishing side earns a published artifact or whether it is a documented example, and
defers the answer to what the code looks like once written. The position taken here is that it ships as a module, and
the position is provisional by the issue's own terms.

Two things decide it. The publisher confirms handling and the CloudEvents binding are the parts an application gets
subtly wrong, and both are code rather than a snippet. And an example in the documentation has no test and no
released version behind it, so it drifts from the library with nothing failing when it does.

The closing review of this epic checks how much of the module is left once the destination record and the resolver are
taken out. If the answer is a thin wrapper over the transport client, the ruling is worth taking again on that
evidence, which is what #414 asked for.

## Consequences

An application stops writing the publish and consume ends of a broker integration and declares them instead. What it
declares is a resolver, a sink bean per publisher and a bridge bean per consumer, plus one queue or consumer group per
projection or saga because ADR 90 requires it.

An application that already has its own publisher wrapper replaces `CloudEventSink` or `DomainEventSink` and keeps
everything else, including the routing, since routing is behind the sink.

Occurrent stays free of a broker dependency where it matters. `occurrent-broker-api-blocking` has no broker client on
its classpath, and the two transport modules are additive artifacts that nothing else depends on.

Delivery guarantees are unchanged from ADR 62. Steady-state delivery is at-least-once, so a projection that receives
the same event twice has to reach the same state as one that received it once. Ordering follows the transport, which
is why decision 7 keys Kafka messages by stream id rather than leaving partitioning to chance.

The reactor variants repeat less than the module names suggest. The sinks and the bridges are stack-typed, because
`PushSubscriptionModel` and `DomainEventFeed` already exist once per stack and a blocking sink returns `void` where a
reactor one returns a `Mono`. `EventDestination` and `DestinationResolver` are not, since they name only `CloudEvent`
and `SubscriptionFilter`, and `SubscriptionFilter` already lives in the stack-neutral `occurrent-subscription-core`. So
putting them in a `-blocking` artifact is a cost this decision accepts for one artifact per broker today, and #418
decides whether they move to a stack-neutral module when the reactor variants arrive.

## Amendment (2026-08-18): a domain-level payload filter is refused on first live-match use, not at register(), and the refusal is permanent

Decision 5 says a payload filter without a `DataFieldReader` "is refused at startup, which is what the subscribe path
already does." [#848](https://github.com/johanhaleby/occurrent/issues/848) implementing the domain-level half of that
found "startup" cannot mean `DomainEventFeed.register(..)` without breaking released code. That overload shipped in
0.33.0 with no `DataFieldReader` dependency at all, since a payload condition on the replay filter has always been
evaluated by the event store during the replay itself. Refusing it inside `register` would newly fail an existing
caller that has never called `acceptCloudEvent`.

The refusal happens on `acceptCloudEvent`'s first call instead, the first moment a filter is actually asked to answer
live rather than during replay, and it is permanent for that registration from then on. The first call builds and
caches an `UnreadableLiveFilterException`, and every later call throws that same instance without rebuilding the
matcher, so a caller cannot retry its way past a configuration error that cannot change without a new registration.

## Amendment (2026-08-19): a rebuilt CloudEvent restores the type of the extensions Occurrent itself owns, not every extension

Decision 4 and decision 7 say a CloudEvent attribute is a string-valued AMQP header, and give the reason. Kafka's
binary binding writes headers as strings too, so a richer type would only work on one transport and the two would
end up disagreeing about which events a filter matches. `RabbitMqCloudEventMapper.toCloudEvent` followed that to the
letter and rebuilt every attribute and extension, without exception, as a `java.lang.String`.

That took the string-only rule further than decision 7 actually asks for, and the consequence went undocumented.
`ConditionMatcher.valuesEqual` compares two `Number`s by value, but falls back to `Objects.equals` for anything else,
and its own comment names this exact case. A rebuilt `streamversion` header comes back as the string `"3"`, so
`Filter.streamVersion(eq(3))`, whose operand is a `Long`, evaluates through `Objects.equals("3", 3L)` instead of by
value, and never matches. On the broker consume bridges (#415), a live match that should have succeeded instead
reports `RoutingOutcome.FILTERED`, and both bridges acknowledge a `FILTERED` delivery. An event a filter should have
accepted is acknowledged and discarded rather than delivered. That is silent event loss, not a cosmetic type wobble.

`toCloudEvent` now restores `streamversion` and `position` as the `Long` `OccurrentCloudEventExtension` itself
defines them as, since Occurrent owns those two extension names and knows their types. `streamid` and `appendid`
stay `String`, since that is what they already are. Any other extension, one an application defined, stays a
`String` too, since this mapper has no way to know what type it should be.

The wire format itself is unchanged. Headers are still `Map<String, String>`, exactly as decision 7 requires, and
RabbitMQ still cannot preserve a type Kafka's binary binding would flatten to a string anyway. Only the read side of
the mapping changed. `toCloudEvent` was discarding type information Occurrent already has, the two extension names
it defines as numbers, for no reason connected to the wire format at all.

## Amendment (2026-08-19): a rebuilt CloudEvent tells data present-but-empty apart from no data at all

`toBody` and `toCloudEvent` conflated two different states into the same zero-length AMQP body. `withData(new
byte[0])`, data present but explicitly empty, and no `withData(...)` call at all, `getData()` returning `null`,
both write as an empty body, and a body has no way to be absent the way `data` itself can. `toCloudEvent` always
rebuilt a zero-length body as `null`, so a round trip silently turned present-but-empty data into no data at all. A
handler or a payload filter can read the two differently, so this was a real corruption, narrow but real, not just
a type wobble like the extension one above.

`toBasicProperties` now writes a `cloudEvents_data_present_empty` header when `data` is present but empty, and
`toCloudEvent` reads it back to rebuild that case correctly instead of defaulting to `null`. The name contains an
underscore, which a real CloudEvent attribute or extension name can never contain under the spec's own naming
rules, so it can never collide with one and needs no reserved-prefix check beyond `cloudEvents_` itself, which
`RabbitMqDestination` already refuses an application header from using.

This is a wire format change, but a safe one to make here. Neither `toBasicProperties` nor `toCloudEvent` had
shipped in a release before this PR, so there is no already-deployed message this changes the meaning of. Decision
7's cross-transport symmetry rule still holds. Kafka can do the same thing, and more directly. A Kafka record value
is natively either `null` or a zero-length `byte[]`, two states the wire itself already tells apart, so
`cloudevents-kafka`'s binary writer does not need a marker header the way this mapping does to make up for AMQP's
body having no way to be absent at all.

## Amendment (2026-08-20): the default Kafka resolver is a shared topic, not one topic per type

Decision 7's keying paragraph says a projection reading one stream needs that stream's events in order, and that
stream-id keying delivers it, but it never names which topic or topics the keyed messages land on. The module that
implemented decision 7 filled that gap with one topic per cloud event type, a choice #416's issue and the epic plan
that built this module made, not something this ADR ever decided. That combination defeats decision 7's own stated
purpose for the case an event-sourced stream mixing event types is built around. Two events of the same stream but
different types are keyed identically, yet a topic-per-type resolver sends them to different topics entirely, so
they were never on the same partition to begin with regardless of the key. `KafkaTopicPerTypeDestinationResolver`'s
own javadoc was corrected earlier on this PR to state that narrower guarantee honestly, ordering within one stream
and type pair rather than within a whole stream.

A narrower documented guarantee is not the same thing as the right default. `AGENTS.md`'s isolation rule already
makes the same point for a different mechanism, that a loss window narrow, documented and warn-logged is still a
loss, and that a change narrowing one is a step on a recorded path to closing it, never the accepted end state. An
application built against decision 7's own stated purpose, one stream in order, silently gets a weaker guarantee
for a stream that is not single-typed, discoverable only by reading a resolver's javadoc rather than by anything
failing. Making that honest was the right fix to land immediately, but it was never the fix for the topology
choice underneath it.

The two topologies are also asymmetric in how expensive it is to change your mind later. Shipping a shared topic as
the default now, with topic-per-type kept as a documented opt-in, is additive. A deployment that wants per-type
topics for retention or independent consumer scaling still gets them, by choosing that resolver explicitly.
Shipping topic-per-type as the default and correcting it later is a breaking behavioural change for any deployment
that has already created per-type topics and pointed consumers at them, since undoing it is a topology migration,
not a code change on top of the same data.

I asked for whatever is best long-term according to the principles of `AGENTS.md`, and the derivation above is that
answer. `KafkaSharedTopicDestinationResolver` is the new shipped default.
`KafkaCloudEventSink` and `KafkaDomainEventSink`'s own documentation leads with it. It publishes every event to
one topic given to its constructor, no default name invented, the same reasoning this decision already gives for
refusing a parking bridge with no `parkingDestination` of its own, that a default destination name is precisely
the thing an operator has to know. It keys by stream id when present and `null` otherwise, unchanged from what
decision 7 already specifies. That guarantee also assumes the topic's partition count is chosen before producing
to it and stays there. Kafka hashes a key against the topic's current partition count, so growing that count
later remaps an existing stream id onto a different partition and can silently break ordering for whatever
streams are still in flight at that moment, a concrete operational rule rather than a caveat to hedge with. Its
`destinationsFor` returns that one topic regardless of the filter it is asked to narrow, since with a single
topic narrowing has nothing left to do and decision 5's rule that the feed remains the decider already covers the
rest. `KafkaTopicPerTypeDestinationResolver` stays exactly as it is, unchanged in behaviour, the documented
alternative for a deployment that wants per-type topics and either has single-type streams or accepts the
narrower guarantee.

## Amendment (2026-08-21): readiness gate for catch-up subscriptions

Decision 1 says `CatchupThenPushSubscriptionModel` keeps the shape it has, and that `PushObserver` and the outcome
`routeReportingMatch` reports to it are the one exception. That was accurate for the single acknowledgement decision
a CloudEvent-level bridge had, whether the routing evaluation reported `DELIVERED`. It stopped being accurate once a
bridge feeds a `PushSubscriptionModel` that a `CatchupThenPushSubscriptionModel` wraps.

The wrapper registers on the live model before its own replay finishes, so a message the bridge pulls off the broker
during that window reaches `routeReportingMatch`, gets reported `DELIVERED`, and is only buffered by the wrapper's
`BlockingHandover`, not yet applied to the projection. `RoutingOutcome` alone cannot tell the two apart, because the
bridge holds only the live `PushSubscriptionModel`, never the wrapper, unchanged from decision 1. A crash before the
buffer drains loses that event for good, and it is not recoverable from the local event store, since a consume
bridge exists precisely to receive an event another service published, one this store never had a copy of to replay
back. The safety constraint this amendment adds is that a bridge must never acknowledge or commit a message for a
subscription still in catch-up replay, because the broker's own copy is the only copy of that message anywhere.

`CatchupThenPushSubscriptionModel` gains `isReadyForLiveDelivery(String)`, delegating to the specific subscription's
`BlockingHandover`, the component that actually owns the buffer, true only once that subscription's catch-up has
reached live. `RabbitMqCloudEventBridge` and `KafkaCloudEventBridge` each gain an optional
`readinessSource(Predicate<String>)` on their builders, `true` for every id by default so a bridge fed a bare
`PushSubscriptionModel` with no catch-up wrapper is unaffected, and AND it into the same coarse consume/fetch poll
decision 1 already describes for the running and paused check. `@Projection(source = PUSH)` and
`@Saga(source = PUSH)` publish the `CatchupThenPushSubscriptionModel` each builds internally as a Spring bean named
`"catchupThenPushSubscriptionModel-" + id`, since that object was otherwise reachable only from inside the registrar
that built it, and application wiring needs a handle on it to pass to `readinessSource`.

Decision 1's "keep the shapes they have" now reads with a second narrow exception beside `PushObserver`.
`CatchupThenPushSubscriptionModel` gains `isReadyForLiveDelivery(String)` and, on the Spring stack, becomes
independently reachable as a named bean rather than staying private to the registrar that builds it. Both are
additive. Nothing about `subscribe`, the catch-up-then-live handover, or the rest of the class's existing contract
changed, and a caller that never touches `readinessSource` sees identical behavior to before this amendment.

This closes the gap for the blocking stack's CloudEvent-level bridges only. The reactor stack has no equivalent yet.
`ReactiveHandover` has no `isReadyForLiveDelivery()`, the reactor `DomainEventFeed` never received the domain-level
version of this fix either, and no reactor broker bridge exists yet to need one. A design that has the bridge hold
the `CatchupThenPushSubscriptionModel` itself, giving it a `Pushable` shape so readiness reaches the per-message
`RoutingOutcome` the way `DomainEventFeed.acceptCloudEvent` already does, was considered and rejected for this
change, since it would falsify decision 1's "not itself a push target" rationale outright rather than adding a
narrow exception beside it. That alternative is tracked as
[#885](https://github.com/johanhaleby/occurrent/issues/885), pending its own decision on whether to amend decision 1
further.

## Amendment (2026-08-21): one evaluation replaces the readiness gate for catch-up subscriptions

The readiness-gate amendment above turned out to be unsound. It closed the acknowledgement gap by narrowing a race
rather than removing it. A `cancelSubscription` immediately followed by a `subscribe` under the same id attaches a
fresh registration to a still-replaying `CatchupThenPushSubscriptionModel` before the coarse readiness poll or
`isRunning` check next runs, and a message the bridge pulls off the broker in that window still gets reported
`DELIVERED` and buffered rather than applied, the exact loss this amendment set out to prevent. A fresh-context
adversarial verify falsified it with a deterministic test reproducing that interleaving.

This amendment replaces the mechanism, not the constraint. Decision 1's conclusion stands: one evaluation, shared
between the match and the report, is still what a caller relies on to acknowledge safely, and
`CatchupThenPushSubscriptionModel` is still not itself a push target, with every constructor and `subscribe`
unchanged. `PushObserver`/`RoutingOutcome` remain one exception decision 1 already names, and the readiness
accessor and bean publication the amendment above added remain the other, both still additive rather than
touching the class's existing contract. Decision 1's own prose above, describing a three-valued outcome reported
before the matched action ran, describes the mechanism this amendment replaces. The outcome is six-valued as of the
2026-08-22 amendment below. Read it as history, not as what
ships. What changes is how `routeReportingMatch` decides what to report.

`RegisteringSubscribable.routeReportingMatch` used to report `RoutingOutcome.DELIVERED` before the matched
registration's action ran at all, then dispatch. For a direct handler that was harmless. The handler either ran
or its exception propagated, and the outcome already reported did not depend on which. For a registration backed
by a catch-up-then-live buffer it was not harmless, because "the action ran" and "the action's target actually has
the event" are different facts, and only the second one is what an acknowledging caller needs.

`routeReportingMatch` now reports after the action runs, from what the action itself returns, from the one
evaluation that already decides the match. The registered action, a new `RegisteringSubscribable.RoutingAction`, takes a
`bufferIfNotLive` flag and returns whether the event genuinely landed. `RoutingOutcome` gains a fourth value,
`DEFERRED`. It marks an event that reached a registration whose target cannot accept it yet, for a reason expected
to resolve on its own, and it is safe to redeliver arbitrarily many times.

`BlockingHandover` gains `acceptIfLive(T)` beside the existing `accept(T)`/`acceptReportingDelivery(T)`. Where those
buffer a payload offered while not live, `acceptIfLive` refuses it outright, reporting `false` without ever
touching the buffer. `PushSubscriptionModel.accept(CloudEvent)`, the write path an in-memory store listener or
another in-process caller uses, is unchanged and keeps buffering, since a write-path event has nowhere else to come
from and refusing it would lose it rather than protect it. A new `PushSubscriptionModel.acceptRedeliverable(CloudEvent)`
is for a caller that can redeliver, a broker bridge, and routes to `acceptIfLive` instead. It refuses rather than
buffers, reports `DEFERRED`, and lets the caller ask again. `CatchupThenPushSubscriptionModel`'s own public surface,
every constructor and `subscribe`, does not change at all. Only which method its internal registration calls on the
underlying handover does, chosen by which public entry point the caller used.

`RabbitMqCloudEventBridge` and `KafkaCloudEventBridge`, and their domain-event equivalents, route
`RoutingOutcome.DEFERRED` around `DeliveryFailurePolicy` entirely rather than through it. RabbitMQ negatively
acknowledges with requeue, Kafka seeks back reusing the same per-partition throttle the earlier readiness gate
already relied on, and neither ever parks. Nothing here is broken or wrong, so parking it would be the exact
avoidable dead-lettering `DEFERRED` exists to rule out.

`isReadyForLiveDelivery(String)` and `readinessSource(Predicate<String>)` stay, but nothing about correctness
depends on them any more. `DEFERRED` is what keeps a bridge correct with no `readinessSource` configured at all,
refusing and redelivering safely until catch-up finishes, and `readinessSource` only cuts down on how often that
refuse-and-redeliver round trip happens. The RabbitMQ and Kafka Spring Boot starters now wire it
automatically, deferring to whichever `CatchupThenPushSubscriptionModel` bean, if any, owns a bridge's subscription
id, so a zero-config Spring application gets the quiet path during a replay without naming `readinessSource` itself.

At the time this amendment was first written, it closed the gap for the blocking stack only, CloudEvent-level and
domain-level alike. The reactor stack carried the identical shape of defect in its own
`RegisteringSubscribable.routeReportingMatch` and `DomainEventFeed.acceptCloudEvent`, worse in one respect: the
reactor `DomainEventFeed` had never received even the readiness-gate half-fix this amendment replaces, so it
reported `DELIVERED` for a buffered-not-applied event unconditionally. No reactor broker bridge existed yet to need
the `DeliveryFailurePolicy` bypass half of this fix, but the correctness half did not depend on one existing, and
was tracked as a follow-up in this same epic. See below: that follow-up has since landed.

Closes [#885](https://github.com/johanhaleby/occurrent/issues/885) as rejected. The wrapper-as-push-target design
that issue proposed, and this amendment's own predecessor line of investigation considered again under a different
name, is rejected a second time, for a different reason than decision 1's original one. `CatchupThenPushSubscriptionModel`'s
public constructors shipped in 0.33.0, so giving it a `Pushable` shape would require changing released API, not
merely revisiting a design choice. The fix that actually closes the gap needed no new push target at all.

The reactor follow-up promised above has landed. `RegisteringSubscribable.routeReportingMatch` on the reactor stack
now reports from a new `Mono<Boolean>`-returning `RoutingAction`, run before the report rather than after, and
`subscribe(...)` keeps its released `Function<CloudEvent, Mono<Void>>` signature by wrapping every event it delivers
as `DELIVERED`. `ReactiveHandover` gains `acceptIfLive(T)` beside `accept(T)`/`acceptReportingDelivery(T)`, refusing
a payload outright rather than buffering it while not live, behind a dedicated
`ReactiveHandover.PreDispatchRefusalException` mirroring the blocking one. `DomainEventFeed.acceptCloudEvent` and
`CatchupProjectionFeed` report `DEFERRED` instead of `NOT_DELIVERABLE` for an event that arrives before the
registered projection is live, through that same `acceptIfLive`, closing the gap this amendment left open for the
reactor `DomainEventFeed`.

## Amendment (2026-08-21): a permanent catch-up refusal stops a bridge, and a lifecycle `NOT_DELIVERABLE` is paced, not failed

A fixpoint review of the blocking bridges found two gaps this ADR's design left open, both on the same four bridges
(`RabbitMqCloudEventBridge`, `KafkaCloudEventBridge`, `RabbitMqDomainEventBridge`, `KafkaDomainEventBridge`).

**A permanently failed catch-up was routed through `DeliveryFailurePolicy` like an ordinary failure.**
`BlockingHandover.acceptIfLive`/`acceptReportingDelivery` throw `PreDispatchRefusalException`, unwrapped, once
`catchUpFailure` is set, and that field is never cleared. A bridge's generic `catch (RuntimeException | AssertionError)`
caught it indistinguishably from a handler that merely threw once, so under `DeliveryFailurePolicy.PARK` every later
message parked and acknowledged, forever, the exact loss decision 1's "never acknowledge a message nothing
consumed" rests on. Each bridge now catches `BlockingHandover.PreDispatchRefusalException` by type ahead of the
generic branch and treats it as permanent, the same shape `UnreadableLiveFilterException` already gets: log at
error once, stop consuming, and negatively acknowledge (RabbitMQ) or seek back leaving the offset uncommitted
(Kafka) rather than ever parking or committing into the same refusal. `BlockingHandover` is an internal type. A
bridge importing it for this one `catch` is judged acceptable, narrower than either matching on the exception's
message or treating every `NOT_DELIVERABLE`-shaped failure as potentially permanent.

**`NOT_DELIVERABLE` conflates a failure with a lifecycle state, and the coarse poll can hand a bridge one mid-batch.**
`routeReportingMatch` reports `NOT_DELIVERABLE` with no exception for a paused subscription or a model that is not
running, indistinguishable at the bridge from `NOT_DELIVERABLE` with no exception for... nothing else, in fact:
every other `NOT_DELIVERABLE` case comes with a thrown exception (the matcher failing, or the refusal above), so a
normal-return `NOT_DELIVERABLE` is *always* a lifecycle state today, never a failure. The coarse poll only notices a
`pauseSubscription`/`stop()` called from inside a handler up to one `pollInterval`/`pollTimeout` later, so a message
already queued behind that call can still reach the bridge before the poll cancels consumption. Routed through
`DeliveryFailurePolicy` as it was, `PARK` would park and acknowledge a message nothing is actually wrong with. Each
CloudEvent-level bridge bypasses `DeliveryFailurePolicy` the same way `DEFERRED` already does: Kafka seeks back and
throttles the partition, reusing the mechanism it already has. *(RabbitMQ's own fix here, a re-read plus an
immediate consumer cancel, is superseded by the amendment below. This sentence is kept only as the historical record
of what shipped first.)* The domain bridges need no equivalent change: `DomainEventFeed.acceptCloudEvent` has no
pause concept and never returns `NOT_DELIVERABLE` at all (see the amendment above).

*(Done by the 2026-08-22 amendment below, which adds `UNAVAILABLE` and `REFUSED`. The paragraph is kept as the
record of why it was deferred at the time.)* **The cleaner long-term shape is a distinct `RoutingOutcome` value for a lifecycle refusal**, the same reasoning
that gave `DEFERRED` its own value rather than overloading it onto `NOT_DELIVERABLE` in the first amendment above:
one value, one meaning, and a bridge that does not have to re-derive a state `routeReportingMatch` already decided
and then discarded. That was not done here. It is a wider, three-module change (a new public `RoutingOutcome`
constant, threaded through `RegisteringSubscribable`, `PushObserver`, and every existing caller that switches on the
enum) for a defect fully closed by a narrower, bridge-local re-read using API `RegisteringSubscribable` already
exposes publicly (`subscriptionIds()`, `isRunning(String)`). The narrower fix ships the correctness fix now without
reopening a settled outcome shape. Widening the enum is tracked as a follow-up rather than bundled in.

## Amendment (2026-08-22): the RabbitMQ lifecycle re-read above is itself removed, held tags carry their own generation, and a permanent stop closes the channel

A Copilot review of the fixpoint round above found the RabbitMQ-specific fix in the previous amendment traded one
race for another, plus two smaller defects on the same bridges. All four apply to `RabbitMqCloudEventBridge` and
`RabbitMqDomainEventBridge`. The third also touches both starters' `CatchupThenPushReadiness`.

**The lifecycle re-read raced `stopPermanently()` for the same delivery tag.** The previous amendment's RabbitMQ fix
re-read the model's running state after a `NOT_DELIVERABLE` report and, when it was the lifecycle case, cancelled
the consumer and negatively acknowledged immediately rather than waiting for the next poll. That immediate action
and a concurrent permanent stop (a `PreDispatchRefusalException` arriving on the very next delivery, say) could both
be deciding the same tag's fate at once, with no lock ordering between them. The fix widens the safe direction instead of trying to close that race.
A lifecycle `NOT_DELIVERABLE` is now paced exactly like `DEFERRED`, unconditionally, with no re-read at all. It costs the immediate-visibility guarantee the re-read bought (a paused
message can now sit unacknowledged for up to one `pollInterval` instead of being requeued at once), a cost already
paid, and accepted, everywhere `DEFERRED` itself applies. `isLifecycleNotDeliverable()` and the RabbitMQ-only
`cancelConsumingNow()` are removed as dead code.

**A held delivery tag's `channelGeneration` was checked once, at hold time, not again at release.** Both bridges
already tracked a generation counter, bumped on an automatic connection recovery or a consumer shutdown, to stop a
stale tag from being acknowledged after the channel that tag belonged to is gone. The check ran when a tag was
first captured, not when it was later acted on, so a bump landing between the two left a window where a stale tag
could still be acted on. Each held-tag deque now stores a `HeldDelivery(deliveryTag, generation)` record instead of
a bare `long`, and every release revalidates the tag's own generation against the current one immediately before
acting, dropping rather than redelivering a mismatch (the dead channel it belonged to already redelivered it by
itself). `channelGeneration`'s own bump, and every immediate acknowledgement, negative acknowledgement or park this
bridge issues, now take the same lock, so the bump can never land inside that window either.

**A permanent stop left an already-held tag stuck until an operator called `close()`.** `stopPermanently()` cancelled
the consumer and stopped the poll for good, but left the channel open with no way for a tag already held (from
before the stop) to ever be released again, the very poll that would have released it now gone. It now releases
every held tag, generation-safely, and then closes the consume channel itself, in that order, under the lock,
rather than waiting for `close()`. Closing the channel also requeues the triggering delivery for a permanent catch-up
refusal, RabbitMQ's own guarantee for a closed channel with an unacked delivery on it, so the explicit negative
acknowledgement the previous amendment described for that case is no longer needed either. `stopPermanently()`'s own
`catch` widens from `IOException` to `IOException | RuntimeException`, since `basicCancel` on an already-closed
channel throws the latter.

**`CatchupThenPushReadiness.memoized(...)`'s cache could lock in a wrong answer taken before the framework registrar
had published anything for this bridge's own live feed.** The registrar publishes its shared identity-registry bean
lazily, on its first registration, so a bridge's own poll can run before that bean exists at all, and separately,
before this specific live feed's own entry lands in it once it does. Either "not found yet" answer used to fall
through to an id-only scan across every wrapper bean in the context and, if that scan found an unrelated wrapper
sharing the subscription id (ADR 102 permits exactly that), cached it as if it were the true identity match, forever.
The fix separates the two. The id-scan fallback runs only when the registry bean is absent from the context
outright, and once the bean exists, only a positive identity match from it is ever cached. "Not (yet) wrapped"
re-resolves fresh, from one cheap map lookup, on every later poll instead.

**Parking and redelivering a genuine failure both logged twice.** A bridge's own generic failure branch logged the
cause at `warn` before routing to `DeliveryFailurePolicy`, and `RabbitMqDeliveryFailureAction`/`KafkaDeliveryFailureAction`
also logged their own `park`/`redeliver` outcome, two lines for one event under `PARK`, and, once `redeliver` gained
its own log line here too, under `REDELIVER` as well. The bridge-side cause logs move to `debug`. The action's own
`park`/`redeliver` log is the one `warn` line an operator sees, in all four bridges and both actions. A permanent
stop stays logged at `error`, unaffected, since it is a distinct, more serious event than an ordinary delivery
failure.

## Amendment (2026-08-22): a routing outcome says which of six things happened, and a stale catch-up cannot act on the id it lost

The follow-up the 2026-08-21 amendment above named as "the cleaner long-term shape", a distinct `RoutingOutcome`
value for a lifecycle refusal, is done here, along with a second value that the same review round showed was needed
for the split to be worth anything. The epic's own fixpoint round found three more defects on the shared
catch-up-then-live files, which are in the same change because they are the same code paths.

**`NOT_DELIVERABLE` meant three different things, and a bridge told them apart by whether an exception came with
the outcome.** That rule was true, but it was written down in two bridge class javadocs and in the amendment above,
describing another module's control flow, and nothing in the type said it. The enum now has six values.
`UNAVAILABLE` is the lifecycle answer, nothing registered, the model not running, or the sole subscription paused,
and it never comes with an exception. `NOT_DELIVERABLE` narrows to two things, both of which do come with one.
The filter itself failing to answer is one. A registered action refusing before it attempted any dispatch, without
promising that refusing is permanent, is the other, which is what a full live buffer during a replay gets.
`REFUSED` is the same kind of refusal with that promise attached.

**A bridge cannot decide on `REFUSED` alone unless the action says whether refusing is permanent.** A
catch-up-then-live engine refuses for two reasons. Its catch-up has failed, which never clears, and its live buffer
is full while a replay is still running, which clears when the replay drains. Both used to arrive as the same
exception type. `RoutingAction.Refusal` now records the action's own promise, and the two push models set it from
their engine, so a full buffer reports `NOT_DELIVERABLE` and goes through `DeliveryFailurePolicy` while a failed
catch-up reports `REFUSED` and stops the bridge.

That is what lets all four bridges stop importing `BlockingHandover` from its `internal` package. The amendment
above judged that import acceptable for one `catch`. It is no longer needed. The two CloudEvent bridges read the
outcome, and the two domain bridges ask their feed, since `DomainEventFeed.acceptCloudEvent` delivers inline and
reports no routing outcome of its own. `DomainEventFeed.refusesPermanently()` is the accessor for that, and it only
ever goes from false to true, which is what makes it safe to read after catching a refusal rather than at the
moment the refusal was thrown.

**A refusal that escaped a handler stopped a healthy bridge.** A handler that reaches into a second engine whose
catch-up has failed lets that engine's refusal out through the first. Both throw the same type, so the first
engine wrapped it as its own refusal and reported a handler that genuinely ran as not having run. The refusal now
records the engine that threw it, and each engine compares identity before claiming it.

**A cancelled catch-up could still act on the id it lost, on the reactor stack.** The blocking model has compared
each attempt against the id's current owner since ADR 104's own follow-ups. The reactor model compared by key, so
after a cancel and a fresh subscribe the old attempt kept working through a history nobody was listening to, wrote
the marker that makes the next catch-up skip its history, evicted the replacement's launcher, and handed the live
feed a pause meant for the replacement.

It cannot reuse the record that says a replay is running. `ReactiveHandover` releases that at the drain so
`isCatchingUp` stays true while the events buffered during the history read are delivered (ADR 132 decision 6), and
for a catch-up with nothing buffered the release happens before the catch-up reports done. A check against that
record would be true for a catch-up that buffered something and false for one that did not. The reactor model keeps a separate record
of which attempt owns each id, written only under its own monitor.

The relaunch check reads that record too. It used to read the running one, which is already gone in the window
between the drain and the launcher being dropped, so a resume landing there took a catch-up that had just succeeded
for one a stop had interrupted, and replayed its whole history again over a handover that was already live.

**Subscribing was not one step against cancelling.** Registering on the live feed, keeping the handover, keeping the
launcher and starting the replay happened one after another with nothing holding them together, so a cancel
arriving part way through left some of them behind for a subscription that no longer existed. Both models hold
their monitor across the whole of it, and cancelling takes the same monitor. Pausing and resuming take it too, so a
pause can no longer be recorded and ignored at once.

The relaunch a stopped replay owes itself runs after that monitor is released. Deciding under it and acting outside
it is what keeps the replay it starts free to take the monitor for its own completion.

**A concurrent producer was told the live buffer had overflowed.** `ReactiveHandover`'s sink comes from the safe
spec, so a second thread offering at the same time is rejected rather than corrupting the queue. That rejection was
reported as an overflow, with the advice to rebuild the read model offline. It is retried briefly instead, since
the claim clears as soon as the thread holding it finishes its own offer. A failure in the live phase also logs at
error now, because the catch-up signal has already completed by then and nothing else tells anyone.

## Amendment (2026-08-22): a catch-up marker means the id's history has been read, and every later attempt trusts it

The marker `CatchupThenPushSubscriptionModel` writes when a catch-up finishes had two meanings at once, and they
disagreed.

Across a restart it meant what its constructor documentation says, that this subscription id has read its history
and the next process can skip it. In one process it meant something narrower. A replacement attempt taking an id
whose previous attempt was still writing its marker distrusted that marker and read the whole history again. That
distrust rested on a map this model keeps in memory, so it did not survive a restart. Same durable state, two
different answers, decided by whether the process happened to stay up.

Closing that by making the distrust durable is not available. A checkpoint write can only be refused against
something already stored, and the losing case starts with nothing stored at all. An attempt reads the whole
history, loses the id to a cancel, and its write reaches an empty storage, which `notOlderThan` and `ifAbsent`
both accept by design (ADR 116). For a condition to refuse it, the replacement would have to record a
version first, and the replacement does not exist yet when that write is already in flight. Recording one at the
start of every attempt needs a way to store a version without storing a checkpoint, which no `CheckpointStorage`
offers, and `delete` clears the version along with the checkpoint rather than raising it. So the fence would be a
new durable claim operation on both `CheckpointStorage` interfaces, in every implementation, with
`cancelSubscription` making a store call it can fail. That is a lot of public surface for one window, and it buys
an answer the marker was never asked for.

**So the per-id meaning is the only meaning.** A marker is written only by an attempt that read the whole history
and still owned the id when the write began, and a marker that is there is trusted by every later attempt, in this
process and after a restart.

Two things already made the first half true and stay as they are. The replay stops at its next event once the id
moves, so an attempt that loses the id part way through never reaches the marker step. And the marker step asks
whether it still owns the id before it writes, so an attempt that lost the id between its last event and that step
writes nothing. The blocking model asks and writes under one monitor, which is why a cancel there waits for the
write rather than racing it. The reactor model asks the same question and then writes outside the monitor, because
a checkpoint store can take as long as it likes and every lifecycle call on that model takes the same monitor. A
cancel landing during that write no longer needs to stop it, since what the write claims is true whatever happens
next.

What goes is the reactor model's record of which attempt was writing a marker, and the distrust that read it.

A caller that wants an id to read its history again deletes its checkpoint, which is the recovery ADR 116 already
documents for a subscription that must start over. That is now the only way to ask for it, in process as well as
after a restart, rather than a cancel and a fresh subscribe sometimes meaning the same thing.

Two things about that write turned out to need saying, both found while implementing the amendment above.

**The write no longer holds the model monitor.** The blocking model asked its ownership question and wrote the
marker inside one `synchronized` step on the model itself, which is what made the answer and the write one step.
It also meant a checkpoint store that took seconds to answer held every other lifecycle call on that model for as
long as the write ran, and the replay runs on a virtual thread, so blocking inside `synchronized` held the platform
thread underneath it too. ADR 131 decided that same tradeoff the other way for the catch-up models, and the same
reasoning applies here. The ownership question and the write now happen under a lock for that subscription id.
`cancelSubscription` and `subscribe` take the same lock, since those are the two calls that move an id out from
under a write, so the atomicity is unchanged. `stop`, `start`, `pause` and `resume` do not, so they no longer wait
for a store, with one exception. A `cancelSubscription` for the same id waits for the write by design, and it is
holding the model monitor while it does, so a lifecycle call arriving behind such a cancel waits for that store
call after all. Removing that too means taking the per-id lock before the monitored section rather than inside it,
in both `cancelSubscription` and `subscribe`, which is recorded on
[#893](https://github.com/johanhaleby/occurrent/issues/893) rather than done here. The model monitor is always taken before that lock and never after it, which is what keeps the two
from deadlocking.

A registration is the only thing that creates a lock. `launchReplay` makes it under the same monitor that
publishes the id and before it publishes it, so a cancel for a registered id always finds one, and the write takes
only that lock and never the monitor. Creating it at the first write instead does not work, and it is worth writing down
why, because it looks like it should. A lifecycle call that finds no lock runs its work without one, and a
`ConcurrentHashMap` get can return null while a `computeIfAbsent` for that key is still in flight, so a cancel can
read no lock, run its removal unlocked, and return while the write it should have waited for is starting. That one
is a loss rather than an untidiness, through the recovery this model documents. The cancel returns, the caller
deletes the checkpoint to force a replay, and the write nobody waited for puts a marker back, so the next
subscription skips its history. Creating the lock at registration is what makes a missing lock proof that the id
was never registered, which is the reading the unlocked branch depends on, and it is what ADR 131 does. A
lifecycle call still takes a lock only if it finds one, so an id this model never registered, an arbitrary one
passed to `cancelSubscription` for instance, adds nothing to the registry.

**A stop refuses a marker write that has not started, and cannot refuse one that has.** The replay is asked
whether to keep going before every event and never after the last one, so a stop arriving after that reaches the
marker step with the replay already finished. `BlockingHandover` asks once more right before it hands over, which
covers the gap after the last event, and the ownership question the amendment above introduced asks only who owns
the id. Both marker steps now ask whether the model is stopped as well.

That is as far as a stop can reach, and `stop()` says so on both stacks rather than promising more. A write that
has already begun is not called off, because calling it off would mean either waiting for a checkpoint store
inside `stop()`, which is what taking the write off the monitor exists to avoid, or abandoning a store call whose
outcome nobody can then know. Such a marker stands, and it is entitled to, since the attempt that made it had
read the whole history and held the id when it began, which is all a marker claims.

Each stack ends up somewhere different, because the two order the write differently against the drain of the
events buffered during the replay. The blocking handover drains and then writes, so a stop arriving during the drain is
refused. The reactive one writes and then drains, so by the time a buffered event is being delivered its marker is
already written and there is nothing left to refuse. Neither is wrong under the contract above, but they are not
the same answer, and the reactive ordering is recorded on [#893](https://github.com/johanhaleby/occurrent/issues/893)
rather than changed here, since moving that write past the drain rearranges the pipeline rather than
changing this one step.

## Amendment (2026-08-22): the RabbitMQ channel-generation fence is removed, because the client already refuses to act on a stale delivery tag

The fence the first 2026-08-22 amendment above describes is gone from `RabbitMqCloudEventBridge` and
`RabbitMqDomainEventBridge`. That paragraph stays as the record of what was built and why it looked right at the
time. This amendment records why it had to go.

**It caused the failure it was written to prevent.** amqp-client re-issues `basic.consume` inside its own
`recoverTopology`, which runs before `notifyRecoveryListenersComplete`. The broker therefore hands the recovered
consumer the requeued message before any `RecoveryListener` has run, `handleDelivery` reads the generation from
before the bump, and the bump happens while that delivery is still being processed. Both drop sites then abandoned
the delivery unacknowledged, `handleDelivery` with a `warn` and `releaseHeldDeferredDelivery` with nothing at all.
At the default `prefetchCount` of 1 the broker sends nothing more on that consumer, so the bridge stopped consuming
and stayed stopped until someone closed it. That is
[#922](https://github.com/johanhaleby/occurrent/issues/922), seen four times in CI before it was understood.

**The premise it rested on was false.** The class javadoc claimed delivery tags restart at 1 on a fresh channel.
They do not, on the only kind of connection the fence was ever registered for.
`RecoveryAwareChannelN.inheritOffsetFrom` gives a recovered channel an offset equal to the dead channel's highest
seen tag, and `basicAck`, `basicNack` and `basicReject` return without transmitting anything when a tag minus that
offset is zero or below. Every channel an `AutorecoveringConnection` hands out is one of those, behind an
`AutorecoveringChannel` that delegates the three calls straight through. Acting on a tag from the dead channel was
already a no-op inside the client, decided by the one component that knows the offsets. A connection with automatic
recovery turned off gets a plain `ChannelN` that nothing ever replaces, so no delivery can arrive on it once it
dies and there is no stale tag there either.

**What the bridges do now.** A held or in-flight delivery tag is acknowledged or negatively acknowledged like any
other, whatever happened to the connection underneath it, and no delivery is ever abandoned unacknowledged. The
generation counter, the `RecoveryListener` registration, the consumer shutdown callback, `isStaleGeneration`,
`logStaleGeneration` and the generation field of `HeldDelivery` are all gone, and both held-tag deques hold plain
tags again.

**One duplicate is the price, under `PARK` only.** A delivery that fails while the connection is recovering is
published to the parking destination, and the acknowledgement that normally follows the park does nothing, so
RabbitMQ requeues the message as well. The result is a parked copy plus a copy still on the source queue. That sits
inside the at-least-once delivery these bridges promise everywhere else, and the alternative was keeping the whole
counter, its listener and its lock coupling alive for one branch, to remove a duplicate nothing else here offers.

The unit tests that asserted the drop,
`a_tag_from_a_generation_that_has_since_moved_on_is_dropped_rather_than_redelivered` in both
`RabbitMqCloudEventBridgeReleaseHeldDeferredDeliveryTest` and its domain twin, are deleted rather than replaced.
Nothing in this repository decides whether a stale tag is safe any more, the client does, so no test with a stubbed
release call can check it. `RabbitMqCloudEventBridgeConnectionRecoveryTest` is what covers the behaviour now, with
a second test that delays every recovery listener on the connection past the redelivery. It fails against the fence
and passes without it.
