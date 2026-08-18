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

**The acknowledgement decision comes from a `PushObserver`, not from a check taken before or after the push.** A
bridge attaches one, and `PushSubscriptionModel` reports through `routeReportingMatch`, which evaluates the sole
registration's eligibility once and tells the observer that same answer before dispatching. A check taken separately
would be two steps, so a `stop()`, a `pauseSubscription` or a `cancelSubscription` landing between them would
acknowledge into a model that then drops the event.

**`PushObserver` has to report why, not just whether, and that is a prerequisite this ADR creates.** Today it is told
a boolean, and `routeReportingMatch` reports `false` for four different situations, the filter not matching, the model
not running, the subscription being paused, and nothing being registered. Only the first of those may be acknowledged.
The other three mean the event was not delivered, so acknowledging discards it.

A lifecycle check afterwards cannot separate them. A pause reports `false`, a concurrent resume then makes
`isRunning(subscriptionId)` true, and the bridge acknowledges a message nothing consumed. That is the same two-step
race one paragraph up, just moved after the push, so it has to be answered in the routing evaluation itself.

So the push module gains a three-valued outcome reported from that one evaluation, `DELIVERED`, `FILTERED` and
`NOT_DELIVERABLE`, and the bridge acknowledges on `DELIVERED` once `accept(...)` returns normally, and on `FILTERED`,
where redelivering would loop forever because the event is simply not this consumer's. It does not acknowledge on
`NOT_DELIVERABLE` and it does not acknowledge when `accept(...)` throws.

This refines a shape that has not shipped rather than breaking a released one, since `PushObserver` is still in the
unreleased section of `changelog.md`, so its existing entry absorbs the outcome contract instead of a migration note
being needed. [#848](https://github.com/johanhaleby/occurrent/issues/848) owns the change and has to be done before
the consume bridges in #415 and #417, which are what need the contract. It is the one change this design needs outside
the three broker modules.

**The bridge therefore holds the `PushSubscriptionModel` rather than a bare `Pushable`.** The observer is a
constructor argument on the model, and the readiness accessors are not reachable through `Pushable` either. The bridge
uses `isRunning(subscriptionId)` to decide when to start and stop consuming, which is a coarse lifecycle question
where a small delay is harmless, and never to decide a single message. `hasSubscriptions()` would be the wrong
question even there, because it stays true for a paused subscription while `route` skips exactly that registration.
The bridge knows which id to ask about because ADR 90 gives it exactly one.

What remains is that a stopped model drops live events, which ADR 85 decided and ADR 104 deliberately kept, on the
grounds that a stop is an operator act with a `start()` on the other side. A bridge stops consuming when its
subscription stops, so it does not feed a stopped model in the first place. This ADR inherits that decision rather
than accepting a new loss of its own.

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

It is not sound for a consumer filtering on subject, source or time, because that consumer can decide differently on
the live path than the same filter does during a catch-up replay over the stored events. A deployment with such a
consumer uses `CloudEventForwarder`, where nothing is regenerated because nothing is converted.

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

It evaluates that filter before decoding, on a CloudEvent rebuilt from the message rather than on the domain event.
`SubscriptionFilterMatcher.matcherFor` gives a `Predicate<CloudEvent>`, and a decoded domain event plus an
`EventMetadata` of extensions cannot answer a condition on subject, source, time or raw data. Decision 8 is what makes
the rebuild possible, since binary mode puts every CloudEvent attribute in the headers, so the bridge has all of them
plus the body without decoding anything. A filter that does not match ends there and the domain event is never
decoded, which also means the common case costs nothing.

**The bridge registers the projection on the feed, so there is one filter rather than two.** `DomainEventFeed` keeps
the `Filter` it is given inside a `CatchupProjectionFeed` and exposes neither it nor a live match, so a bridge
configured with its own filter alongside would be the two independently configured copies this decision just refused,
and replay and live delivery could disagree. Instead the bridge is constructed with the feed and the filter and is
what calls `register` on the feed.

The one thing it is configured with is a plain `Filter`, not a `SubscriptionFilter`, because those are two different
types here and only one of them fits both places. `register` takes a `Filter`, while `matcherFor` and
`destinationsFor` take a `SubscriptionFilter`, so the bridge passes the `Filter` straight to `register` and wraps it
for the other two. A `DcbSubscriptionFilter` has no plain `Filter` inside it at all, it has a `DcbCriteria`, so a DCB
filter cannot drive a domain bridge and is refused there. ADR 62 already refuses one for a catch-up replay on the same
grounds, that a DCB boundary needs a different read, so this inherits an existing limit rather than adding one.

**A filter with a payload condition needs a `DataFieldReader`, and is refused at startup without one.** The
one-argument `SubscriptionFilterMatcher.matcherFor` builds with `DataFieldReader.refusing()` and throws while
constructing the matcher when the filter reads a data field, so the bridge takes a reader and uses the two-argument
overload. That refusal happening at startup rather than on the first matching event is the existing behaviour on the
subscribe path, and inheriting it is what keeps the two consistent.

That leaves the bindings derived from the same filter the consumer is configured with at both levels. A CloudEvent
bridge that wants to see what arrived and whether it matched reads its `PushObserver`, which decision 1 already
requires it to have.

A filter whose type part cannot be derived returns an empty `Optional`, and a consumer that gets one binds the
transport's own catch-all rather than a set of destinations. On RabbitMQ that is a `#` binding on the topic exchange,
and on Kafka it is a subscription by pattern. Guessing narrower would drop an event the filter would have matched, and
losing an event is a hard rule in `AGENTS.md` rather than a tuning question, so the imprecise answer has to be the
inclusive one.

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

A sink takes a client connection, a resolver, an acknowledgement setting, a timeout and a `RetryStrategy`, so it gets
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

**The Kafka resolver uses the stream id as the message key by default.** Kafka only orders within a partition, and a
projection reading one stream needs that stream's events in order, so keying by stream id puts them on one partition.
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
