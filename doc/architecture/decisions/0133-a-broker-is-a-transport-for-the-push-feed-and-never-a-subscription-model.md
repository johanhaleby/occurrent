# 133. A broker is a transport for the push feed, and never a subscription model

Date: 2026-08-18

## Status

Accepted. Decides [#412](https://github.com/johanhaleby/occurrent/issues/412), the design layer for the broker work
tracked by [#388](https://github.com/johanhaleby/occurrent/issues/388). This ADR decides a design and writes no code.
The modules it names are built by #413 through #417.

**Scope.** Blocking only. The reactor variants are [#418](https://github.com/johanhaleby/occurrent/issues/418) and are
not decided here, beyond the requirement that nothing below prevents them. Spring Boot auto-configuration is not in
scope either, for the reason given in decision 9. The `@RabbitSaga` and `@KafkaSaga` sketches on
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
`OccurrentCloudEventExtension`, and `dcbtags` comes from `EventStoreCloudEventExtensions`. `EventMetadata` reads all
four off the CloudEvent, and every DSL that keys on stream or position reads them through it. A message that arrives
without them produces an empty `EventMetadata`, and ADR 62's 2026-07-26 amendment recorded what that does to a
projection keyed by metadata. The loud half throws. The quiet half returns `null` for the projection id, which is a
documented instruction to skip the event, so the projection silently receives nothing.

## Decision

### 1. Each transport module contributes a bridge into the push feed that already ships

A transport module consumes a broker message, rebuilds the event, and hands it to `Pushable.accept(CloudEvent)` at the
CloudEvent level or to `DomainEventFeed.accept(EventMetadata, E)` at the domain level.

No new `SubscriptionModel` is added, for the reason above, and none of `PushSubscriptionModel`, `Pushable`,
`DomainEventFeed`, `CatchupThenPushSubscriptionModel` or `RegisteringSubscribable` changes.

Two existing rules constrain what a bridge may do, and both are there to stop events being lost.

[ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) decided that an `accept(...)` which returns
normally without delivering is an acknowledgement of an event nothing consumed. So a bridge acknowledges the broker
message only after `accept(...)` returns normally, and never when it throws.

[ADR 90](0090-a-push-sink-feeds-one-consumer.md) decided that a push sink takes exactly one consumer, because one
received message has one acknowledgement decision. So one bridge feeds one `Pushable` or one `DomainEventFeed`, and a
deployment with three projections has three queues on RabbitMQ or three consumer groups on Kafka.

### 2. Three interface families, in an api module with no transport dependency

**`EventDestination`** says where an event goes. It is one record per transport, defined in that transport's own
module, because an exchange and a routing key are not a topic and a partition key and pretending otherwise buys
nothing. `RabbitMqDestination` has an exchange, a routing key and headers. `KafkaDestination` has a topic, a nullable
message key and headers.

Headers are a component of the record from the start rather than a later addition. A header added by the sink instead
would be the same for every event the sink publishes, and the whole reason to want application headers is that they
vary with the event.

**`DestinationResolver<D extends EventDestination>`** derives the destination, and it answers in both directions:

```java
D destinationFor(CloudEvent cloudEvent);

Set<D> destinationsFor(SubscriptionFilter filter);
```

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

All three live in `occurrent-broker-api-blocking`, which depends on `occurrent-subscription-core` for
`SubscriptionFilter` and on the CloudEvents SDK, and on no broker client.

### 3. Both directions work at the CloudEvent level and at the domain level

The CloudEvent level is the base. The domain level exists so an application whose message converter already produces
domain events does not convert twice, which is the same reason ADR 62 added `DomainEventFeed` beside
`PushSubscriptionModel`.

The domain-level types live in the same artifact as their transport, in a separate package
(`org.occurrent.broker.rabbitmq.blocking.domain` beside `org.occurrent.broker.rabbitmq.blocking`). A separate artifact
would depend on the transport artifact and add two classes to it, and a user would then have to know which of two
dependencies to declare for one broker.

The two levels are not symmetric about where the conversion happens, and the asymmetry is deliberate.

On the publish side, `DomainEventSink<E>` holds a `CloudEventSink` and a `CloudEventConverter<E>`, converts, and
delegates. A domain event has to be serialized exactly once whichever way it is done, so delegating costs nothing and
it keeps one place that writes the extension headers.

On the consume side there is no such delegation. The domain bridge reads the message body with the application's
converter and the extension headers into an `EventMetadata`, then calls `DomainEventFeed.accept(metadata, event)`.
Routing it through the CloudEvent bridge instead would decode the body into a CloudEvent and then decode it again into
a domain event, which is exactly the double conversion ADR 62 added the domain feed to avoid.

### 4. The Occurrent extensions are written as message headers, at both levels and in both directions

`streamid`, `streamversion`, `position` and `dcbtags` are on the message as headers, never only inside the body.

This is an invariant rather than a convenience. `EventMetadata` is how the subscription, saga, projection, view and
DCB DSLs all read stream identity and position, so a message without those headers produces an empty `EventMetadata`
and the failure described in the Context section. It also makes
[#389](https://github.com/johanhaleby/occurrent/issues/389) harder rather than easier, since a consumer that cannot
see a stream version cannot detect that it received events out of order.

At the CloudEvent level the sinks write through the CloudEvents SDK's own binary message writer for that transport, so
the four extensions are written as CloudEvent extension attributes and the header names follow the CloudEvents binding
specification rather than a naming scheme invented here. Decision 8 covers the binding mode itself.

At the domain level the extensions come from wherever the caller got them, and the API says so rather than pretending
otherwise. `publish(EventMetadata, E)` writes the four headers from the metadata the caller supplies, which is what a
subscription forwarding stored events in domain space has. `publish(E)` writes whatever the `CloudEventConverter`
produced, and for an event that has never been through the event store that is nothing, because a stream version and a
position are properties of a stored event and Occurrent cannot derive them. A consumer of such a message sees an empty
`EventMetadata`, and a projection keyed by metadata refuses it at delivery.

### 5. A binding derived from a filter narrows what arrives, and never decides what is handled

`destinationsFor(SubscriptionFilter)` exists so a consumer can bind only the routing keys it wants, or subscribe to
only the topics it wants, instead of taking everything and discarding most of it.

It works for the event-type part of a filter and for nothing else, because the event type is the only part of a filter
the destination mapping knows about. A stream id, a data field, a time range and a DCB criteria are all invisible to
the broker. So `SubscriptionFilterMatcher` on the consumer side still decides whether a received event reaches the
handler, exactly as it does for a change-stream subscription model, and a bridge always applies it.

A filter whose type part cannot be derived resolves to every destination. Guessing narrower would drop an event that
the filter would have matched, and losing an event is a hard rule in `AGENTS.md` rather than a tuning question, so the
imprecise answer has to be the inclusive one.

### 6. Three modules, named after the existing convention

| Directory | Artifact | Contents |
|---|---|---|
| `broker/api/blocking` | `occurrent-broker-api-blocking` | `EventDestination`, `DestinationResolver`, `CloudEventSink`, `DomainEventSink`, the shared settings |
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

**The publish acknowledgement setting is named the same on both sinks.** RabbitMQ calls it publisher confirms and
Kafka calls it acks plus waiting on the send future, and neither name is usable on the other transport. Both builders
take `waitForAcknowledgement(boolean)` and `acknowledgementTimeout(Duration)`, and the transport javadoc names the
underlying mechanism.

Waiting is the default, with a timeout of 5 seconds. A publish that returns before the broker accepted the message
reports success for an event the broker may never have received, and `AGENTS.md` makes no design may lose events a
hard rule rather than a default worth trading for throughput.

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

**The two consume-side bridges differ in exactly one place, and it is written down here so the Kafka one does not
invent something else.** RabbitMQ has a per-message negative acknowledgement, so a bridge whose `accept(...)` threw
calls `basicNack` with requeue and the broker redelivers. Kafka has no such thing, so the bridge does not commit the
offset and seeks back to it, which is how the record is redelivered. What the Kafka bridge must not do is log the
failure and commit anyway, because that is the loss ADR 104 named.

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

Structured mode is therefore not offered, rather than offered and discouraged. An application that needs it for a
consumer outside Occurrent implements `CloudEventSink`, which is what that interface is for. Adding a structured
writer later changes neither `EventDestination`, nor `DestinationResolver`, nor either sink interface, so this is a
decision that can be revisited without a migration.

### 9. Spring Boot auto-configuration is deferred to its own issue

No `occurrent-spring-boot-starter-broker-*` module is built as part of this work.

Auto-configuration fixes property names and bean names, and doing that before anyone has wired these three interface
families by hand fixes them against a guess. The manual wiring is also small already, since `@Projection(source =
PUSH)` resolves a push feed bean without any broker knowledge, so what an application declares by hand is a sink bean
and a bridge bean per consumer.

The `@RabbitSaga` and `@KafkaSaga` ideas on #415 and #417 are the shape that layer will eventually take, and building
auto-configuration now without them would produce a layer that gets rewritten by the work that was already planned.

### 10. The RabbitMQ publishing module ships as a module, not as a documented example

#414 asks whether the RabbitMQ publishing side earns a published artifact or whether it is a documented example, and
defers the answer to what the code looks like once written. The position taken here is that it ships as a module, and
the position is provisional by the issue's own terms.

Two things decide it. The publisher confirms handling and the CloudEvents binding are the parts an application gets
subtly wrong, and both are code rather than a snippet. And an example in the documentation has no test and no version,
so it drifts from the library silently, while `AGENTS.md` already requires the documentation to describe released
behaviour rather than to be the behaviour.

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

The reactor variants have to repeat the interface families rather than share them, because `Pushable` and
`DomainEventFeed` already exist once per stack. #418 decides how much of the transport-specific code the two stacks
can share.
