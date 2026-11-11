# 170. A RabbitMQ source and sink take a mapper for the wire format

Date: 2026-11-11

## Status

Accepted. Resolves [#1433](https://github.com/johanhaleby/occurrent/issues/1433) and
[#1434](https://github.com/johanhaleby/occurrent/issues/1434). Applies to `RabbitMqCloudEventSource`,
`RabbitMqDomainEventSource`, `RabbitMqCloudEventSink` and the RabbitMQ Spring Boot starter. Amends
[ADR 133](0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md) on decision 4, that the
Occurrent extensions are written as message headers.

## Context

A RabbitMQ source reads a delivery only through `RabbitMqCloudEventMapper.toCloudEvent`, which reads the
`cloudEvents_` headers that `RabbitMqCloudEventSink` writes. A sink writes only those headers, as strings, with the
stored event data as the body.

So a source cannot consume from an exchange where another publisher already sends events, for example a plain JSON
body with the stream id and version in application headers. Every such message fails to map and goes to the delivery
failure policy.

A sink cannot publish in the format the existing consumers of an exchange expect, such as a number header, a
`messageId` property or a body in the consumer's own JSON. An application that consumes with Spring AMQP today has to
switch every publisher and every consumer of an exchange at the same time to move to Occurrent.

## Decision

Each direction takes a mapper, set on the builder.

```java
public record RabbitMqMessage(BasicProperties properties, byte[] body) { }

@FunctionalInterface
public interface RabbitMqOutboundMapper {
    RabbitMqMessage toMessage(CloudEvent cloudEvent, RabbitMqDestination destination);
    static RabbitMqOutboundMapper cloudEventHeaders() { ... }
}

@FunctionalInterface
public interface RabbitMqInboundMapper {
    CloudEvent toCloudEvent(BasicProperties properties, byte[] body);
    static RabbitMqInboundMapper cloudEventHeaders() { ... }
}
```

| Builder method | Default |
| --- | --- |
| `RabbitMqCloudEventSink.Builder#outboundMapper(RabbitMqOutboundMapper)` | `RabbitMqOutboundMapper.cloudEventHeaders()` |
| `RabbitMqCloudEventSource.Builder#inboundMapper(RabbitMqInboundMapper)` | `RabbitMqInboundMapper.cloudEventHeaders()` |
| `RabbitMqDomainEventSource.Builder#inboundMapper(RabbitMqInboundMapper)` | `RabbitMqInboundMapper.cloudEventHeaders()` |

Both defaults delegate to `RabbitMqCloudEventMapper`, so an application that sets nothing gets the 0.34.0 wire format.
The Spring Boot starter passes a `RabbitMqOutboundMapper` bean to the sink and a `RabbitMqInboundMapper` bean to
both source factories when the application declares one.

**The sink keeps everything around the mapper.** It calls the mapper inside each publish attempt, then does the
publisher confirm, the return handling of [ADR 169](0169-a-rabbitmq-sink-can-log-an-unroutable-event-and-continue.md),
the retries and the channel recovery as before. That is the reason to use the sink instead of writing a
`CloudEventSink` from scratch. A message the RabbitMQ client cannot encode, such as one with a `UUID` header value or a
`messageId` over 255 bytes, fails the publish with the client's own exception, usually an `IllegalArgumentException`.
The sink replaces its channel, since the client counts that publish as waiting for a confirm the broker never sends,
and does not retry it.

**The sink makes a message persistent when the mapper does not say.** If the mapper does not set `deliveryMode`, the
sink sets it to 2. A mapper that forgot it would otherwise publish transient messages that a broker restart can drop
after the confirm, a loss the default mapper never had. A `deliveryMode` the mapper sets is kept.

**The sink adds one header of its own.** Every message has the header `x-occurrent-rabbitmq-confirm-publisher-return-token`,
which matches a `basic.return` to the publish it belongs to, since a return has no delivery tag. A consumer that
rejects unknown headers sees it. Removing it would mean a different way to match returns across channel recoveries,
so it stays and is documented on `RabbitMqOutboundMapper`.

**A message the inbound mapper cannot map goes to the delivery failure policy.** A throw or a `null` from the mapper
takes the same path a failed `toCloudEvent` takes today, so it is parked or redelivered as the source is configured.

**Decision 4 of ADR 133 holds for the default mappers only.** A custom mapper owns the wire format, and with it whether
`streamid`, `streamversion`, `position` and `dcbtags` reach the other side. An inbound mapper that sets the `streamid`
and `streamversion` extensions from application headers gives a handler a full `EventMetadata`, so `@StreamId` and
`@StreamVersion` work. With one that does not, a handler that reads them fails on every message, so each delivery goes
to the delivery failure policy. In `RabbitMqDomainEventSource` the inbound mapper runs
before the `CloudEventConverter`, so the converter must resolve the `type` the mapper sets.

### Alternatives

- **One mapper type for both directions**, which both issues suggested. An application that only moves a consumer
  would have to implement or stub the outbound half. Two single-method interfaces are lambdas, and each is an
  unambiguous Spring bean. A `RabbitMqMapper` that extends both can be added later without breaking either.
- **`BiFunction<BasicProperties, byte[], CloudEvent>`.** It has no place for javadoc, and looking up a generic
  `BiFunction` bean in Spring is fragile.
- **Widening `RabbitMqDestination.headers()` to `Map<String, Object>`.** It changes a released record component and
  fixes only the header types, not the properties or the body.
- **A non-final sink with a method to override.** It exposes the confirm handling to subclasses and makes a larger
  surface to keep stable.
- **A CloudEvents SDK protocol binding.** The SDK has none for AMQP 0-9-1.

## Consequences

An application can move consumers and publishers of an existing exchange to Occurrent one at a time, with the wire
format the other side already uses. Nothing changes for an application that sets no mapper.

A custom mapper is the application's responsibility. Occurrent no longer guarantees that the extensions arrive as
headers once one is set.
