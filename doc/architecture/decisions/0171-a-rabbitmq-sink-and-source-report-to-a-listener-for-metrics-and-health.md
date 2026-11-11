# 171. A RabbitMQ sink and source report to a listener for metrics and health

Date: 2026-11-11

## Status

Accepted. Resolves [#1437](https://github.com/johanhaleby/occurrent/issues/1437). Applies to `RabbitMqCloudEventSink`,
`RabbitMqCloudEventSource`, `RabbitMqDomainEventSource` and the RabbitMQ Spring Boot starter.

## Context

The RabbitMQ broker modules have no metrics and no health indicator. When a sink fails to publish, or a source stops
for good, the log is the only place that says so. A service on Spring Boot keeps reporting healthy, and nothing counts
how many events were published, returned or parked.

The broker modules are plain Java and depend on nothing but the RabbitMQ client, which is why a non-Spring application
can use them. Micrometer and Spring Boot Actuator belong in the starter, where the rest of the Spring wiring is.

## Decision

**The sink and the sources call a listener, and know nothing about Micrometer.** Each builder takes one, and the
default does nothing.

```java
public interface RabbitMqSinkListener {
    default void published(RabbitMqDestination destination, Duration publishTime) { }
    default void unroutable(RabbitMqDestination destination) { }
    default void failed(RabbitMqDestination destination, Throwable failure) { }
}

public interface RabbitMqSourceListener {
    default void delivered(String queue) { }
    default void failed(String queue, @Nullable Throwable failure) { }
    default void parked(String queue) { }
    default void redelivered(String queue) { }
}
```

| Callback | When |
| --- | --- |
| `published` | The broker confirmed a publish attempt and did not return it. `publishTime` runs from the start of the attempt to the confirm. |
| `unroutable` | The broker returned a publish attempt, under both unroutable policies. |
| `failed` (sink) | A publish attempt threw for any other reason, the outbound mapper included. A publish the sink retries can call it more than once. |
| `delivered` | The source acknowledged a delivery its subscriber took or filtered out. |
| `failed` (source) | A delivery went to the delivery failure policy. `failure` is `null` for a `NOT_DELIVERABLE` outcome. |
| `parked` | The parking destination confirmed a parked copy and the source acknowledged the original. |
| `redelivered` | The source handed a failed delivery back to the queue, under `REDELIVER` or after a park that failed. |

A listener that throws is logged and ignored, so a broken metrics binding never fails a publish or a delivery.

**The sink and the sources say whether they work.** `RabbitMqCloudEventSink#isOpen()` is `false` once the sink is
closed or its connection is. `status()` on both sources returns a `RabbitMqSourceStatus`, `CONSUMING`, `STOPPED` after
a permanent stop, or `CLOSED`, and `isConnected()` says whether the connection is open.

**The starter adds metrics and health only when their libraries are there.** `micrometer-core` and
`spring-boot-health` are optional dependencies of the starter.

- With a `MeterRegistry` bean, the starter passes a Micrometer listener to its sink and to every builder its source
  factories return. The counters are `occurrent.rabbitmq.sink.published`, `.unroutable` and `.failed`, tagged with the
  exchange, and the timer `occurrent.rabbitmq.sink.publish`. The source counters are
  `occurrent.rabbitmq.source.delivered`, `.failed`, `.parked` and `.redelivered`, tagged with the queue. The routing
  key is not a tag, since a type-derived routing key has one value per event type.
- With `spring-boot-health` on the classpath, a health contributor named `rabbitMqBroker` has one indicator per sink
  and source bean that exists. A sink is down when `isOpen()` is `false`. A source is down when its status is not
  `CONSUMING` or its connection is closed. The contributor never creates a bean, so a `@Lazy` sink nothing has asked for
  yet is not listed.

An application listener bean replaces the Micrometer one, and an application that builds its sink or source by hand
sets a listener on the builder.

**Standby on a competing consumer model is not part of this.** A sink cannot see the subscription that feeds it, so it
cannot tell a standby instance from a stopped one. [#1441](https://github.com/johanhaleby/occurrent/issues/1441) tracks
that for the framework starter.

### Alternatives

- **Micrometer as an optional dependency of the broker modules.** It ties plain Java code to one metrics library, and
  every call site would need a check for whether the class is there.
- **The Micrometer `Observation` API.** It also gives tracing, but an observation wraps one operation, and parking and
  redelivery happen on paths that are not one operation. The listener can be adapted to observations later.
- **One listener for both directions.** An application that only consumes would implement half of it for nothing, the
  same reason [ADR 170](0170-a-rabbitmq-source-and-sink-take-a-mapper-for-the-wire-format.md) gives two mappers.

## Consequences

A service on Spring Boot with Actuator and Micrometer gets broker metrics and health without code of its own. An
application without Spring can count the same events through the listener. Two more callbacks per message cost one
virtual call each when nothing listens.
