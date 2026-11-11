# 169. A RabbitMQ sink can log an unroutable event and continue

Date: 2026-11-11

## Status

Accepted. Resolves [#1435](https://github.com/johanhaleby/occurrent/issues/1435). Applies to `RabbitMqCloudEventSink`
and the RabbitMQ Spring Boot starter. Amends
[ADR 133](0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md) on one point, what the
sink does when the broker returns a message it could not route.

## Context

ADR 133 makes the RabbitMQ sink publish with `mandatory` set and treat a `basic.return` as a failed publish. A typo in a
routing key then fails loudly instead of looking like a working deployment.

When the sink forwards stored events from a durable subscription, a failed publish makes the subscription try the same
event again. An event type that no queue is bound for therefore stops the forwarding of every event after it, until
someone binds a queue for that routing key.

That is the right outcome when a lost message is worse than a stuck subscription. On an exchange where events are
published for whoever wants them, an event nobody consumes is normal, and the stuck subscription is the bug. Spring
AMQP's `RabbitTemplate` does not set `mandatory` by default, so the broker drops such a message and the publisher moves on.

## Decision

`RabbitMqCloudEventSink.Builder#onUnroutable(RabbitMqUnroutablePolicy)` picks what a return does.

| Policy | What `publish` does on a return |
| --- | --- |
| `FAIL`, the default | Throws `RabbitMqUnroutableEventException`, as in 0.34.0. |
| `LOG_AND_CONTINUE` | Logs a warning with the exchange, the routing key, and the id and type of the event, and returns normally. |

The starter exposes it as `occurrent.broker.rabbitmq.sink.on-unroutable`, with `FAIL` as its default.

**The sink sets `mandatory` under both policies.** Turning it off would make an unroutable event invisible, since the
broker would neither return the message nor say anything else about it. With `mandatory` set, the sink still learns of
every return and can log it under `LOG_AND_CONTINUE`. The metrics planned in
[#1437](https://github.com/johanhaleby/occurrent/issues/1437) need the same signal to count unroutable events.

**The policy applies inside one publish attempt, before the `RetryStrategy` sees anything.** A retry strategy that
retries every exception would otherwise publish an event again after the policy had already accepted it as dropped.
Under `LOG_AND_CONTINUE` a return never reaches the retry strategy, so the event is published exactly once.

**A parked delivery still fails on a return.** The parking path of a RabbitMQ source uses its own publisher and
ignores this policy. If a park went nowhere, acknowledging the original would lose the event, which is what ADR 133
set out to prevent.

**The policy is set per sink, not per `RabbitMqDestination`.** `RabbitMqDestination` is a released record that also
describes bindings and parking destinations, where an unroutable policy means nothing. Adding a component would change
its canonical constructor and its `equals`. An overload that takes a `Function<RabbitMqDestination,
RabbitMqUnroutablePolicy>` can be added later without breaking anything, once there is a case that needs it.

## Consequences

An application that publishes to an open exchange can set `LOG_AND_CONTINUE`, and an event type without a queue no
longer stops the forwarding of the events after it. The broker drops that event, and the warning in the log is the only
trace of it.

Nothing changes for an application that sets nothing. `FAIL` is the default and behaves like 0.34.0.
