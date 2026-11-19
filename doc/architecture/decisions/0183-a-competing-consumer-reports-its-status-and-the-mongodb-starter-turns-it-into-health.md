# 183. A competing consumer reports its status, and the MongoDB starter turns it into health

Date: 2026-11-19

## Status

Accepted. Resolves [#1441](https://github.com/johanhaleby/occurrent/issues/1441). Applies to
`CompetingConsumerSubscriptionModel`, `SpringMongoLeaseCompetingConsumerStrategy`,
`NativeMongoLeaseCompetingConsumerStrategy` and the MongoDB Spring Boot starter. The health contributors are built the
way [ADR 171](0171-a-rabbitmq-sink-and-source-report-to-a-listener-for-metrics-and-health.md) builds the RabbitMQ one.

## Context

A node running `CompetingConsumerSubscriptionModel` gives no outside sign of where a subscription is. A node on standby
and a node whose subscription the wrapped model refused to start look the same to Spring Boot Actuator, and so does a
node that has kept failing to pause or resume a subscription for an hour. The log is the only place that says so.

One case gets past every indicator Spring Boot has. When MongoDB answers reads and refuses writes, the lease refresh
fails on every node, no node holds the lease, and every node waits on standby. Spring Boot's MongoDB indicator runs a
read, so it stays up, while no subscription runs anywhere.

## Decision

**`CompetingConsumerSubscriptionModel#competingConsumerStatus(subscriptionId)` returns a `CompetingConsumerStatus`.** It
has the subscriber id, a state and an optional failure with the time the failures began.

| State | When |
| --- | --- |
| `LEASE_HELD` | The wrapped model runs the subscription and `hasLock` answers `true`. |
| `STANDBY` | The subscription competes for the lease and this node doesn't run it. A node that lost the lease is on standby, although `isPaused` answers `true` for it. |
| `PAUSED` | The user paused the subscription, or the model holds it paused because the wrapped model refused to start or resume it from its position. The refusal is the failure in the second case, until the user resumes it. |
| `STOPPED` | The model is stopped and the user hasn't resumed the subscription since. |
| `GIVEN_UP` | The wrapped model refused the start after `subscribe` returned, so the model forgot the subscription and freed its id as ADR 181 describes. The refusal is the failure. |

A call to the lease strategy or the wrapped model that throws, and that the model tries again, records the failure. The
failure stays, with the time of the first one, until a try brings the subscription to the state it should be in.
`competingConsumerStatuses()` lists every status.

A given-up subscription is kept in a map keyed by subscription id, since the model has forgotten everything else about
it. A subscribe with the same id removes it, and so do a cancel of that id and a shutdown. The status also reads a
refusal the wrapped model has reported and the model hasn't acted on yet, so `GIVEN_UP` shows at once.

**`CompetingConsumerStrategy` is unchanged.** The two MongoDB lease strategies get `leaseRefreshFailure()`, which
returns the failure of the latest refresh attempt and when the failures began, or empty once an attempt refreshed every
lease. The model records its own failures, so the strategy doesn't have to report them through the interface, and a
strategy written by an application needs nothing new.

**The MongoDB starter adds two health contributors when `spring-boot-health` is on the classpath.**

- `competingConsumers` has one component per subscription id of each `CompetingConsumerSubscriptionModel` it finds by
  unwrapping the `SubscriptionModel` beans that exist. A component is down when the status has a failure, which is a
  given-up subscription, a subscription held after a refusal and a call that failed with no success since. Standby, a
  pause by the user and a stopped model are up while no call fails. The details are `state`, `subscriberId`, `failure` and `since`.
- `competingConsumerLeases` has one component per `SpringMongoLeaseCompetingConsumerStrategy` bean, down while
  `leaseRefreshFailure()` returns a failure.

Both follow ADR 171. They look up the beans on every health call and never create one, they turn off with
`management.health.competingconsumers.enabled=false` and `management.health.competingconsumerleases.enabled=false`, and
an application bean under one of the three names Spring Boot maps to the contributor name replaces the starter's. Neither
is in a probe group, and there is no threshold and no gauge.

### Alternatives

- **A failure callback on `CompetingConsumerStrategy`.** Every strategy an application wrote would have to implement or
  inherit it, and the model already sees each failure of a call it makes.
- **Health on `isPaused` alone.** A node that lost the lease answers `true`, so every standby node would be down.
- **One contributor per model.** A subscription is what an operator looks for, and one level matches
  `rabbitMqBroker`.
- **A threshold before a failing call counts as down.** The failure records when it began, so a monitor can wait as long
  as it likes. A threshold can be added later without changing what the status reports.

## Consequences

An application on the MongoDB starter with Actuator sees a given-up subscription, a refused start, a call that keeps
failing and a lease refresh that keeps failing in `/actuator/health`. Spring Boot reports the overall status as down
when one of them is, so a probe that reads `/actuator/health` itself, rather than the liveness or readiness group, can
restart a node over a subscription. The upgrade guide says how to turn them off.

A given-up subscription stays down until someone subscribes it again or cancels it, also when the cause is gone.
