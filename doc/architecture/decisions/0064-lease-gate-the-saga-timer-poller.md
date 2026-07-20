# 64. Lease-gate the saga timer poller for multi-instance execution

Date: 2026-07-20

## Status

Accepted. Additive on top of the saga DSL (ADR 63), blocking stack only.

## Context

A saga (ADR 63) is driven by two independent activities: a subscription that delivers events, and a timer poller that
queries the saga state store for due timeouts and fires them. When an application runs in multiple instances, these two
activities behave very differently across the fleet.

The event path is already single-active by default. The Spring Boot starter injects a competing-consumer subscription
model (ADR 22) backed by a Mongo lease, so for a given subscription id only one instance receives events at a time. That
is exactly the coordination a saga wants for its event side, and it comes for free from the default wiring.

The timer poller has no such coordination. `SagaRunner.run` starts a `ScheduledExecutorService` on every instance that
calls `SagaStateStore.findWithDueTimers` every poll interval (1 second by default). With N instances that is N times the
timer-query load against the store, every interval, forever, even though at most one instance needs to fire a given due
timer. The firing itself stays correct under this redundancy, because the executor dispatches commands before a
compare-and-set save and a lost CAS discards the duplicate (the at-least-once contract of ADR 63), but the query load is
pure waste and grows linearly with the number of instances.

The competing-consumer machinery that already solves this for subscriptions is reusable. `CompetingConsumerStrategy`
(the lease SPI) lives in `occurrent-subscription-api-blocking`, which the saga blocking module already depends on for
`Subscribable`, and its lease is keyed by opaque strings, not by anything subscription-specific. So the poller can be
gated by the same mechanism with no new module and no change to the SPI.

## Decision

**The timer poller is gated by a competing-consumer lease when a `CompetingConsumerStrategy` is supplied.** A new
`SagaRunner.run` overload takes an optional strategy. When present, the runner registers a competing consumer for the
saga and, on each poll tick, checks `hasLock` before querying the store; only the instance that currently holds the
lease polls, the others wake and return immediately without touching the store. When the strategy is absent (the
shorter overloads, in-memory and single-node use), the poller runs on every tick exactly as before. This is additive:
no existing signature changes, and no backward-incompatible change is introduced, so no OpenRewrite recipe or migration
is needed.

**The poller's lease key is namespaced apart from the event subscription's lease.** The event subscription already
holds a lease keyed by the raw subscription id in the shared `competing-consumer-locks` collection. The poller uses
`saga-timer:<subscriptionId>` (`SagaRunner.timerLeaseKey`). Reusing the raw id would make the poller compete with the
event subscription for one lock document, which it could never win on any instance, so timers would never fire. The two
leases are therefore separate documents that coexist.

**Gating is checked in-memory, not by querying the lock.** `hasLock` reads the status the strategy's own background
lease-refresh thread maintains, so a standby instance's poll tick costs nothing against the database. The check can lag
reality by up to one refresh interval (half the lease time), so during a failover two instances can briefly both poll.
That is the same window the subscription model already tolerates and is safe under the existing CAS plus idempotency
contract.

**The lease is released on close.** `SagaSubscription.close` unregisters the competing consumer before stopping the
poller, so another instance takes over within roughly one lease period rather than waiting for the lease to expire.

**The Spring Boot starter gates by default.** The starter already builds a single
`SpringMongoLeaseCompetingConsumerStrategy` bean for the subscription model. The saga post-processor resolves that same
bean and passes it to every `@Saga` runner, mirroring the subscription-model default. This is opt-out via
`occurrent.saga.competing-consumer.enabled` (default `true`); when disabled, or when no strategy bean exists (for
example subscriptions disabled), the poller runs ungated on every instance.

**Only the timer poller is coordinated, not the cross-node dispatch race.** Gating the poller removes the query load,
which is the goal. It does not change the residual cross-node race ADR 63 documents (an event on one instance
interleaving with a timeout fired on another), because the poller lease and the event-subscription lease are
independent and may be held by different instances. Closing that race would require co-locating both leaderships on one
instance and is left to the same future work as the exactly-once outbox in ADR 63.

## Consequences

- Timer-query load against the store no longer grows with instance count. With the default starter, one instance polls
  a given saga's timers at a time and the rest cost only their share of the lease refresh, the same overhead the
  subscription model already pays.
- Failover is automatic: when the polling instance dies, another acquires the timer lease and resumes firing within
  roughly one lease period.
- The behavior is unchanged for single-node and in-memory use (no strategy supplied), and the change is fully additive,
  so there is no migration.
- The reactive starter is unaffected: `@Saga` is a blocking-stack feature (ADR 63), so there is no reactive poller to
  gate.
- The residual cross-node duplicate-dispatch race is unchanged and still handled by CAS plus idempotent, decider-backed
  receivers. Eliminating it (single-leadership co-location) remains deferred alongside the ADR 63 outbox.
- A neutral leader-election module was considered and not built. The lease SPI is reused as-is; extracting it into a
  shared module would be a breaking rename for a mostly cosmetic benefit and is left until a third consumer needs
  generic leader election.
