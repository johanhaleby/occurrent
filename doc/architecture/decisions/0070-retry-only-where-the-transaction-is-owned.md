# 70. Retry a write conflict only where the transaction is owned

Date: 2026-07-26

## Status

Accepted

## Context

The DCB write path retries a conflict between concurrent appends. In the Spring Mongo stores that retry wrapped the whole transaction:

```java
return executeWithTransientRetry(() -> transactionTemplate.execute(...));
```

`TransactionTemplate` uses `PROPAGATION_REQUIRED`, and the store sets `SessionSynchronization.ALWAYS`, so when a transaction is already open the template joins it rather than starting one of its own. The retry then cannot work. The first attempt hits a write conflict, MongoDB aborts the transaction, and every later attempt rejoins that dead transaction and fails on its first read with error 251 `NoSuchTransaction`. That error carries the `TransientTransactionError` label, so the retry predicate keeps matching and all 15 attempts are spent, about 5 seconds, before the failure propagates anyway.

Two situations put a transaction around the append. An application can wrap a command in `@Transactional`. Occurrent itself does it whenever a synchronous subscription is registered, because `GenericDcbApplicationService` runs the whole read-decide-append through a `TransactionExecutor` so the write and the handlers commit together.

The native driver store never had the problem. It looks for an ambient `ClientSession` and, when it finds one, runs the append body once without opening a session, starting a transaction, or retrying. The two Spring stores were simply missing the equivalent check, so this is a gap between the four DCB stores rather than a new policy.

## Decision

A retry never spans a transaction its layer does not own, because only the code that began a transaction can begin a fresh one. Each layer therefore either owns the transaction and retries, or participates in someone else's and runs once.

The two Spring Mongo stores check for an active transaction before choosing. `SpringMongoEventStore` uses `TransactionSynchronizationManager.isActualTransactionActive()`, and `ReactorMongoEventStore` uses `ReactiveMongoDatabaseUtils.isTransactionActive(...)` to read the reactive context. When a transaction is already active they run the append body once and let the conflict reach whoever owns it.

The retry that the store gives up is picked up by the layer that does own the transaction. `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry a conflict around the transaction they open, and skip the retry when they are themselves joining a caller's transaction. That keeps the synchronous-subscription configuration retrying, one level up, where each attempt genuinely starts a fresh transaction.

When neither the store nor the executor owns the transaction, nothing in the library retries and the conflict reaches the application immediately. That is the intended outcome. The application has to retry at its own transaction boundary. The word-guessing-game example shows the shape: its use cases dropped a `@Transactional` that was doing nothing but disabling the store's retry, and added the transient type to their `@Retryable`, which works because retry advice sits outside transaction advice so each attempt gets a fresh transaction.

The rule is enforced through a single `retryOnlyWhenThisStoreOwnsTheTransaction` helper in each Spring store, and every retry on the write path goes through it, including the position counter's cold-start retry. Routing them through one place is deliberate: the counter retry was written without an ownership check and is exactly the kind of site that gets added again later.

The two Spring stacks change together, so the retry parity recorded in [ADR 53](0053-dcb-api-freeze-consistency.md) still holds between them. The in-memory store needs none of this because it has no transactions. The native driver is consistent in shape but not in policy, and that is accepted rather than corrected here: `NativeMongoTransactionExecutor` runs its work through `ClientSession.withTransaction`, so the driver itself re-runs the body on a `TransientTransactionError` until its own deadline rather than for a bounded number of attempts, and it does not treat a duplicate key as retryable the way the Spring stores and executors do.

## Consequences

Retrying at the caller's own transaction boundary is not merely the recommended remedy, it is the only one that works. A participating `TransactionTemplate.execute` that throws marks the surrounding transaction rollback-only, so an application that catches the conflict and carries on inside the same transaction gets `UnexpectedRollbackException` at commit instead of the partial success it expected.

The executors match `DataIntegrityViolationException` as well as `TransientDataAccessException`. These are Spring's translated types, used because the executor modules are storage-neutral and cannot inspect a driver error label the way the stores do. MongoDB's WriteConflict makes this necessary rather than merely convenient: the server labels it `TransientTransactionError`, yet Spring translates it to `DataIntegrityViolationException`, which is a non-transient type, so a predicate built only on `TransientDataAccessException` would miss the most common conflict there is. The cost is that a genuine integrity violation, say a synchronous subscription handler hitting a unique index, is retried too. That is wasteful rather than wrong, since it fails the same way every attempt and then propagates.

Position reservation behaves differently depending on ownership, and the comments in both stores now say so. [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) describes the counter `findAndModify` as running outside the append transaction, which holds only while the store owns that transaction. When the store joins a caller's transaction the counter update joins it too, so one shared document becomes a conflict point for every concurrent append in that transaction, even for appends to disjoint boundaries. In practice a nested append often loses on the counter before it ever reaches the append body, which is worth knowing when reading a failure. A test measuring this saw only one of six contended appends reach the body at all.

The retry the executors apply covers the whole unit of work, which for a synchronous-subscription setup includes the handlers. A handler with a side effect outside the transaction, an HTTP call say, can therefore run more than once for one command. That was already true of the application service's existing append-condition retry, so it is not a new class of behaviour, but it now has a second trigger. The two retries do not compound, because their predicates are disjoint: the executor matches only data-access conflicts and the service only `DcbAppendConditionNotFulfilledException`.

A conflict inside a caller-owned transaction now fails immediately instead of after roughly 5 seconds of attempts that could never have committed, so the failure is both faster and easier to attribute.
