# 163. A Spring Mongo transaction manager sends a commit again when its result is unknown

Date: 2026-10-30

## Status

Accepted. Resolves [#1384](https://github.com/johanhaleby/occurrent/issues/1384).

## Context

MongoDB marks a commit it could not answer with the error label `UnknownTransactionCommitResult`. The commit reached
the server, or may have, and the reply was lost, for example to a network error or to a primary that stepped down. The
transaction is then committed or not, and the MongoDB documentation says to send `commitTransaction` again. The driver
accepts that on the same session.

The native `MongoEventStore` runs its transaction through `ClientSession.withTransaction`, which does so for up to 120
seconds. `SpringMongoEventStore` and `ReactorMongoEventStore` run it through Spring's `MongoTransactionManager` and
`ReactiveMongoTransactionManager`, and neither sends a commit again. Spring turns the failure into a
`TransactionSystemException` that holds the `MongoException` with its label, and no code in either store looks at it.
The stores' own retry looks for the `TransientTransactionError` label and for a duplicate key, not for this label.

An append that may have been saved is therefore reported to the caller as a failure. If the caller sends it again with
new event ids, the events can be written twice. If it sends the same ids, the unique `(id, source)` index refuses them
as a `DuplicateCloudEventException`, for events that are stored.

## Decision

Occurrent ships `CommitRetryingMongoTransactionManager` and `CommitRetryingReactiveMongoTransactionManager`. Each
extends Spring's manager and overrides the protected `doCommit`, which Spring documents as the place to retry a commit.
The retry follows the driver's `withTransaction`:

- It retries a commit that fails with the `UnknownTransactionCommitResult` label, found on the first `MongoException`
  in the cause chain of the failure.
- It does not retry a top-level `MaxTimeMSExpired` error (`MongoExecutionTimeoutException`) or
  `MongoOperationTimeoutException`, which the driver does not retry either. It does retry a `MongoWriteConcernException`
  with the `MaxTimeMSExpired` code (50) when it has the label, as the driver does.
- It stops retrying 120 seconds after the first commit was sent, and throws or emits the last error. The number is the
  driver's `MAX_RETRY_TIME_LIMIT_MS`, but the driver counts it from the start of its `withTransaction` call, before the
  transaction body runs, so the driver's window can be shorter than ours by the time the body takes. The caller sees the same failure it sees today, and the append may or may not be saved.
- It checks the time between attempts, as the driver does, so a commit that has started is not interrupted and ends on
  the driver's own timeouts.
- It sends the next commit at once, as the driver does. The second `commitTransaction` call on the same session is the
  one that makes the driver send the commit with a majority write concern, so the resend is not acknowledged by the
  primary alone, whatever write concern the transaction started with.

Both Spring Boot starters create these managers instead of the plain ones. They still back off when the application
defines its own `MongoTransactionManager` or `ReactiveMongoTransactionManager`, and the blocking one still passes the
majority read and write concern as transaction options.

A `SpringMongoEventStore` whose transaction manager is not a `CommitRetryingMongoTransactionManager` logs one warning
when it is created. The reactive store's `EventStoreConfig.Builder.transactionConfig(ReactiveTransactionManager)` does
the same. `transactionConfig(TransactionalOperator)` can't, because a `TransactionalOperator` does not expose the
manager it came from, so an application that passes an operator gets no warning. The migration guide says so.

The store does not retry the commit itself. By the time Spring has translated the failure, the store would have to run
the whole append again, and an append that was in fact saved would be reported as a failure, because the same event
ids are refused by the unique `(id, source)` index. Retrying only the commit is correct because the transaction is
already complete on the session.

Running the transaction through `ClientSession.withTransaction` was considered and rejected. It bypasses the
`TransactionTemplate` the user configured, and the reactive driver has no such method.

## Consequences

- A commit whose result is unknown is sent again for up to two minutes. Its append is reported as written when the
  second commit succeeds, and it is written once.
- An application that wires the store by hand must switch its transaction manager to get the retry. The warning
  tells it so, and the migration guide to 0.35.0 shows the change. The examples in this repository were switched.
- When the store joins a transaction the caller started, the commit belongs to the caller's manager, so the retry
  happens only if that manager is one of these two. The tests cover both a retrying and a plain manager for that case.
- The two classes are public API in `eventstore-mongodb-spring-blocking` and `eventstore-mongodb-spring-reactor`. The
  decision of what counts as retryable lives in an internal class in `eventstore-mongodb-common`, so the two stacks
  cannot drift apart.
