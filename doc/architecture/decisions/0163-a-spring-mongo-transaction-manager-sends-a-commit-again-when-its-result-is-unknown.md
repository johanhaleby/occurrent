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

- It retries a commit that fails with the `UnknownTransactionCommitResult` label, found on the exception or on one of
  its causes.
- It does not retry `MongoExecutionTimeoutException` (`MaxTimeMSExpired`) or `MongoOperationTimeoutException`, which the
  driver does not retry either.
- It stops retrying 120 seconds after the first commit was sent, the driver's `MAX_RETRY_TIME_LIMIT_MS`, and throws or
  emits the last error. The caller sees the same failure it sees today, and the append may or may not be saved.
- It sends the next commit at once, as the driver does.

Both Spring Boot starters create these managers instead of the plain ones. They still back off when the application
defines its own `MongoTransactionManager` or `ReactiveMongoTransactionManager`, and the blocking one still passes the
majority read and write concern as transaction options.

A `SpringMongoEventStore` whose transaction manager is not a `CommitRetryingMongoTransactionManager` logs one warning
when it is created. The reactive store's `EventStoreConfig.Builder.transactionConfig(ReactiveTransactionManager)` does
the same. `transactionConfig(TransactionalOperator)` can't, because a `TransactionalOperator` does not expose the
manager it came from, so an application that passes an operator gets no warning, and the migration guide tells it to
build the operator on the new manager.

The store does not retry the commit itself. By the time Spring has translated the failure, the store would have to run
the whole append again, and an append that was in fact saved would be written twice. Retrying only the commit is
correct because the transaction is already complete on the session.

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
