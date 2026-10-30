# 164. A Spring Mongo DCB append retries a transaction conflict for about two minutes

Date: 2026-10-30

## Status

Accepted. Amends [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) and [ADR 80](0080-concurrent-write-conformance.md),
which have shipped in a release and are therefore corrected here and not edited. Resolves [#1379](https://github.com/johanhaleby/occurrent/issues/1379).

## Context

ADR 21 lists, among its positive consequences, "Contention surfaces as a retry, not a spurious command failure." That
holds only until the retry runs out. `SpringMongoEventStore` and `ReactorMongoEventStore` retried a DCB append that
lost a transaction conflict 15 times in all, which is about 4.6 seconds of waiting with the backoff capped at 500 ms. When
many appends write the same marker document, the last of them can lose to every other one, and then the 15th failure
reached the caller as the command's failure. #1379 measured that on a loaded single-node MongoDB 8.0: 69 of 1038
commands failed with 64 threads on one type marker. Each failure took about 5 seconds.

The native `MongoEventStore` did not fail in the same test. It runs the append through the driver's
`ClientSession.withTransaction`, which keeps retrying a transient transaction error for up to 120 seconds a call. The Spring
stores run the transaction through Spring's transaction manager, which retries nothing, so their own attempts were the
whole budget.

## Decision

Both Spring stores retry a transient transaction conflict for about two minutes of waiting, the 120 seconds that one
`withTransaction` call gives the native store. The retry stops after a number of attempts, so the time each attempt
takes comes on top of the waiting, and also after a deadline of 120 seconds from the start of the append. The deadline is there
because the driver labels a failure to select a server in an active transaction as transient too. With no primary an
attempt can take about 30 seconds, and 240 of them would be two hours.

- `SpringMongoEventStore` makes 240 attempts in total with the backoff it already had, 10 ms doubling up to 500 ms.
- `ReactorMongoEventStore` makes 320 retries, which is 321 attempts in all, with the same backoff. Reactor's default
  jitter makes each wait between 250 and 500 ms once the backoff reaches its cap, 375 ms on average, so 320 retries
  add up to about as much waiting as 240 attempts do on the blocking store.
- The default `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` get the same budget and the same
  deadline. With synchronous subscriptions the executor owns the transaction, the store joins it and retries nothing, so
  the executor's retry is the whole budget there. A retry passed to an executor's constructor is used as given.
- `ReactorMongoEventStore` retries a lost `any()` version race 320 times too. See Consequences.
- There is no setting for the count or the deadline and no extra jitter. A user who needs another budget can raise it
  when a need shows up.
- The retry still runs only when the store owns the transaction, as in [ADR 74](0074-retry-only-where-the-transaction-is-owned.md).

Moving the stores to `ClientSession.withTransaction` was considered and rejected. It would bypass the
`TransactionTemplate` the user configured, and the reactive driver has no such method.

## Consequences

- ADR 21's sentence is true with a limit. An append that contends on a marker document gets 240 attempts on the
  blocking store and 321 on the reactive one, which is about two minutes of waiting. A caller that loses every
  attempt still gets the last failure. In the case #1379 measured that was a `DataIntegrityViolationException`, and
  `GenericDcbApplicationService` does not retry that exception.
- A command that used to fail after about 5 seconds under heavy contention now waits about two minutes between its
  attempts before it fails, plus the time the attempts themselves take. No new attempt starts once 120 seconds have
  passed since the append started, but an attempt that is already running is not interrupted and ends on the driver's
  own timeouts, as it does in a native `withTransaction` call. The native store's 120 seconds applies to each `withTransaction` call, and its append runs that
  call inside a retry of 15 attempts, so the native store can wait longer overall. A caller with a shorter deadline of
  its own sees that deadline first.
- A genuine integrity violation, such as a duplicate key that MongoDB does not label transient, is retried like a
  conflict by the default executors, because Spring translates a WriteConflict to the same exception type. It fails
  the same way every attempt, so the transaction callback runs again for up to two minutes and any side effect it
  has outside the transaction repeats. Narrowing the predicate would miss an unlabelled collision that a retry does
  resolve, and ADR 74 already chose it.
- A reserved position block is still abandoned when an append exhausts its retries, so `dcbposition` can still have
  gaps, as ADR 21 says.
- The reactive stream write retries a lost version race for an append with `StreamVersionWriteCondition.any()`. That is a
  different condition from the one above, and it had the same 15-retry limit. 64 writers appending 20 unconditional
  events each to one stream lost 7 of 1280 writes to it on a single-node MongoDB 8.0, where the blocking stores retry
  with no limit at all. It now makes 320 retries too, which lost none in the same test. [ADR 80](0080-concurrent-write-conformance.md)
  describes the 15, and is corrected here.
