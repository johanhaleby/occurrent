# 164. A Spring Mongo DCB append retries a transaction conflict for about two minutes

Date: 2026-10-30

## Status

Accepted. Amends [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md), [ADR 74](0074-retry-only-where-the-transaction-is-owned.md)
and [ADR 80](0080-concurrent-write-conformance.md), which have shipped in a release and are therefore corrected here and not edited.
ADR 74 says the executors match `DataIntegrityViolationException` because they cannot inspect a driver error label, that retrying a genuine
integrity violation is wasteful rather than wrong, and that the Spring executors treat a duplicate key as retryable. The Decision below corrects all three for the executors. The stores keep
retrying a duplicate key of their own. Resolves [#1379](https://github.com/johanhaleby/occurrent/issues/1379).

## Context

ADR 21 lists, among its positive consequences, "Contention surfaces as a retry, not a spurious command failure." That
holds only until the retry runs out. `SpringMongoEventStore` and `ReactorMongoEventStore` retried a DCB append that
lost a transaction conflict 15 times, which is about 4.6 seconds of waiting with the backoff capped at 500 ms. When
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
takes comes on top of the waiting, and also once 120 seconds have passed since the first attempt began, which is after the position block was reserved. The deadline is there
because the driver labels a failure to select a server in an active transaction as transient too. With no primary an
attempt can take about 30 seconds, and 240 of them would be two hours.

- `SpringMongoEventStore` makes 240 attempts in total with the backoff it already had, 10 ms doubling up to 500 ms.
- `ReactorMongoEventStore` makes 320 retries, which is 321 attempts in all, with the same backoff. Reactor's default
  jitter makes each wait between 250 and 500 ms once the backoff reaches its cap, 375 ms on average, so 320 retries
  add up to about as much waiting as 240 attempts do on the blocking store.
- The default `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` get the same budget and the same
  deadline, and they retry on the error label and not on `DataIntegrityViolationException`. See the next paragraph. With synchronous subscriptions and the default propagation the executor owns the transaction, the store joins it and
  retries nothing, so the executor's retry is the whole budget there. An executor built on a template or operator
  that opens no transaction means the store retries as well. A retry passed to an executor's constructor is used as given.
- `ReactorMongoEventStore` retries a lost `any()` version race 320 times too. See Consequences.
- The default executors retry a `TransientDataAccessException`, or any exception that has the `TransientTransactionError`
  label anywhere in its cause chain. The label is what MongoDB puts on an error that a new transaction can get past,
  and it is what the driver's own `withTransaction` retries on. ADR 74 matched `DataIntegrityViolationException` instead,
  because Spring translates a WriteConflict to it and the executor modules did not look at the driver. That also matched a
  duplicate key and a validation failure, which have no label. The two executor modules now depend on
  `mongodb-driver-core` as an optional dependency and look for the label only when `com.mongodb.MongoException` is on the
  classpath, so an application without the driver still gets the executor and still retries a `TransientDataAccessException`.
  A commit that fails with the `UnknownTransactionCommitResult` label is not retried even though a commit failure
  can have both labels, since the commit may have gone through and running the unit of work again could apply it twice.
- There is no setting for the count or the deadline and no extra jitter. A user who needs another budget can raise it
  when a need shows up.
- The retry still runs only when the store owns the transaction, as in [ADR 74](0074-retry-only-where-the-transaction-is-owned.md).

Moving the stores to `ClientSession.withTransaction` was considered and rejected. It would bypass the
`TransactionTemplate` the user configured, and the reactive driver has no such method.

## Consequences

- ADR 21's sentence is true with a limit. An append that contends on a marker document gets up to 240 attempts on the
  blocking store and 321 on the reactive one, which is about two minutes of waiting. A caller that loses every
  attempt still gets the last failure. In the case #1379 measured that was a `DataIntegrityViolationException`, and
  `GenericDcbApplicationService` does not retry that exception.
- A command that used to fail after about 5 seconds under heavy contention now waits about two minutes in all, across
  attempts that each wait 10 to 500 ms, before it fails, plus the time the attempts themselves take. A command that
  `GenericDcbApplicationService` runs again after a failed append condition can spend that time on each of its runs. No retry is scheduled once 120 seconds have
  passed since the first attempt began, but one scheduled just before then still starts after its backoff of up to
  500 ms, and an attempt that is already running is not interrupted and ends on the driver's own timeouts, as it does
  in a native `withTransaction` call. The native store's 120 seconds applies to each `withTransaction` call, and its append runs that
  call inside a retry of 15 attempts, so the native store can wait longer overall. A caller with a shorter deadline of
  its own sees that deadline first.
- A duplicate key or a validation failure inside the transaction now runs the unit of work once and reaches the caller,
  as against the 214 runs in 120 seconds that a duplicate key got under the old predicate in a measurement on MongoDB 8.0.
  One case that was retried before is not any more. It is a duplicate from a write made outside the transaction, for
  example through a template that does not join it. MongoDB labels no such error, so a unit of work that checks, loses
  a race to a rival and then gets the duplicate used to succeed on its second run, and now fails with the duplicate. A
  caller that relied on it handles the duplicate itself, or does the write through a template that joins the transaction.
- A commit that fails with `UnknownTransactionCommitResult` is also not retried. The old predicate did not retry it
  either, since a commit failure surfaces as a `MongoException` and not as a `DataAccessException`.
- The budget of 240 attempts and 120 seconds was chosen when a retry could be spent on an error no retry could cure. Now
  that only a conflict uses it, I measured how much a conflict needs, on a single-node MongoDB 8.0 with a
  `TransientTransactionError` predicate. 64 threads appending 10 times each to one marker needed at most 21 attempts, and 256
  threads appending 3 times each needed at most 36, in runs that took 46 seconds. None of the 768 commands in the larger run
  failed. The budget is about seven times the worst case seen, so it stays. The measurement does not cover a stuck rival
  transaction, which can hold a document for up to a minute under the server's default transaction lifetime, and the 120
  second budget is what rides that out.
- A reserved position block is still abandoned when an append exhausts its retries, so `dcbposition` can still have
  gaps, as ADR 21 says.
- The reactive stream write retries a lost version race for an append with `StreamVersionWriteCondition.any()`. That is a
  different condition from the one above, and it had the same 15-retry limit. 64 writers appending 20 unconditional
  events each to one stream lost 7 of 1280 writes to it on a single-node MongoDB 8.0, where the blocking stores retry
  with no limit at all. It now makes 320 retries too, which lost none in the same test. [ADR 80](0080-concurrent-write-conformance.md)
  describes the 15, and is corrected here. This retry has no deadline of its own, and the blocking stores' retry of the
  same race has no limit at all.
