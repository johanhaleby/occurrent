# 164. A Spring Mongo DCB append retries a transaction conflict for about two minutes

Date: 2026-10-30

## Status

Accepted. Amends [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md), which has shipped in a release and is
therefore corrected here and not edited. Resolves [#1379](https://github.com/johanhaleby/occurrent/issues/1379).

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
`withTransaction` call gives the native store. The limit is a number of attempts and not a deadline, so the time each
attempt takes comes on top of the waiting.

- `SpringMongoEventStore` makes 240 attempts with the backoff it already had, 10 ms doubling up to 500 ms.
- `ReactorMongoEventStore` makes 320 attempts with the same backoff. Reactor's default jitter makes each wait between
  250 and 500 ms once the backoff reaches its cap, 375 ms on average, so 320 attempts add up to about as much waiting
  as 240 do on the blocking store.
- There is no setting for the count and no extra jitter. A user who needs another budget can raise it when a need shows up.
- The retry still runs only when the store owns the transaction, as in [ADR 74](0074-retry-only-where-the-transaction-is-owned.md).

Moving the stores to `ClientSession.withTransaction` was considered and rejected. It would bypass the
`TransactionTemplate` the user configured, and the reactive driver has no such method.

## Consequences

- ADR 21's sentence is true with a limit. An append that contends on a marker document is retried 240 times on the
  blocking store and 320 times on the reactive one, which is about two minutes of waiting. A caller that loses every
  attempt still gets the last failure. In the case #1379 measured that was a `DataIntegrityViolationException`, and
  `GenericDcbApplicationService` does not retry that exception.
- A command that used to fail after about 5 seconds under heavy contention now waits about two minutes between its
  attempts before it fails, plus the time the attempts themselves take. The native store's 120 seconds applies to
  each `withTransaction` call, and its append runs that call inside a retry of 15 attempts, so neither store has a fixed
  time limit. A caller with a shorter deadline of its own sees that deadline first.
- A reserved position block is still abandoned when an append exhausts its retries, so `dcbposition` can still have
  gaps, as ADR 21 says.
- The reactive stream write retries only a lost version race, 15 times, for an append with
  `StreamVersionWriteCondition.any()`. That retry is a different condition from the one above and is unchanged.
