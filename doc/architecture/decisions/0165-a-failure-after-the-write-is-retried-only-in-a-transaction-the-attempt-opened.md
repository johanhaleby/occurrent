# 165. A failure after the write is retried only in a transaction the attempt opened

Date: 2026-10-30

## Status

Accepted. Changes what [ADR 57](0057-synchronous-subscriptions.md) and
[ADR 74](0074-retry-only-where-the-transaction-is-owned.md) say about a handler running again, and what ADR 57 says about
the handlers behind a failing one in a transaction the attempt joined. Both have shipped in a
release, so they get a pointer here rather than an edit.

## Context

With `noTransaction()` and no transaction of the caller's, a synchronous subscription runs after the write has committed. ADR 57 accepts that a handler that
throws surfaces as an `execute` failure even though the events persist.

That failure didn't surface when the service's retry matched it. The write and the dispatch both ran inside the retry, so a
handler that threw `WriteConditionNotFulfilledException` or `DcbAppendConditionNotFulfilledException` once made the default
retry read, decide and write again. The events were stored twice and the caller got no exception. A custom predicate that
matches `RuntimeException` did the same for any handler failure and for a failed re-read of the written events.

I ran the handler case on all four services, blocking and reactive, stream and DCB, on the in-memory store on the blocking
stack and on the MongoDB stores, and it does the same at the `occurrent-0.34.0` tag.

A transaction the executor only joins has the same hole, since no rollback has happened when the retry runs. On real
MongoDB, a session the caller bound for `NativeMongoTransactionExecutor` and a service nested in another service's handler
both stored the events twice. So did a caller's transaction on an executor with the joined branch of
`SpringReactiveTransactionExecutor`. An executor with the joined branch of `SpringTransactionExecutor` marked the
transaction rollback-only, so the second attempt decided on the first attempt's uncommitted event and the caller got
`UnexpectedRollbackException`. I ran these on the commit before this decision and not at the tag.

ADR 74 says the executors' retry covers the handlers, and that this "was already true of the application service's
existing append-condition retry". Both are right when the attempt's own transaction rolled it back, and it gives the
reason itself, that a retry only works where it owns the transaction.

## Decision

A failure after a successful write, in the re-read of the written events or in a handler, can make the service's
retry strategy run the command again only when the attempt opened a transaction, which the service takes to mean that
transaction undoes the write. Otherwise the retry strategy doesn't run the command again, and `execute` fails.

- **Who says the attempt opened it.** The service runs the attempt through `inTransactionTellingIfOpened`, a new default
  method on `TransactionExecutor` and `ReactiveTransactionExecutor`, which tells the action whether this call opened the
  transaction it runs in. The two Spring executors answer from `status.isNewTransaction()`. The default infers it from
  `isTransactional()` answering `false` before the executor and `true` inside it, so a first answer of `true` counts as
  joining. `NativeMongoTransactionExecutor` keeps the default, since it joins a transaction already open on the thread.
- **It's still an inference about the write.** The service can't see whether the store joined the transaction. Consequences
  lists where that goes wrong.
- **What the retry strategy still sees.** A failure of the write itself, which normally means nothing was written. Whether that
  retries is up to the strategy's predicate.
- **How the retry is kept from running the command again.** One object per `execute`, or per subscription on the reactive stack, holds
  the failure, and a private stand-in exception passes through the executor so a transaction the attempt opened still rolls back. On
  the blocking stack, whatever comes out of the executor once a failure is held is replaced by the held failure, and the
  service passes `RetryStrategy.execute` a shutdown predicate that answers `false` while a failure is held. The strategy
  then doesn't ask its own predicate, doesn't run the command again, and gives the failure to `onError` and `mapError`, as a
  failure it doesn't retry. A strategy of your own that ignores the
  shutdown predicate gets an empty attempt from any later run, before it reads or writes, and the held failure is thrown
  after the strategy returns. When such a strategy throws instead, the caller gets what it throws. On the
  reactive stack, whatever comes out of the executor means an empty attempt, a later run returns an empty attempt, and the
  original failure is thrown after the retry returns. So on both stacks a predicate that matches every exception, an
  executor that translates what passes through it and an executor with its own retry can't run the command again. An exception that replaces the
  stand-in on the way out, a failed rollback say, stays with the failure as a suppressed exception, and so does an
  exception an executor attached to a wrapper of the stand-in.
- **`NativeMongoTransactionExecutor`.** `isTransactional()` now answers whether the session bound to the thread has a
  transaction in force instead of always `true`, so the service can tell a transaction it would open from one it would join.
- **Which handlers get the event when one fails.** The dispatch stops at the first failing handler only when the write
  can't commit after the failure, that is when the attempt opened the transaction, or when it joined one and
  `rollsBackAJoinedTransactionOnFailure()` answers `true`. That's a new default method on `TransactionExecutor` and
  `ReactiveTransactionExecutor`, and it answers `false`. `SpringTransactionExecutor` answers whether its transaction
  manager marks a transaction rollback-only when a participant fails, which `AbstractPlatformTransactionManager` does
  unless `setGlobalRollbackOnParticipationFailure(false)` was called. In 0.34.0 the dispatch stopped whenever
  `isTransactional()` answered `true` inside the executor. At the `occurrent-0.34.0` tag, a caller that caught the handler
  failure and committed a transaction `NativeMongoTransactionExecutor` had joined stored the event, and the handler behind
  the failing one never got it. `SpringReactiveTransactionExecutor` commits in the same case, as the table under
  Consequences shows.
- **The side effect.** It doesn't run, the same as for a handler failure that was never retried. It's for a command that
  succeeded, and the caller is about to get an exception.

## Alternatives

**Infer it from `isTransactional()` alone, with no new method.** It's what I built first, and it adds no public API. It
counted `PROPAGATION_REQUIRES_NEW` under a caller's transaction, and a caller's transaction on another transaction manager,
as joining, because `isActualTransactionActive` covers the whole thread. Those attempts roll back their own write, so they
lost a retry that was safe.

**Ask the store whether the write survived.** A prototype that called `isConfirmed` after the failure got the retry back
on `PROPAGATION_NOT_SUPPORTED`, `SUPPORTS` and `NEVER`, on the DCB service only, since the stream service has no such
call. A read that fails answers `false` too, so it would run a command again whose write is stored. I didn't run that
case or the native store. Since then a DCB append on those propagations stays when `execute` runs outside every Spring
transaction scope, as the table below shows, so the DCB service no longer has a rolled-back write there to retry.

**Hold the failure inside the executor and return normally.** It keeps the code in one place, but the executor then sees
success. An executor that opens a transaction and doesn't override `isTransactional()`, which answers `false` by default,
would commit a write it rolls back today.

**Dispatch after the executor returns when it reports no transaction.** That same executor would commit before the handlers
run, where a failing handler rolls the write back today.

**Wrap the failure in a type and catch that type outside the executor.** It's what I built first. An executor that
translates what passes through it turns the wrapper into another exception that reaches the retry, and an executor that
retries what passes through it writes again before the catch runs. Both ran the command twice in a probe.

**Keep the held failure out of the retry and throw it after the retry returns, on the blocking stack too.** It's the first
version of this decision. It skipped `onError`, `onAfterRetry` and `mapError`, so a strategy that maps every failure to an exception
of the application's own let a handler failure through unmapped, and `onAfterRetry` reported success for an attempt
that failed. `RetryStrategy.execute` already takes a shutdown predicate that the built-in strategies honour, so the
decision fits inside the retry.

**Make `rollsBackAJoinedTransactionOnFailure()` answer `true` by default.** It keeps the 0.34.0 behaviour for an executor
of your own that answers a fixed `true` from `isTransactional()`, and `NativeMongoTransactionExecutor` and
`SpringReactiveTransactionExecutor` would override it to `false`. An executor of your own that joins a transaction the
caller can still commit after a failure would keep losing the reactions behind the failing handler. With `false` the
worst case is handlers doing work that a rollback then discards.

**Mark the failure with a type and leave it to the predicate.** A predicate is the application's code, and one that
matches `RuntimeException` is a reasonable thing to write, so it would still run the command again.

## Consequences

`execute` now throws in a case where it used to run the command again and return normally, and a command that fails this
way doesn't write a second copy of its events. That changes released behaviour, so the changelog lists it under breaking
changes.

A transaction the attempt didn't open now gets the handler's failure straight away. What's stored by then depends on the
executor, its propagation and the store. I ran these on real MongoDB with a handler that fails once and a retry strategy
that matches the failure, on both stacks unless a row says otherwise. Every Spring row is over the Spring MongoDB stores.

| Executor and propagation | Transaction around `execute` | The caller | Outcome | Stored |
| --- | --- | --- | --- | --- |
| `noTransaction()` | none | | gets the failure | 1 |
| Spring, `REQUIRED` | the caller's | doesn't catch it | gets the failure | 0 |
| `SpringTransactionExecutor`, `REQUIRED` | the caller's | catches it and commits | `UnexpectedRollbackException` | 0 |
| `SpringReactiveTransactionExecutor`, `REQUIRED` | the caller's | catches it and commits | the commit succeeds | 1 |
| `NativeMongoTransactionExecutor` | the caller's | catches it and commits | the commit succeeds | 1 |
| Spring, `REQUIRES_NEW` | the caller's | | the command runs again, no exception | 1 |
| Spring, `REQUIRED` | the caller's, on another transaction manager | | the command runs again, no exception | 1 |
| Spring, `NOT_SUPPORTED`, `SUPPORTS` or `NEVER` | none, in a Spring scope of the caller's on `NOT_SUPPORTED` or `SUPPORTS` | | gets the failure | 0, blocking only |
| Spring, `NOT_SUPPORTED` | the caller's | catches it and commits | the commit succeeds | 0 |
| Spring, `NOT_SUPPORTED`, `SUPPORTS` or `NEVER` | none, and no Spring scope | | gets the failure | 0 on a stream, 1 on a DCB boundary |

The Spring MongoDB stores set `SessionSynchronization.ALWAYS` themselves, so with no transaction reported the store
still writes to a stream in a synchronization-based transaction that rolls back. In the last row a DCB append stays on
both stacks. Outside every Spring scope the Spring executors allow a read before them, so the DCB service reads before
its executor, and the store reserves positions outside every transaction, so nothing binds a session before the append
and the store commits it in a transaction of its own. I ran that after merging main at 4485a0462, and before that merge
the append rolled back too. In a Spring scope of the caller's that opens no transaction, the Spring executors don't
allow a read before them, so the DCB service reads in the executor as in 0.34.0, and a DCB append rolls back as a write
to a stream does. I ran that row on the blocking stack only, with a retry strategy that doesn't match the handler's
failure. In the last two rows, over a stream, a strategy that matched the failure used to run the command again and
store one event with no exception. That retry is now withheld, so those rows are a regression that loses a retry rather
than an event.

That lost retry costs more when a handler runs the command. A handler on a service with no transaction that writes to a
stream through a service in those rows loses its reaction. I ran that on both stacks over a stream, with the handler's
service on `PROPAGATION_NOT_SUPPORTED`. The first command's event is stored, the handler's write rolls back, and the
first `execute` throws. Nothing runs the handler's command again, since the first service's retry doesn't run the
command again for a failure after its write and a synchronous subscription has no replay. On the commit before this
decision the same setup stored both events and threw nothing. An executor that opens a transaction on the handler's
service keeps the retry.

The dispatch stops at the first failure when the attempt opened the transaction, or joined one with
`SpringTransactionExecutor` on the default transaction manager settings, and there the handlers behind the failing one
don't get the events. In the `SpringReactiveTransactionExecutor` and `NativeMongoTransactionExecutor` rows every handler
gets them.
I didn't key on `isSynchronizationActive`, since a store that ignores the
synchronization doesn't roll back under it, and the service can't see the store.

The service is also wrong in two other cases:

- **An executor of your own that answers a fixed `true`, or that opens a transaction with another already open, and doesn't
  override `inTransactionTellingIfOpened`.** The default counts both as joining and the retry is withheld, which can't write
  twice. Overriding the method gets the retry back.
- **A store that doesn't join the executor's transaction, or a rollback rule that doesn't cover the failure.** The attempt
  opened a transaction, nothing undoes the write, and a strategy that matches the failure writes again. The native MongoDB
  store under `SpringTransactionExecutor` did that and reported success.

An executor that opens a transaction and overrides neither `isTransactional()` nor `inTransactionTellingIfOpened` still
rolls back when its rollback rule covers any `RuntimeException`. A rule keyed on the handler's exception type sees the stand-in instead. The command doesn't
run again, because the service can't tell a rollback from no transaction, and that includes a retry inside the executor,
such as the one in `ClientSession.withTransaction`, for a failure after the write. Overriding either method gets both back.

The service now asks `isTransactional()` outside any transaction. An executor whose answer fails there makes an `execute`
with synchronous subscriptions fail before the write.

A failure in or after the commit of an owned transaction isn't held, so a predicate that matches it can still repeat a
write that committed. A failure the executor raises itself, after the action returned or as a timeout around it, isn't held
either, with or without a transaction, and a predicate that matches it runs the command again. A command can also still run twice when the write
itself reports a failure after committing, as a lost acknowledgement does, and the retry strategy retries on it. The service
can't tell any of these from a rollback, so this ADR doesn't change them.

The holder lasts one `execute` or one subscription. A caller that retries around `execute` or subscribes to the returned
`Mono` again runs the command again, and so does an outer service's retry around a service called from its domain function.

On the blocking stack, the `onError` listener of a strategy made by the factory methods of `RetryStrategy` hears a held failure and its `mapError` maps it, the same as for a failure its predicate doesn't match.

A handler can still run more than once for one command under a transaction it owns, and one with a side effect outside the
transaction, an HTTP call say, still has to tolerate that.
