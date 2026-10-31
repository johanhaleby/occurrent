# 165. The retry sees a failure after the write only in a transaction the attempt opened

Date: 2026-10-30

## Status

Accepted. Changes what [ADR 57](0057-synchronous-subscriptions.md) and
[ADR 74](0074-retry-only-where-the-transaction-is-owned.md) say about a handler running again. Both have shipped in a
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

A failure after a successful write, in the re-read of the written events or in a handler, reaches the service's retry
strategy only when `isTransactional()` answers `false` before the service enters the executor and `true` inside it, before
the write. The service takes those two answers to mean the attempt opened a transaction that undoes the write. Otherwise
the failure reaches the caller without the retry strategy seeing it.

- **It's an inference.** The service can't ask who owns the transaction or see whether the store joined it. Consequences
  lists where the inference is wrong. A first answer of `true` means a transaction is already open, so the attempt counts as
  joining it.
- **What the retry strategy still sees.** A failure of the write itself, which normally means nothing was written. Whether that
  retries is up to the strategy's predicate.
- **How the failure stays away from the retry.** One object per `execute`, or per subscription on the reactive stack, holds
  the failure, and a private stand-in exception passes through the executor so a transaction the attempt opened still rolls back. Once a
  failure is held, whatever comes out of the executor means an empty attempt, the action refuses to write again, and the
  original failure is thrown after the retry returns. So a predicate that matches every exception, an executor that
  translates what passes through it and an executor with its own retry can't run the command again. An exception that
  replaces the stand-in on the way out, a failed rollback say, stays with the failure as a suppressed exception.
- **`NativeMongoTransactionExecutor`.** `isTransactional()` now answers whether the session bound to the thread has a
  transaction in force instead of always `true`, so the service can tell a transaction it would open from one it would join.
- **The side effect.** It doesn't run, the same as for a handler failure that was never retried. It's for a command that
  succeeded, and the caller is about to get an exception.

## Alternatives

**Add a method to the executor that says whether this call owns the transaction.** It's the direct question, and it would
get `PROPAGATION_REQUIRES_NEW` and a fixed `true` right. It adds public API in 0.35.0, and it can't help with the two cases
where the store does something the executor can't see. Those need the same inference. An executor can get that method later
without breaking anyone.

**Hold the failure inside the executor and return normally.** It keeps the code in one place, but the executor then sees
success. An executor that opens a transaction and doesn't override `isTransactional()`, which answers `false` by default,
would commit a write it rolls back today.

**Dispatch after the executor returns when it reports no transaction.** That same executor would commit before the handlers
run, where a failing handler rolls the write back today.

**Wrap the failure in a type and catch that type outside the executor.** It's what I built first. An executor that
translates what passes through it turns the wrapper into another exception that reaches the retry, and an executor that
retries what passes through it writes again before the catch runs. Both ran the command twice in a probe.

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
| `SpringTransactionExecutor`, `REQUIRED` | the caller's, on another transaction manager | catches it and commits | the commit succeeds | 0 on the stream service, 1 on the DCB service |
| `NativeMongoTransactionExecutor` | the caller's | catches it and commits | the commit succeeds | 1 |
| Spring, `REQUIRES_NEW` or `NOT_SUPPORTED` | the caller's | catches it and commits | the commit succeeds | 0 |
| Spring, `NOT_SUPPORTED`, `SUPPORTS` or `NEVER` | none | | gets the failure | 0 |

The Spring MongoDB stores set `SessionSynchronization.ALWAYS` themselves, so with no transaction reported the store still
writes in a synchronization-based transaction that rolls back. In the last two rows a strategy that matched the failure used
to run the command again and store one event with no exception. That retry is now withheld, so those rows are a regression
that loses a retry rather than an event. By reading the code at the tag, so is the row with another transaction manager on the stream service.
When `isTransactional()` answers `true` inside the executor, the dispatch stops at the first failure, so in a row with a
transaction that stores 1, the handlers behind the failing one don't get the events.
I didn't key on `isSynchronizationActive`, since a store that ignores the
synchronization doesn't roll back under it, and the service can't see the store.

The answers are also wrong in two other ways:

- **An executor that answers a fixed `true`, and one entered under an open transaction, even on another transaction manager
  or on a propagation that opens another.** `isActualTransactionActive` covers the whole thread, so the first answer is `true`, so they count as joining and the retry is withheld, which can't write twice. Answering for the moment of the
  call gets the retry back for the first one only.
- **A store that doesn't join the executor's transaction, or a rollback rule that doesn't cover the failure.** The answers
  are `false` then `true`, nothing undoes the write, and a strategy that matches the failure writes again. The native
  MongoDB store under `SpringTransactionExecutor` did that and reported success.

An executor that opens a transaction and doesn't override `isTransactional()` still rolls back when its rollback rule
covers any `RuntimeException`. A rule keyed on the handler's exception type sees the stand-in instead. The command doesn't
run again, because the service can't tell a rollback from no transaction, and that includes a retry inside the executor,
such as the one in `ClientSession.withTransaction`, for a failure after the write. Overriding `isTransactional()` gets both
back.

The service now asks `isTransactional()` outside any transaction. An executor whose answer fails there makes an `execute`
with synchronous subscriptions fail before the write.

A failure in or after the commit of an owned transaction isn't held, so a predicate that matches it can still repeat a
write that committed. A failure the executor raises itself, after the action returned or as a timeout around it, isn't held
either, with or without a transaction, and a predicate that matches it runs the command again. A command can also still run twice when the write
itself reports a failure after committing, as a lost acknowledgement does, and the retry strategy retries on it. The service
can't tell any of these from a rollback, so this ADR doesn't change them.

The holder lasts one `execute` or one subscription. A caller that retries around `execute` or subscribes to the returned
`Mono` again runs the command again, and so does an outer service's retry around a service called from its domain function.

A retry strategy that logs through `onError` hears nothing about a held failure.

A handler can still run more than once for one command under a transaction it owns, and one with a side effect outside the
transaction, an HTTP call say, still has to tolerate that.
