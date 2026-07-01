# 41. Native MongoDB subscription model restart-on-error

Date: 2026-07-01

## Status

Accepted

## Context

`SpringMongoSubscriptionModel` is the production-hardened MongoDB change-stream subscription model: it survives replica-set primary elections/failovers, transient network errors, and change-stream history loss by classifying the underlying error and restarting the subscription rather than letting it die. `NativeMongoSubscriptionModel`, the equivalent built on the plain MongoDB driver, had none of this. Its change-stream consumption loop caught `MongoException` and `IllegalStateException` and only ever logged them at DEBUG and let the subscription's thread end, silently, with no restart. Any operational MongoDB event that the driver itself could not transparently resume (a longer failover, a killed cursor, a non-resumable error) permanently killed the subscription with no visible signal beyond a debug log line.

The native model already had per-event action retry (`RetryStrategy`), a running/paused subscription lifecycle, and `waitUntilStarted`, all wired through `executeWithRetry(internalSubscription, NOT_SHUTDOWN, retryStrategy)` in `startSubscription`. That existing retry wrapper was already being triggered by accident for any exception type it didn't explicitly catch (i.e. anything other than `MongoException`/`IllegalStateException`), always restarting from the subscription's original `StartAt`. The fix is to route the errors that matter through that same wrapper deliberately, with the right classification and the right restart position, rather than swallowing them.

Also, `resumeSubscription` reused the subscription's original `StartAt` verbatim. For a subscription started at a fixed position, this replays every event since that position on every resume. For one started at `StartAt.now()`/`StartAt.subscriptionModelDefault()` (which the shared `MongoCommons.applyStartPosition` resolves as "no explicit position, let the driver default to the current time when the cursor opens"), reusing the original marker on resume instead skips every event written while the subscription was paused, since the newly opened cursor starts from "now" at resume time. Either way, resume did not continue from where the subscription actually left off.

## Decision

Track the position of the last event actually delivered from the change stream in an `AtomicReference<StartAt>` (`currentStartAt`), shared between the `Runnable` passed to `executeWithRetry` and every subsequent invocation it makes of `newInternalSubscription`. It starts at the caller-supplied `StartAt` and is updated to `StartAt.subscriptionPosition(new MongoResumeTokenSubscriptionPosition(...))` after each document is read from the cursor, matched or not.

Widen the try block in `newInternalSubscription` to cover opening the cursor (`eventCollection.watch(...)` and `.cursor()`), not just iterating it, since a change-stream error can surface at either point. In the catch block, classify the throwable and decide, in order:

1. The subscription was intentionally closed (a new `intentionallyClosed` flag on `InternalSubscription`, set before `cursor.close()`) or the exception matches the pre-existing "cursor is not longer open" heuristic: benign, log at DEBUG, do not restart. This is the pause/cancel/shutdown path.
2. `MongoCommandException` with error code 286 (`ChangeStreamHistoryLost`, via the existing `MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE` constant): if `NativeMongoSubscriptionModelConfig.restartSubscriptionsOnChangeStreamHistoryLost` is `true` (default `false`, matching the Spring default), set `currentStartAt` to `StartAt.now()` and rethrow; otherwise log at ERROR and do not restart.
3. The model itself is shutting down: benign, log at DEBUG, do not restart.
4. Anything else: log at WARN and rethrow.

A rethrow is caught by the already-installed `executeWithRetry(internalSubscription, NOT_SHUTDOWN, retryStrategy)` wrapper in `startSubscription`, which retries with the configured backoff and re-invokes `newInternalSubscription`, which reads the now-updated `currentStartAt`. No new threading or restart mechanism was needed; the existing retry-Runnable already does exactly what a restart needs, once the classification stops swallowing the exceptions that reach it.

`resumeSubscription` now reuses the same `currentStartAt` reference the paused `InternalSubscription` was tracking, instead of a fixed original `StartAt`, so resume continues from the position of the last event delivered before the pause.

Add `NativeMongoSubscriptionModelConfig`, mirroring `SpringMongoSubscriptionModelConfig`'s builder shape, carrying `retryStrategy` (default: exponential backoff 100ms to 2s, matching the existing default) and `restartSubscriptionsOnChangeStreamHistoryLost` (default `false`). It does not bundle the collection name or time representation the way the Spring config does, since the native constructors already take those positionally; the new config only adds the two new toggles, via a new constructor overload alongside the existing ones.

## Consequences

- `NativeMongoSubscriptionModel` survives the same class of MongoDB operational disruption `SpringMongoSubscriptionModel` does: failovers, transient network errors, and (opt-in) change-stream history loss, recovering gap-free from the position of the last event it actually delivered rather than replaying or skipping.
- `resumeSubscription` no longer replays (fixed `StartAt`) or drops events written during the pause window (`StartAt.now()`/default); it continues from where the subscription left off.
- The restart-on-error behavior reuses the model's existing per-event `RetryStrategy` and backoff rather than introducing a second retry mechanism, keeping the change small and avoiding a parallel "restart in a new thread" scheme like the one `SpringMongoSubscriptionModel` needs (native does not need it, since its retry wrapper already runs on a dedicated per-subscription thread).
- Existing constructors are unchanged; the config-object constructor is additive.
- Verified against real MongoDB (a replica-set Testcontainer) by injecting the exact exception types a failover, a transient socket error, and a 286 history-lost response would surface, rather than orchestrating a live replica-set election. `SpringMongoSubscriptionModelTest`, the reference this behavior mirrors, uses the identical injection technique; a live multi-node failover test was considered and rejected as disproportionate infrastructure for the coverage it would add over the deterministic injection tests.
