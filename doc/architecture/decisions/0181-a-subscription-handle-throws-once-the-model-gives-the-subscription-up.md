# 181. A subscription handle throws once the model gives the subscription up, also after it answered that it started

Date: 2026-11-16

## Status

Accepted. Part of [#1502](https://github.com/johanhaleby/occurrent/issues/1502). Amends
[ADR 105](0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md), whose handle
keeps answering `true` once the subscription has started. Applies to the handles `NativeMongoSubscriptionModel` and
`SpringMongoSubscriptionModel` return, and to a `CompetingConsumerSubscriptionModel` handle over one of them, which
asks the handle it wraps.

## Context

ADR 105 decided that a `Subscription` handle reports on the one start it was created for. Once the subscription has
started, the handle keeps answering `true` from `waitUntilStarted(..)`, also after a pause, a stop or a lost lock race,
and `isRunning(id)` and `isPaused(id)` answer the present.

That rule assumed the subscription stays known to the model after it started. Two cases in 0.35.0 break that
assumption for the MongoDB change stream models:

* With `restartSubscriptionsOnChangeStreamHistoryLost` off, a loss of the change stream's history on a later read makes
  the model refuse the subscription for good. It removes the subscription, frees its id and tells the start refusal
  listeners. In 0.34.0 the model removed it the same way and the handle went on answering `true`, so an application
  that kept the handle saw a subscription that had started and was fine, while no node ran it.
* With restarts after a loss of history on, a restart from the present that gets no position to open at holds the
  subscription paused until a resume ([#1482](https://github.com/johanhaleby/occurrent/issues/1482)). The handle
  answered `true` there too, though nothing delivers until someone resumes the subscription.

In both cases the application has to act before the subscription delivers again, which is the case ADR 105 says a
handle reports as not started.

## Decision

**A handle answers for its subscription until the model gives the subscription up.**

* When the model refuses the subscription for good, the next `waitUntilStarted(..)` throws the refusal, also after an
  earlier call answered `true`, and so does every call after it. The refusal is an `UnsupportedStartAtException`, the
  same exception a refused start throws.
* When the model holds the subscription paused until a resume, each call throws the reason until a resume starts the
  subscription again.
* Every handle of the subscription answers so, the one `subscribe(..)` returned and each one a resume returned,
  whichever run it was returned for. A refusal or a hold of a later run reaches a handle a resume returned earlier, and
  a later run that starts ends a hold for all of them.
* A pause, a stop or a lost lock race still keeps the answer `true`, as ADR 105 decided, since the subscription stays
  known and the application or the model can start it again without changing it.

## Consequences

* A caller that waits on a kept handle, such as a readiness check that calls `waitUntilStarted(Duration.ZERO)`, gets an
  `UnsupportedStartAtException` where 0.34.0 answered `true`. It has to catch it and decide what to do, as it already
  had to for a refused start.
* The answer of a handle and the start refusal listeners now agree, so an application can learn about the refusal from
  either.
* A wrapper that hands out its own handle over one of these models gets the new answer by asking the wrapped handle.
  `CompetingConsumerSubscriptionModel` does, and it also records the refusal it is told about.

## Alternatives considered

* **Keep `true` and point users at `isRunning(id)` and the listeners.** Rejected, since the handle is what the blocking
  Spring Boot starter waits on and what an application that kept it asks, and a `true` for a subscription the model
  forgot reads as healthy.
* **Answer `false` instead of throwing.** Rejected, since `false` means "not yet" and a caller would wait again for a
  subscription that never starts. A refused start already throws, and this is the same refusal.
