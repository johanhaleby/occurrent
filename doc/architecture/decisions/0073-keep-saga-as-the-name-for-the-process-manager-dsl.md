# 73. Keep Saga as the name for the process manager DSL

Date: 2026-07-26

## Status

Accepted. This ADR names no new API and changes no code. It closes the pre-release window in which the saga DSL could
have been renamed at no cost to callers, and records why that window was allowed to close.

## Context

[ADR 63](0063-saga-dsl.md) introduced `Saga<E, S, C>`: a pure-data descriptor that reacts to domain events, and to its
own timeouts, by issuing commands while holding per-instance state. That ADR called it "a saga (process manager)" in
passing and did not examine the term, because the term was not what the ADR was deciding.

The term does not survive examination. In the original definition (Garcia-Molina and Salem, 1987) a saga is a long-lived
transaction decomposed into sub-transactions, each paired with a compensating transaction, such that a failure runs the
compensations for the completed prefix in reverse order. Backward recovery through registered inverses is the defining
feature, not an optional extra.

Occurrent ships no such thing. The effect alphabet is closed and complete:

```java
SagaEffect<C> = IssueCommand(C) | StartTimeout(name, Duration) | StartTimeoutAt(name, Instant) | CancelTimeout(name)
```

There is no compensation member, no registry of inverses, and no reverse unwinding anywhere in the executor. A
repository-wide search for "compensat" over Java, Kotlin, and Markdown sources returns a single hit, in an unrelated
ADR about projection event sources. By the original definition, what Occurrent ships is not a saga.

What it is, precisely, is the Process Manager of Hohpe and Woolf's *Enterprise Integration Patterns*: a component that
holds the state of a sequence and determines the next processing step from intermediate results. `Saga<E, S, C>` matches
that description member for member.

That leaves a real question with a deadline. The saga DSL is unreleased as of this ADR, so renaming it costs nothing to
callers now, and after 0.31.0 ships it costs a breaking change plus an OpenRewrite recipe. The rename surface, measured:
88 files with "saga" in their path, 3323 textual occurrences across 123 files, three published Maven artifacts
(`occurrent-saga-dsl-common`, `occurrent-saga-dsl-blocking`, `occurrent-saga-dsl-mongodb-spring`) plus their aggregator,
the `occurrent.saga.*` configuration namespace, the `saga-<id>` collection convention, the `sagaInstances-<id>` bean
name, three ADRs, the `example/saga/order-fulfillment` module, and the user documentation.

Against that sits the state of the term in this library's actual ecosystem. Axon Framework, NServiceBus, MassTransit and
Rebus all use "saga" for a stateful, correlated, timeout-driven component that issues commands, none of them requiring
registered compensations. NServiceBus in particular pairs a correlated saga type with timeout requests, which is the
same design as this one. A JVM developer arriving from any of them reads `@Saga` correctly on sight.

## Decision

**Keep `Saga` as the name, at every level of the API, and state the process-manager equivalence at every entry point.**

This follows the shape of [ADR 30](0030-keep-matchall-dcb-append-condition-with-documented-limit.md): keep a capability
whose imprecision is real, reject the strictly correct alternative because its cost is disproportionate, and state the
limit loudly at the API rather than enforcing it in the type system.

**The disambiguation is part of the decision, not a footnote.** The Javadoc of `Saga` opens with "A saga, more precisely
an event-driven *process manager*"; the `@Saga` annotation says "an event-driven process manager"; the user
documentation's Saga DSL section opens with "A saga (more precisely a process manager)" and carries a dedicated section
stating that a saga does not roll back, that a compensating action is an ordinary forward command the user issues on the
failing branch, and that there is no automatic inverse. A reader who cares about the distinction is told at whichever
surface they arrive through. Removing any of these weakens this decision and should be treated as a regression.

**`ProcessManager` was the only precise candidate, and it was rejected on ergonomics and internal consistency.** It
lengthens every call site permanently (`ProcessManagerEffect.startTimeout(...)`,
`SpringMongoProcessManagerStateStore`, `ProcessManagerStateStoreQueries`,
`occurrent.process-manager.timer-poll-interval`, `processManagerInstances-<id>`, Kotlin `processManager { }`, and
`FlowProcessManager` for the flow layer). It would also be the only two-word, `-Manager`-suffixed type in a core
vocabulary of crisp single nouns: `Decider`, `Projection`, `View`, `Snapshot`, `Subscription`, `SideEffect`. The
precision is bought with a permanent cost at every use, in a term whose imprecision one Javadoc sentence already
resolves.

**No shorter precise name exists.** The candidates were examined and each fails on its own terms:

- `Process` collides with `java.lang.Process`, which is imported into every compilation unit by default.
- `Flow` collides with `java.util.concurrent.Flow`, and the flow layer of this very DSL already owns the word
  (`FlowSaga`, `FlowState`).
- `Reactor` collides with Project Reactor, which this repository has reactive modules built on.
- `Workflow` is claimed by Temporal and Cadence for durable-execution replay of imperative code, which is a different
  model from a fold over a closed input alphabet, so it would mislead rather than clarify.
- `Automation` is Event Modeling vocabulary with little currency in the JVM ecosystem, and `@Automation` reads oddly for
  a correlated, stateful, timer-owning component.

**Compensation is not reflected in the name because there is no compensation machinery to reflect.** The
`order-fulfillment` example is nonetheless compensation-shaped: `PaymentFailed` and the payment timeout both issue
`CancelOrder`, undoing the `ReservePayment` issued when the process started. The idiom is fully supported and
hand-written, forward-moving rather than automatic, and the documentation says so explicitly.

## Consequences

- `Saga` and its whole type family, the `occurrent.saga.*` namespace, the `saga-<id>` collection convention, the
  `sagaInstances-<id>` bean name, and the `occurrent-saga-dsl-*` coordinates all ship as they are in 0.31.0.
- The pre-release rename window closes deliberately rather than by omission. A later rename becomes a breaking change to
  a released API and needs an OpenRewrite recipe and an upgrade-guide entry, which is a real cost this ADR accepts in
  exchange for not paying the ergonomic one.
- A reader who holds the strict 1987 definition will find the type name wrong until they read one sentence of Javadoc or
  the first sentence of the documentation section. That is the accepted downside, and it is why the disambiguation at
  those entry points is load-bearing rather than decorative.
- This ADR reopens and settles a question ADR 63 declared closed in passing. Where the two disagree on the standing of
  the term, this ADR governs; ADR 63 continues to govern the design of the descriptor itself.
- If the DSL ever grows real compensation machinery, registered inverses unwound in reverse on failure, the name becomes
  accurate by the original definition and this ADR becomes moot rather than wrong.
- The reasoning is recorded here rather than only in the Javadoc, so the decision is auditable and does not have to be
  reconstructed from the code. The previous vocabulary decision in this project, the `Policy` to `SideEffect` rename,
  was never given an ADR, and ADR 63's attempt to cite it names four ADRs that do not contain it while the codebase
  ships a `SnapshotPolicy`. That is the failure mode this ADR exists to avoid.
