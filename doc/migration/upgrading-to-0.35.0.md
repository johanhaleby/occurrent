# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change, what the `UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and
what you have to do by hand. Find the parts of Occurrent you use in the lists below and read those sections.

The sections that can break the build are listed under "Changes that can break the build", and each bullet says when.
The compiler finds those breaks for you, but a section in that list can also describe a change that still compiles. The
other sections compile fine and behave differently, so read them first.

### Changes that can break the build

- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  `SagaEnvelope` gains a 14th record component and `SagaRunnerConfig` a 6th. **Breaks the build** if you use a record
  pattern over either.
- [A quarantined saga instance can be released, and quarantine is on by default](#a-quarantined-saga-instance-can-be-released-and-quarantine-is-on-by-default).
  `SagaFailure` gains a 6th and a 7th record component, and `SagaEnvelope` a 15th. **Breaks the build** if you use a
  record pattern over either.
- [A saga timer that keeps failing waits behind the timers already due](#a-saga-timer-that-keeps-failing-waits-behind-the-timers-already-due).
  `SagaEnvelope.TimerEntry` gains a third record component. **Breaks the build** if you use a record pattern over it.
- [The deprecated `accept(..)` on the projection feeds is removed](#the-deprecated-accept-on-the-projection-feeds-is-removed). `DomainEventFeed` and
  `CatchupProjectionFeed` lose both `accept(..)` overloads, on the blocking and the reactor stack. **Breaks the build**
  if you still call one of them.
- [The reactive `executeAndReturnState` emits a non-null state](#the-reactive-executeandreturnstate-emits-a-non-null-state).
  `ReactiveSnapshotDeciderApplicationService` and `ReactiveSnapshotDcbDeciderApplicationService` return
  `Mono<@NonNull S>`. **Breaks the build** for Kotlin code that writes the result type as `Mono<State?>`.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from).
  `DcbStartAt` gains the case `DcbStartAt.AtCheckpoint`. **Breaks the build** if you `switch` exhaustively over
  `DcbStartAt` without a `default`.
- [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions). `Snapshot` gains a 4th and a 5th
  record component. **Breaks the build** if you use a record pattern over it.
- [The reactor `HistoryLossReportingSubscriptions` gains a method](#the-reactor-historylossreportingsubscriptions-gains-a-method).
  **Breaks the build** if you implement the interface yourself.
- [A synchronous event dispatcher you implement breaks on `beforeWrite()`](#a-synchronous-event-dispatcher-you-implement-breaks-on-beforewrite).
  **Breaks the build** if you implement `SynchronousEventDispatcher` or `ReactiveSynchronousEventDispatcher` yourself,
  and an implementation compiled against 0.34.0 throws `AbstractMethodError`.
- [A record pattern over `NativeMongoSubscription` stops compiling](#a-record-pattern-over-nativemongosubscription-stops-compiling).
  `NativeMongoSubscription` gains a third record component. **Breaks the build** if you use a record pattern over it.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  `DcbConsistencyToken` becomes a class, and `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)` are
  removed. **Breaks the build** if you create a token, call one of those constructors or call `value()`, which a
  `DcbEventStore` of your own usually does.
- [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). **Breaks the build** if you
  use a record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`, or call `new DcbEventStream(events, head)`.
- [`ComposedDefaultStartPosition` and its bean are removed](#composeddefaultstartposition-and-its-bean-are-removed).
  **Breaks the build** if you inject the `occurrentComposedDefaultStartPosition` bean, name the type, or call or
  override `occurrentCompetingDurableSubscriptionModel(..)`.
- [Five types the Spring Boot starters share move to an internal package](#five-types-the-spring-boot-starters-share-move-to-an-internal-package).
  **Breaks the build** if you refer to `SubscriptionAnnotations`, `PushCatchupStatusImpl`, `AsynchronousSubscribables`,
  `AppliedAppendRecordingRegistry` or `PolledCatchupSignals`, and `UpgradeToOccurrent_0_35` rewrites the references.
- [A held event is withdrawn instead of delivered without the lease](#a-held-event-is-withdrawn-instead-of-delivered-without-the-lease).
  `AttemptNotMade` moves from the nested `org.occurrent.retry.internal.RetryExecution.AttemptNotMade` to
  `org.occurrent.retry.AttemptNotMade`. **Breaks the build** if you name the nested class.
- [A `SagaStateStore` stores an encoded saga](#a-sagastatestore-stores-an-encoded-saga). `SagaStateStore` and
  `SagaStateStoreQueries` take and return `StoredSaga` instead of `SagaEnvelope`, and `SagaStateStore` gains
  `stateCodec()`. **Breaks the build** if you implement a `SagaStateStore`, or call one of its reads directly.
- [The shorter `SagaEnvelope` and `SagaFailure` constructors are removed](#the-shorter-sagaenvelope-and-sagafailure-constructors-are-removed).
  **Breaks the build** if you call the 13- or 11-argument `SagaEnvelope` constructor, or the 5-argument `SagaFailure`
  one.

### Sagas

- [A global position is no longer a saga redelivery key](#a-global-position-is-no-longer-a-saga-redelivery-key). A
  saga recognises a redelivery by `streamid` with `streamversion` only. An event that has just a `position` is refused
  under the default `REQUIRED`, and an unreadable `streamid` or `streamversion` fails the delivery. Compiles unchanged.
- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  A saga keeps the highest applied version per stream and event type, and applies a late version instead of skipping
  it. Saga instances store a new `streamMarks` field, and an `ACTIVE` instance that holds it is stored as
  `ACTIVE_MARKED`, which 0.34.0 can't read and a query of your own for `status: "ACTIVE"` doesn't find. Stop every
  0.34.0 node that runs sagas before any 0.35.0 node starts, and run one update before going back to 0.34.0. Breaks the build only for a record pattern over `SagaEnvelope` or `SagaRunnerConfig`.
- [A saga's quarantine budget runs per failing event](#a-sagas-quarantine-budget-runs-per-failing-event). Each failing
  event keeps its own clock. Compiles unchanged.
- [A quarantined saga instance can be released, and quarantine is on by default](#a-quarantined-saga-instance-can-be-released-and-quarantine-is-on-by-default).
  A saga quarantines an instance after 5 minutes unless you turn quarantine off, and `SagaSubscription.release(sagaId)`
  brings a quarantined instance back. A `SagaRunner` with quarantine on and no `EventStoreQueries` fails at startup
  until you pass it one or call `disableQuarantine()`. Stop every 0.34.0 node that runs sagas before any 0.35.0 node
  starts, since a 0.34.0 node fails on every event for an instance that 0.35.0 quarantined. On MongoDB such an
  instance is stored as `QUARANTINED_RECORDING`, so a query of your own for `status: "QUARANTINED"`, such as the
  0.34.0 runbook's, no longer finds it. `SpringMongoSagaStateStore` refuses a collection name that ends in
  `_skipped_inputs`, creates a second collection next to its own, and reads instances from the primary, with
  quarantine on or off. Breaks the build only for a record pattern over `SagaFailure`, `SagaEnvelope` or
  `SagaRunnerConfig`.
- [A saga timer that keeps failing waits behind the timers already due](#a-saga-timer-that-keeps-failing-waits-behind-the-timers-already-due). A failed
  timer is tried again after every instance that was already due. Compiles unchanged unless you use a record pattern
  over `TimerEntry`.
- [A `SagaStateStore` stores an encoded saga](#a-sagastatestore-stores-an-encoded-saga). Occurrent encodes and decodes
  a saga instance with `SagaCodec`, and a store persists the `StoredSaga` it's given. `SpringMongoSagaStateStore` keeps
  its document layout, so nothing needs migrating. Breaks the build for a `SagaStateStore` of your own and for code that
  reads an instance from a store directly.
- [Saga writes on MongoDB wait for a majority to journal them](#saga-writes-on-mongodb-wait-for-a-majority-to-journal-them).
  `SpringMongoSagaStateStore` writes every saga instance at `majority` with `j: true` whatever your write concern, and
  quarantine on MongoDB needs a replica set. On a replica set of a primary, a secondary and an arbiter, every saga
  write fails while the secondary is down. Compiles unchanged, and a store built on a `MongoOperations` that isn't a
  `MongoTemplate` fails at construction.
- [The shorter `SagaEnvelope` and `SagaFailure` constructors are removed](#the-shorter-sagaenvelope-and-sagafailure-constructors-are-removed).
  Only the canonical constructors remain.

### DCB

- [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). **Breaks the build** if you
  use a record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`, or call `new DcbEventStream(events, head)`.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  A `DcbAppendCondition` with a token read with other criteria throws `IllegalArgumentException`, on every store. A
  token value saved under 0.34.0 isn't valid. **Breaks the build** if you create a token, call
  `new DcbEventStream(events, head)` or `new DcbDomainEventStream(events, head)`, or call `value()`, which a
  `DcbEventStore` of your own usually does.
- [Criteria with tags conflict on their tags](#criteria-with-tags-conflict-on-their-tags). On MongoDB, sharing a type
  with a criteria item that has tags is no longer a reason for a condition to refuse.
- [Appends of one type can spread over eight marker documents](#appends-of-one-type-can-spread-over-eight-marker-documents).
  On MongoDB, `spreadTypeMarkerWrites(true)` lets appends of one type with different tags commit at the same time.
  Whatever the setting, a read or conditional append on a criteria item without tags uses eight marker documents per
  type. Don't turn it on while a 0.34.0 node uses the store.
- [An append on MongoDB waits for majority on its position reservation](#an-append-on-mongodb-waits-for-majority-on-its-position-reservation).
  This overrides a w:1 on that one write, which on the Spring and reactor stores now runs outside a transaction of yours
  too. On a set of a primary, a secondary and an arbiter with the secondary down, an append throws after
  `markerVersionsCommitTimeout`.
- [The default transaction executors retry on a MongoDB error label](#the-default-transaction-executors-retry-on-a-mongodb-error-label).
  A duplicate key from a write made outside the transaction is no longer retried.

### Subscriptions and catch-up

- [A read by position makes no promise about resuming](#a-read-by-position-makes-no-promise-about-resuming). A loop that
  keeps a position and reads after it through `readInPositionOrder(..)` or `afterPosition(..)` still compiles, and can
  skip an event.
- [A resume at a position through a `DurableSubscriptionModel` stores the checkpoint](#a-resume-at-a-position-through-a-durablesubscriptionmodel-stores-the-checkpoint).
  `RepositionableSubscriptions.findIn(durableModel)` finds the durable model, which stores the position before it
  resumes there. Compiles unchanged.
- [A first position a checkpoint storage can only write unconditionally is refused](#a-first-position-a-checkpoint-storage-can-only-write-unconditionally-is-refused).
  A `CheckpointStorage` of your own that answers `false` from `evaluatesWriteConditionsFor` fails a new subscription
  at the model default, where 0.34.0 wrote its first position unconditionally with a warning. Compiles unchanged.
- [A capability that acts on a subscription is not found past a wrapper](#a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper).
  `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. Compiles unchanged.
- [A `CompetingConsumerListener` can be told the same thing twice, and after the call returns](#a-competingconsumerlistener-can-be-told-the-same-thing-twice-and-after-the-call-returns).
  A listener of your own on a MongoDB lease strategy can hear the same answer twice, a prohibition with no grant
  before it, and the change of a register, release or unregister after that call returned. Compiles unchanged.
- [A held event is withdrawn instead of delivered without the lease](#a-held-event-is-withdrawn-instead-of-delivered-without-the-lease).
  A competing consumer over a MongoDB subscription model makes the action throw `AttemptNotMade` for an event that
  waits for the lease, where 0.34.0 ran it without the lease. A subscription model, a wrapper or a
  `CompetingConsumerStrategy` of your own needs a look. **Breaks the build** if you name `RetryExecution.AttemptNotMade`.
- [The deprecated `accept(..)` on the projection feeds is removed](#the-deprecated-accept-on-the-projection-feeds-is-removed). Call `acceptAndWait(..)` instead, which
  `accept(..)` already forwarded to. `UpgradeToOccurrent_0_34` renames the calls, but only while the project still
  depends on 0.33.0 or 0.34.0. Breaks the build.
- [A catch-up needs the event store itself](#a-catch-up-needs-the-event-store-itself). A catch-up subscription model
  throws `IllegalArgumentException` when you construct it over a decorator, a test double or your own implementation of
  the event store interfaces. Compiles unchanged.

- [A catch-up keeps its live start in the checkpoints it stores](#a-catch-up-keeps-its-live-start-in-the-checkpoints-it-stores).
  0.34 refuses the new checkpoints, so a rolling upgrade or a rollback needs care. A catch-up that loses its live
  start on every replay now throws `CatchupCannotResumeException`. Compiles unchanged.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from).
  A blocking catch-up delivers each replayed event as a `CheckpointAwareCloudEvent`, which equals only another one
  with the same checkpoint. Store the checkpoint with your state to resume from it. **Breaks the build** if you
  `switch` exhaustively over `DcbStartAt` without a `default`.
- [A catch-up over both a stream and a DCB model refuses a position start without a filter](#a-catch-up-over-both-a-stream-and-a-dcb-model-refuses-a-position-start-without-a-filter).
  A subscription with no filter that starts at a position checkpoint throws `IllegalArgumentException`. Compiles
  unchanged.
- [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it).
  A start after a position above 0 with no live start, `DcbStartAt.afterPosition(n)` among them, and a replay from
  above the beginning whose live start is lost throw `CatchupCannotResumeException`. Compiles with a removal warning.
- [A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0).
  `StartAt.checkpoint(GlobalCheckpoint.of(0))` replays the history where it used to go live, and a position above 0
  throws `UnsupportedStartAtException`. Compiles unchanged.
- [A MongoDB subscription model refuses a catch-up's checkpoint](#a-mongodb-subscription-model-refuses-a-catch-ups-checkpoint).
  A subscribe at a `GlobalCheckpoint` or a `CatchupTimeCheckpoint`, or at a dynamic start that returns one, is refused
  with `UnsupportedStartAtException` instead of opening at the present. Compiles unchanged, except a record pattern
  over `NativeMongoSubscription`.
- [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
  Every subscription a catch-up subscription runs on MongoDB, which in Spring Boot is every subscription by default.
  Compiles unchanged.
- [A blocking cancel doesn't wait for a subscribe whose `StartAt.dynamic` function runs](#a-blocking-cancel-doesnt-wait-for-a-subscribe-whose-startatdynamic-function-runs).
  A cancel made while the function runs returns first, and that subscribe subscribes nothing. Compiles unchanged.
- [A blocking cancel no longer promises a rebuild](#a-blocking-cancel-no-longer-promises-a-rebuild).
  `cancelSubscription(id)` on the blocking `CatchupThenPushSubscriptionModel` still deletes the catch-up marker, but a
  rebuild takes a new subscription id. Under a new id the subscription starts from the beginning of the history and
  repeats everything its handler does, the records it writes, the commands it dispatches and the messages it sends.
  Stop every instance on the old id before the first one on the new id starts. Stopping them doesn't prevent the
  repeats. With more than one instance, what else a new id needs depends on where each instance keeps what it builds
  and which broker queue or consumer group it consumes, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes. Compiles unchanged.
- [A dynamic `StartAt.now()` over `ReactorDurableSubscriptionModel` fails when the wrapped model answers nothing](#a-dynamic-startatnow-over-reactordurablesubscriptionmodel-fails-when-the-wrapped-model-answers-nothing).
  `waitUntilStarted()` fails where 0.34.0 started the subscription from wherever the feed had reached. Over
  `ReactorMongoSubscriptionModel` it fails only when MongoDB's reply to `hello` has no `localTime`, where 0.34.0 didn't
  start it either. Compiles unchanged.
- [A dynamic `StartAt.now()` that the wrapped model refuses fails `waitUntilStarted()` instead of `subscribe(..)`](#a-dynamic-startatnow-that-the-wrapped-model-refuses-fails-waituntilstarted-instead-of-subscribe).
  A refusal of the subscribe, for an unsupported filter for example, no longer throws from `subscribe(..)`. Compiles
  unchanged.
- [A resumed MongoDB subscription waits for the action that ran when it was paused](#a-resumed-mongodb-subscription-waits-for-the-action-that-ran-when-it-was-paused).
  A resumed `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` subscription calls its action only once the
  call from before the pause has returned. Compiles unchanged.
- [A MongoDB subscription at the present is refused when the server gives it no position](#a-mongodb-subscription-at-the-present-is-refused-when-the-server-gives-it-no-position).
  On a server that sends no operation time, a `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel`
  subscription at the present reads the change stream once to record its position, and `waitUntilStarted(..)` and
  `start(true)` throw `UnsupportedStartAtException` when that read gives none. Compiles unchanged.
- [A MongoDB subscription whose history is lost is refused when restarts are off](#a-mongodb-subscription-whose-history-is-lost-is-refused-when-restarts-are-off).
  With `restartSubscriptionsOnChangeStreamHistoryLost` off, `waitUntilStarted(..)` and `start(true)` throw
  `UnsupportedStartAtException` for a `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` subscription
  whose change stream history is gone. In 0.34.0 a `waitUntilStarted()` with no timeout never returned, one with a
  timeout answered `false`, and `start(true)` returned. Through a mongos, a subscription at a stored position, or at
  a present that `subscribe(..)` read earlier than right before its change stream opened, such as on a stopped model,
  counts as started once the change stream's first reply came, whatever that setting is. Compiles unchanged.
- [A subscription handle throws once the model gives the subscription up](#a-subscription-handle-throws-once-the-model-gives-the-subscription-up).
  A handle from `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` that answered `true` throws from
  `waitUntilStarted(..)` once the model refuses the subscription after a loss of history, or holds it paused after a
  restart that gets no position. In 0.34.0 it kept answering `true`. Compiles unchanged.
- [A blocking catch-up resume at a dynamic start uses the catch-up's answer](#a-blocking-catch-up-resume-at-a-dynamic-start-uses-the-catch-ups-answer).
  The live model resumes at the start a `StartAt.dynamic(..)` function answered the catch-up, where 0.34.0 let the live
  model ask the function for itself. Compiles unchanged.
- [A blocking catch-up refuses a resume at a position of a subscription that runs](#a-blocking-catch-up-refuses-a-resume-at-a-position-of-a-subscription-that-runs).
  `resumeSubscription(id, startAt)` throws `SubscriptionAlreadyRunningException` itself, where 0.34.0 passed the resume
  on to the live model and the MongoDB models threw `UnknownSubscriptionException` while the `subscribe(..)` replay ran.
  Compiles unchanged.
- [A catch-up marker on the reactor stack is a position to resume after](#a-catch-up-marker-on-the-reactor-stack-is-a-position-to-resume-after).
  A reactor `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` or `DomainEventFeed` start that reads a stored
  marker replays the events after it instead of skipping its replay, and a cancel no longer promises a rebuild.
  Compiles unchanged.
- [The reactor `HistoryLossReportingSubscriptions` gains a method](#the-reactor-historylossreportingsubscriptions-gains-a-method).
  **Breaks the build** if you implement the interface yourself.
- [A failure after the write doesn't run the command again](#a-failure-after-the-write-doesnt-run-the-command-again). A
  handler of a synchronous subscription that fails now makes `execute` throw, where a retry could run the command again.

### Spring Boot starter

- [A first position a checkpoint storage can only write unconditionally is refused](#a-first-position-a-checkpoint-storage-can-only-write-unconditionally-is-refused).
  A `CheckpointStorage` bean of your own that answers `false` from `evaluatesWriteConditionsFor` fails startup when the
  starter would store a live start as a subscription's first checkpoint, or fails the creation of a handler bean built
  after startup. Compiles unchanged.

- [A catch-up needs the event store itself](#a-catch-up-needs-the-event-store-itself). A context whose
  `EventStoreQueries`, event store or `DcbEventStore` bean is a decorator, a mock or your own implementation fails at
  startup when a catch-up runs over it. Compiles unchanged.

- [A catch-up keeps its live start in the checkpoints it stores](#a-catch-up-keeps-its-live-start-in-the-checkpoints-it-stores).
  A subscription with `resumeBehavior = SAME_AS_START_AT` goes live from the live start its catch-up read. Compiles
  unchanged.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from). A
  `SubscriptionResumePoints` bean says where each annotated subscription resumes. Compiles unchanged.
- [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it).
  `startAtGlobalPosition` and `startAtDcbPosition` above 0 are refused at start, and deprecated for removal. Compiles
  with a removal warning.
- [A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0).
  `startAt = StartPosition.BEGINNING` replays the history over a store with stream position off, where from 0.30.0 to
  0.34.0 it got none of it. Compiles unchanged.
- [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
  `restart-on-change-stream-history-lost` no longer affects a subscription the starter runs through a catch-up
  subscription, which is every subscription by default. Compiles unchanged.
- [A Spring Mongo event store sends a commit again when MongoDB can't say whether it succeeded](#a-spring-mongo-event-store-sends-a-commit-again-when-mongodb-cant-say-whether-it-succeeded).
  The starters now create `CommitRetryingMongoTransactionManager` and `CommitRetryingReactiveMongoTransactionManager`,
  and a blocking event store built by hand, or a reactive one built from a `ReactiveTransactionManager`, logs a warning
  until it uses one. Compiles unchanged.
- [A restart follows `resumeBehavior` for `startAt = BEGINNING_OF_TIME` and `startAt = NOW`](#a-restart-follows-resumebehavior-for-startat--beginning_of_time-and-startat--now).
  On the reactive stack a restart no longer replays the history of a `BEGINNING_OF_TIME` `@StreamSubscription`, and a
  `NOW` subscription no longer misses the events written while the application was down. On the blocking stack `NOW` with
  `SAME_AS_START_AT` starts at the present on every restart and no longer competes with the other instances, so every
  instance is delivered every event. A reactive `NOW` subscription with the default `resumeBehavior` also asks the
  server where the feed is each time it registers and needs the answer only when no checkpoint is stored, so when it
  waits for its start, which is the default in `auto` subscription mode, the application does not start when no
  checkpoint is stored and the MongoDB server cannot answer. Compiles unchanged.
- [A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint).
  On the blocking stack a subscription with `startAt` unset no longer skips the events written while the application
  was down when a catch-up's replay stopped before it went live. Compiles unchanged.
- [A handler method the bean inherits is registered](#a-handler-method-the-bean-inherits-is-registered). An annotated
  method that a bean's class inherits from a superclass, or gets as an interface default method, starts running, and
  replays the history on its first start when its `startAt` or `source = PUSH` asks for that. The application fails
  at startup when two beans run one annotated method, for example a bean of the class that declares it and a bean of
  a subclass, or when a bean behind a CGLIB proxy inherits a final subscription handler. A method without a handler
  annotation that shares its name and parameters with a package-private annotated method in another package fails at
  startup instead of running in that method's place. A method that repeats the handler annotation of such a method, or
  of a private superclass method, runs together with it, and with the same id the application fails at startup. A
  static method that hides an annotated static factory method of a superclass fails at startup the same way. A
  handler declared with a type variable gets only the type the bean's class gives it, which can narrow a handler that
  ran in 0.34.0, one of the bridge kind or one on a JDK proxy that can't be unwrapped. Compiles unchanged.
- [A write through the application service is refused while the application starts](#a-write-through-the-application-service-is-refused-while-the-application-starts).
  A blocking write from a `@PostConstruct` method, an `InitializingBean` or a `@Bean` method fails startup with
  `WriteRefusedException`, and so does a reactive one whose error reaches that method, for example because it blocks
  on the `Mono`. A reactive write subscribed without an error consumer stores nothing, and the application starts. A
  context whose event store bean is a Mockito mock fails startup with an `IllegalStateException`. Under `MANUAL` the
  synchronous handlers run from the start on writes through the application service. A handler that starts live,
  `@Projection` and `@Saga` by default among them, can get what is written directly to the event store while the
  application starts, except in the cases that section lists. A write while the
  synchronous subscription model is stopped, or one a paused synchronous subscription matches, is refused, also
  without a starter. Compiles unchanged.
- [A synchronous event dispatcher you implement breaks on `beforeWrite()`](#a-synchronous-event-dispatcher-you-implement-breaks-on-beforewrite).
  **Breaks the build** if you implement `SynchronousEventDispatcher` or `ReactiveSynchronousEventDispatcher` yourself,
  and an implementation compiled against 0.34.0 throws `AbstractMethodError`.
- [A synchronous handler the startup scan can't see fails the creation of its bean](#a-synchronous-handler-the-startup-scan-cant-see-fails-the-creation-of-its-bean).
  A `@Lazy` bean whose declared type hides its synchronous handler, created after the startup scan, throws an
  `IllegalStateException`. Compiles unchanged.
- [Five types the Spring Boot starters share move to an internal package](#five-types-the-spring-boot-starters-share-move-to-an-internal-package).
  `SubscriptionAnnotations`, `PushCatchupStatusImpl`, `AsynchronousSubscribables`, `AppliedAppendRecordingRegistry`
  and `PolledCatchupSignals` move to `org.occurrent.springboot.common.internal`, and `UpgradeToOccurrent_0_35` rewrites
  the references. Code that calls `invokeDescriptorFactory`, `runsImplementationOf` or `resolveTypeFilter` directly
  sees the changes the starters make to find inherited handlers. **Breaks the build** if you refer to one of the five.
- [A quarantined saga instance can be released, and quarantine is on by default](#a-quarantined-saga-instance-can-be-released-and-quarantine-is-on-by-default).
  `occurrent.saga.quarantine-after` defaults to 5 minutes, so a `@Saga` quarantines unless you set
  `occurrent.saga.quarantine-enabled=false`. A blank value fails startup and names `quarantine-enabled=false`. With quarantine on, no `EventStoreQueries` bean, or several and none
  of them `@Primary`, fails at startup. The `sagaSubscription-<id>` bean releases an instance. Compiles unchanged.

- [The MongoDB starter adds two health contributors for competing consumers](#the-mongodb-starter-adds-two-health-contributors-for-competing-consumers).
  With Spring Boot Actuator, `/actuator/health` is down when a competing consumer subscription was given up or keeps
  failing, or when the lease refresh keeps failing. Compiles unchanged.

### Brokers

- [A catch-up over both a stream and a DCB model refuses a position start without a filter](#a-catch-up-over-both-a-stream-and-a-dcb-model-refuses-a-position-start-without-a-filter).
  `CloudEventForwarder.forward(subscriptionId, startAt)` and `DomainEventForwarder.forward(subscriptionId, startAt)` are
  refused at a position checkpoint over such a model. Compiles unchanged.

#### RabbitMQ

- [A RabbitMQ source consumes again after a recovery without topology recovery](#a-rabbitmq-source-consumes-again-after-a-recovery-without-topology-recovery).
  A source on a connection with topology recovery turned off registers a new consumer after a recovery. A source stops
  in three cases where it kept running in 0.34.0. Two log lines change their text. Compiles unchanged.

#### Kafka

### Snapshots

- [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions). The DCB deciders and
  a `@Snapshot` DCB view save at marker versions, and ignore a snapshot 0.34.0 saved at a position. `Snapshot` gains two
  components. **Breaks the build** if you use a record pattern over `Snapshot`. A DCB application service with
  synchronous subscriptions on `noTransaction()` or a Spring executor no longer runs your domain function in its
  transaction executor's transaction outside a Spring transaction scope of yours. On the blocking service that holds only
  for a command that asks for marker versions, as a snapshot decider's does.
- [The reactive `executeAndReturnState` emits a non-null state](#the-reactive-executeandreturnstate-emits-a-non-null-state).
  The emitted state is declared non-null. Breaks the build only for a Kotlin `Mono<State?>` written by hand.

### The rewrite recipe

Run `UpgradeToOccurrent_0_35` while the project still depends on 0.34.0, and then move the dependency to 0.35.0.
`MoveAttemptNotMade_0_35` and `MoveSpringBootCommonTypesToInternal_0_35` look for each type at its old name, and once
the dependency is 0.35.0 no type has that name, so they change nothing.

- `org.occurrent.MigrateDcbMarkerVersionsRecordPattern_0_35` adds the new bindings to a record pattern over
  `DcbAppendResult`, `DcbEventStream`, `DcbReadOptions` or `Snapshot`. See
  [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions) and
  [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions).
- `org.occurrent.MigrateNativeMongoSubscriptionRecordPattern_0_35` adds the new binding to a record pattern over
  `NativeMongoSubscription`. See
  [A record pattern over `NativeMongoSubscription` stops compiling](#a-record-pattern-over-nativemongosubscription-stops-compiling).
- `org.occurrent.MoveAttemptNotMade_0_35` changes each reference to `RetryExecution.AttemptNotMade` to
  `org.occurrent.retry.AttemptNotMade`. See
  [A held event is withdrawn instead of delivered without the lease](#a-held-event-is-withdrawn-instead-of-delivered-without-the-lease).
- `org.occurrent.MoveSpringBootCommonTypesToInternal_0_35` changes each reference to `SubscriptionAnnotations` and its
  nested types, `PushCatchupStatusImpl`, `AsynchronousSubscribables`, `AppliedAppendRecordingRegistry` and
  `PolledCatchupSignals` to the package `org.occurrent.springboot.common.internal`. See
  [Five types the Spring Boot starters share move to an internal package](#five-types-the-spring-boot-starters-share-move-to-an-internal-package).
- No recipe rewrites a `DcbConsistencyToken` or the streams that create one. See
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
- No recipe rewrites a record pattern over `SagaFailure`, `SagaEnvelope` or `SagaRunnerConfig`, and none changes
  how quarantine is configured. See
  [A quarantined saga instance can be released, and quarantine is on by default](#a-quarantined-saga-instance-can-be-released-and-quarantine-is-on-by-default).
- No recipe ports a `SagaStateStore` of your own, or adds the components the removed `SagaEnvelope` and `SagaFailure`
  constructors filled in. See [A `SagaStateStore` stores an encoded saga](#a-sagastatestore-stores-an-encoded-saga)
  and
  [The shorter `SagaEnvelope` and `SagaFailure` constructors are removed](#the-shorter-sagaenvelope-and-sagafailure-constructors-are-removed).

## A global position is no longer a saga redelivery key

A saga recognises a redelivered event by its `streamid` together with its `streamversion`, and by nothing else. From
0.31.0 to 0.34.0 it also recognised an event without a stream key by its `position`, and that skipped events it had
never applied. The reason is in [ADR 150](../architecture/decisions/0150-a-global-position-is-not-a-saga-redelivery-key.md),
and the report is [#1298](https://github.com/johanhaleby/occurrent/issues/1298).

### Who is affected

A saga that receives events with a `position` and no `streamid` and `streamversion`. Events read from an Occurrent
event store have both, so a saga fed only from the store is not affected.

A custom feed that drops the stream key is affected, and so is an event whose `streamid` or `streamversion` cannot be
read. 0.34.0 treated an unreadable value as absent.

### What changes

| The event | `REQUIRED` (the default) | `BEST_EFFORT` |
| --- | --- | --- |
| Has `streamid` and `streamversion` | Applied, a redelivery is skipped | Applied, a redelivery is skipped |
| Has only a `position`, or nothing | Refused with `SagaRedeliveryDetectionException` | Applied with a warning, and a redelivery runs the reactions again |
| Has a `streamid` or `streamversion` that cannot be read | Refused with `SagaRedeliveryDetectionException` | Refused with `SagaRedeliveryDetectionException` |

An event the saga applies without a key can no longer be quarantined, since quarantine identifies the event it stopped
on by its key. Releasing a quarantined instance can apply such an event again, when the instance applied the same event
before it was quarantined.

### What to do

Forward `streamid` and `streamversion` from the feed, and the saga recognises its redeliveries.

If the feed cannot supply them, set `RedeliveryDetection.BEST_EFFORT` with `@Saga(redeliveryDetection = ..)` or
`SagaRunnerConfig.withRedeliveryDetection(..)`. Do that only when every command the saga issues is safe to receive
more than once.

If the exception reports an unreadable `streamid` or `streamversion`, fix the feed that wrote the value. Neither setting
lets the event through.

### Stored saga state

A saga instance saved by 0.31.0 to 0.34.0 may hold a `positionWatermark`. 0.35.0 loads it and ignores it, and the field
is gone from that instance's document after its next save. A document without the field reads as `null`, and
`SagaEnvelope.positionWatermark()` is deprecated.

0.35.0 reads a document that 0.34.0 saved. 0.34.0 can't read an instance that 0.35.0 stored as `ACTIVE_MARKED`, and
going back to 0.34.0 needs one step for those, as
[the stored saga state for late versions](#stored-saga-state-1) describes. Stop every 0.34.0 node that runs
sagas before any 0.35.0 node starts, and stop every 0.35.0 node before 0.34.0 starts again, as
[Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it) says.

A failure record whose input is `position:N` was written for a position-only event. On an `ACTIVE` instance 0.35.0
treats it as no record, so the next failure starts a fresh budget and the next successful delivery clears it.

A `QUARANTINED` instance keeps its record and stays quarantined. This finds the ones that hold such a record:

```javascript
db.<collection>.find({ failureInput: /^position:\d+$/ })
```

The [runbook for a quarantined saga instance](../runbooks/quarantined-saga-instance.md) says what to do with them.

### Why there is no recipe

No signature changes, so there is nothing for `UpgradeToOccurrent_0_35` to rewrite. Whether a feed can supply the
stream key, or should use `BEST_EFFORT`, depends on the feed.

## A resume at a position through a `DurableSubscriptionModel` stores the checkpoint

In 0.33.0 and 0.34.0, `RepositionableSubscriptions.findIn(durableModel)` answered the model the
`DurableSubscriptionModel` wraps, since the durable model didn't implement `RepositionableSubscriptions` and the
lookup went past it. A `resumeSubscription(subscriptionId, startAt)` through that answer resumed the subscription at
`startAt` and stored nothing, so a restart before the next event resumed from the checkpoint stored earlier.

From 0.35.0 the lookup answers the `DurableSubscriptionModel` itself when the model it wraps can resume at a position,
and empty otherwise. Its `resumeSubscription(subscriptionId, startAt)` stores the position as the subscription's
checkpoint, then resumes the wrapped model there.

* `StartAt.checkpoint(..)` stores that checkpoint.
* `StartAt.now()` stores the position the wrapped model's `globalCheckpoint()` answers. When it answers `null`, the
  call throws `IllegalStateException`, nothing is stored and the subscription stays paused.
* `StartAt.subscriptionModelDefault()` resumes from the stored checkpoint, as `resumeSubscription(subscriptionId)`
  does.
* A subscription subscribed without checkpointing, or one the wrapped model doesn't report as paused, is passed on to
  the wrapped model and nothing is stored.
* A checkpoint the wrapped model's new `restartsAt(..)` answers `false` for is passed on to the wrapped model and
  nothing is stored. The method answers `false` unless the model overrides it. The MongoDB subscription models answer
  `false` for a `GlobalCheckpoint`, a `CatchupTimeCheckpoint`, a time and every other checkpoint their change stream
  can't open at. They open at the present when they subscribe at one, so a stored one would make a restart skip
  events.

The checkpoint is written with the same write condition as every other checkpoint the durable model writes. A node
whose write the checkpoint storage refuses, because another node has written with a newer lease, gets
`CheckpointWriteConditionNotFulfilledException` and the subscription stays paused. When the wrapped model's resume
throws after the write, the checkpoint stored before is put back with the write condition the position was stored
with and the exception is thrown, so a failed reposition leaves the stored position as it was before the call. When
nothing was stored before, the new checkpoint stays.

A catch-up model whose live delegate is a `DurableSubscriptionModel` now has the position stored when it resumes the
durable model at one. [Section 29 of the 0.34.0 upgrade guide](upgrading-to-0.34.0.md#29-a-blocking-subscription-that-resumes-from-another-nodes-catch-up-position-replays-from-it) says such a resume stores nothing until the first live
position or the quiet save. That section stays as it was released, and over a durable model it no longer holds.

A blocking catch-up model with checkpoint storage that resumes a subscription at a checkpoint or at now also replaces a
stored position a catch-up replays from, such as the one its replay stored, whatever its live delegate. So a
`resumeSubscription(subscriptionId)` after a later pause doesn't replay from that position, as it could in 0.34.0. At
now it stores the present position, and throws `IllegalStateException` when it can read none. At a time after the
beginning without a live start, a stream catch-up stores that time with the present position as its live start, in the
`position:time;v:2;` form that
[A catch-up keeps its live start in the checkpoints it stores](#a-catch-up-keeps-its-live-start-in-the-checkpoints-it-stores)
describes, and 0.34 refuses that form. While a replay that `subscribe(..)` started is still running for a subscription
paused during that replay, it stores nothing and reads no present position.

There is nothing to change in your code unless it depends on the checkpoint staying where it was after a resume at a
position. A `DurableSubscriptionModel` over a repositionable subscription model you wrote yourself stores nothing on a
resume at a position, as in 0.34.0. To have the position stored, override `RepositionableSubscriptions.restartsAt(..)`
in that model to answer `true` for every checkpoint a subscription it subscribes there starts reading at. Apart from a
time a stream catch-up stores in the `;v:2;` form, nothing stored changes format, and there is no OpenRewrite recipe for
this. See
[ADR 151](../architecture/decisions/0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md) and
[#1253](https://github.com/johanhaleby/occurrent/issues/1253).

## A first position a checkpoint storage can only write unconditionally is refused

A durable subscription that starts at the model default, with no checkpoint stored, records its first position before
anything is delivered. `DurableSubscriptionModel` and `ReactorDurableSubscriptionModel` write it with
`CheckpointWriteCondition.ifAbsent()`, so of two nodes recording one at the same moment, only one write is stored. Both
models record one for a `StartAt.dynamic(..)` that answers `StartAt.subscriptionModelDefault()` too, and the Spring
Boot starters store a live start as the first checkpoint the same way.

### Who is affected

An application with a `CheckpointStorage` of its own that answers `false` from `evaluatesWriteConditionsFor(String)`,
which every implementation does unless it overrides `evaluatesWriteConditions()` or `evaluatesWriteConditionsFor`.
The `UpgradeToOccurrent_0_33` recipe from the 0.33.0 upgrade, through its `AddCheckpointStorageConditionalWriteStubs`
step, adds stubs that keep `evaluatesWriteConditions()` at `false`, so a storage it migrated is affected until it implements the conditions.

Every `CheckpointStorage` Occurrent ships answers `true` apart from `SpringRedisCheckpointStorage` for two kinds of
id. In Cluster-safe mode it answers `false` for an id Redis Cluster can't put in one slot with its version key, such
as `{}orders`, and its javadoc says no id an application would choose is built like that by accident. It also answers
`false` for an id in the reserved namespace of its version key, but its `read(..)`, `exists(..)` and `save(..)` refuse
such an id with `IllegalArgumentException`. Occurrent reads the stored checkpoint before it decides on a first
position, so a subscription with such an id fails on that read, as it did in 0.34.0.

### What changes

In 0.34.0 such a storage got the first position written unconditionally, and the model logged a warning. Two nodes
recording a first position at the same moment could then overwrite each other's and lose the events between the two
positions for good.

From 0.35.0 the subscription is refused with `IllegalStateException` before anything is written for it:

* `DurableSubscriptionModel` throws from `subscribe(..)`, where it refuses a subscription that has no start position to
  record. When a cancel on another node deletes the stored checkpoint before the subscription first starts, the
  subscription starts from the checkpoint read during `subscribe(..)` and records no first position, so it can deliver
  events again but skips none.
* `ReactorDurableSubscriptionModel` signals it on `Subscription.waitUntilStarted()`.
* Both Spring Boot starters fail startup when they would store a live start as the subscription's first checkpoint.
  For a handler on a bean built after startup, the creation of that bean fails instead.

Under `CompetingConsumerSubscriptionModel`, the exception reaches `subscribe(..)` on a node that holds the lease when
the subscription is made, and on every node whose model is stopped then. Every other node logs it as a warning when it
gets the lease and gives the lease back, and does that again each time it gets the lease.

A subscription that records no first position isn't refused, whatever the storage answers. That covers a checkpoint
already stored, a `StartAt` of your own that doesn't resolve to the model default, `StartAt.now()` and a dynamic one
answering it included, and a starter subscription at `NOW` with `resumeBehavior = SAME_AS_START_AT`.

### What to do

Make the storage evaluate `CheckpointWriteCondition.ifAbsent()` and answer `true` from `evaluatesWriteConditions()`,
or from `evaluatesWriteConditionsFor(String)` for the ids it evaluates conditions for. Or use one of the storages
Occurrent ships.

To keep the 0.34.0 behaviour and accept the loss, configure

```java
new DurableSubscriptionModelConfig(EveryN.everyEvent()).startWhenFirstPositionCanBeOverwritten(true)
```

or the same method on `ReactorDurableSubscriptionModelConfig`. With a Spring Boot starter, set

```properties
occurrent.subscription.start-when-first-position-can-be-overwritten=true
```

The starter reads only the property before it stores a first checkpoint, so the config method alone doesn't stop that
refusal. The MongoDB starters pass the property to the durable subscription model they build, so the property is all
that model needs. An application that defines its own `SubscriptionModel` bean gets no durable model from the starter,
and the one it builds reads only its own config, so it needs the config method as well as the property. There is no OpenRewrite recipe for this. See
[ADR 184](../architecture/decisions/0184-a-first-position-a-checkpoint-storage-can-only-write-unconditionally-is-refused.md).

## A capability that acts on a subscription is not found past a wrapper

In 0.33.0 and 0.34.0 the blocking `capability(type)` answered the model itself when it implemented `type`, and
otherwise unwrapped a `SubscriptionModelWrapper` and asked the model beneath. Every `findIn(..)` method calls it. So
`RepositionableSubscriptions.findIn(competingConsumerModel)` answered the model the `CompetingConsumerSubscriptionModel`
wraps, and a `resumeSubscription(subscriptionId, startAt)` through that answer ran the subscription while the competing
consumer still had it as paused and held no lease for it. A node that didn't hold the lease could then run the
subscription next to the node that did.

From 0.35.0 a wrapper is unwrapped only for a capability that a wrapper's own state doesn't depend on:

* `IntrospectableSubscriptions`, `ReplayAwareSubscriptions` and `HistoryRetainingSubscriptions`, which answer questions
  about subscriptions.
* `QuietPositionReportingSubscriptions`, which adds and removes a listener that is told positions.
* `Pushable`, which takes events in at the bottom of the stack.
* `HistoryLossReportingSubscriptions`, which a catch-up model reaches under a durable model.
* `StartRefusalReportingSubscriptions`, which a competing consumer model reaches under a durable model, so it can
  give back the lease of a subscription whose start the MongoDB model refused. A blocking catch-up model implements it
  itself. It passes the listener on to the MongoDB model under it, and also tells the listener when it holds a
  subscription paused because the live start of its replay is gone.

Every other capability is found only on a model that implements it. That is `RepositionableSubscriptions`,
`Subscribable`, `CancellableSubscriptions`, `SubscriptionModelLifeCycle`, `SubscriptionModel`,
`CheckpointAwareSubscriptionModel`, `StreamSubscriptionModel` and `DcbSubscriptionModel`.

* `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. There is no way to resume a competing subscription at a position yet.
  Pause and resume it through the `CompetingConsumerSubscriptionModel`. Over a `DurableSubscriptionModel` the resume
  starts from the stored checkpoint. The exception is a subscription subscribed at a `StartAt.checkpoint(..)` of its own
  that has stored no checkpoint yet, while the checkpoint and write version stored for the id are the ones read when it
  was subscribed, or that read failed. Its resume goes on from the position the model it wraps has read to. [#1304](https://github.com/johanhaleby/occurrent/issues/1304) decides how a resume at
  a position through a competing consumer should work.
* `capability(CheckpointAwareSubscriptionModel.class)`, or another model type, on a wrapper that doesn't implement it
  answers empty where it used to answer the model beneath. On the stack the Spring Boot starter builds, a
  `CompetingConsumerSubscriptionModel` over a `CatchupSubscriptionModel` over a `DurableSubscriptionModel`, it used to
  answer the `DurableSubscriptionModel`, and a `subscribe` through it went around both the catch-up and the competing
  consumer. Subscribe through the outermost model instead.
* A `SubscriptionModelWrapper` of your own is found for an acting capability only if it implements it. Implement it on
  the wrapper, update the wrapper's own state for the subscription and then forward to the model it wraps. The
  observing capabilities are still found through your wrapper without any change. A wrapper of your own that a
  `CatchupSubscriptionModel` or a `StreamCatchupSubscriptionModel` wraps has to implement `RepositionableSubscriptions`
  and forward to the model beneath. Otherwise the catch-up model treats it as a model that can't be resumed at a
  position, and resuming a subscription from a position the catch-up model stored throws `IllegalStateException`.

[Section 29 of the 0.34.0 upgrade guide](upgrading-to-0.34.0.md#29-a-blocking-subscription-that-resumes-from-another-nodes-catch-up-position-replays-from-it) says a resume at a position reached through `RepositionableSubscriptions.findIn(..)`
on a `CompetingConsumerSubscriptionModel` goes to the catch-up model under it. That section stays as it was released, and
from 0.35.0 there is no such resume.

The code compiles unchanged, and there is no OpenRewrite recipe, because the method signatures are the same and only
what the lookup answers changes. Nothing stored changes. See
[ADR 151](../architecture/decisions/0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md) and
[#1251](https://github.com/johanhaleby/occurrent/issues/1251).

## A `CompetingConsumerListener` can be told the same thing twice, and after the call returns

In 0.34.0 a `CompetingConsumerListener` added to `SpringMongoLeaseCompetingConsumerStrategy` or
`NativeMongoLeaseCompetingConsumerStrategy` was told each change of a lease once. A `registerCompetingConsumer`,
`releaseCompetingConsumer` or `unregisterCompetingConsumer` told the listeners of the change it made before it
returned, and threw what they threw. An `OutOfMemoryError` at the wrong moment could lose a grant or a prohibition for
good, which [#1349](https://github.com/johanhaleby/occurrent/issues/1349) fixes, and the fix needs both promises to go.

### Who is affected

Code that implements `CompetingConsumerStrategy.CompetingConsumerListener` itself and adds it to one of the two MongoDB
lease strategies with `addListener`. `CompetingConsumerSubscriptionModel` adds itself as a listener and handles
everything below, so a competing consumer built on it needs no change.

### What changes

* A listener can be told the same thing twice in a row. That can happen, for example, after an `OutOfMemoryError` cut a
  call short, when a lease is lost while the strategy works out what to tell, and in every round for as long as a
  listener keeps throwing an `Error`.
* A listener can be told a prohibition with no grant before it. A grant that was undone before the strategy got to tell
  it may never be told, but the prohibition after it is.
* A register, release or unregister may call the listeners before it returns, and throw what they throw, but only for
  the change it made itself and only while nothing else is calling them about that consumer, and anything else they
  are told, which can include that change, comes on a notifier thread, before or after the call returns, where what
  they throw is logged. A listener's `Error` that came out of `subscribe`, `resume` or `cancel` in 0.34.0 can end up
  only in the log, at ERROR.

### What to do

* Act on each call as "this is now true" rather than as "this just changed". On `onConsumeGranted`, start consuming
  unless the subscriber already does. On `onConsumeProhibited`, stop unless it has already stopped. A second call with
  the same answer then does nothing.
* Don't expect the listener to have been called once `registerCompetingConsumer`, `releaseCompetingConsumer` or
  `unregisterCompetingConsumer` returns. Use the `boolean` that `registerCompetingConsumer` returns when the caller
  needs to know at once whether it got the lease, and ask `hasLock(subscriptionId, subscriberId)` before it consumes.
  `hasLock` answers `false` while the listeners are still owed a prohibition for that consumer, so right after a
  register that returned `true` it can answer `false` until they have heard it. See
  [A held event is withdrawn instead of delivered without the lease](#a-held-event-is-withdrawn-instead-of-delivered-without-the-lease).
* Don't count on catching what the listener throws around one of those calls. Handle the failure inside the listener,
  since on a notifier thread the strategy only logs it.

The code compiles unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.
See [ADR 156](../architecture/decisions/0156-a-refresh-round-tells-the-listeners-what-they-have-not-heard.md).

## A held event is withdrawn instead of delivered without the lease

`CompetingConsumerSubscriptionModel` asks `CompetingConsumerStrategy.hasLock` before it runs the action of a competing
subscription for an event, and holds the event while the answer is no. In 0.34.0 a pause on the loss of the lease, a
cancel, a stop, a shutdown, or a call that only asked whether the wrapped model ran the subscription let the held event
through to the action. The node that held the lease could run the same event, so it reached the handler on two nodes.
[#1195](https://github.com/johanhaleby/occurrent/issues/1195) fixes that.

### Who is affected

* A subscription model of your own that a `CompetingConsumerSubscriptionModel` wraps, directly or under
  `DurableSubscriptionModel`, a blocking catch-up model or `ManualStartSubscriptionModel`.
* A `SubscriptionModelWrapper` of your own between a `CompetingConsumerSubscriptionModel` and a MongoDB subscription
  model.
* A `CompetingConsumerStrategy` of your own.
* Code that names `org.occurrent.retry.internal.RetryExecution.AttemptNotMade`.

A competing consumer built only from Occurrent's own blocking models over MongoDB, with one of the MongoDB lease
strategies, needs no change. The Spring Boot starter builds one.

### What changes

* Over a wrapped model that declares the new `RedeliveringSubscriptions` capability with `true`, a held event is
  withdrawn instead of let through. The action throws `org.occurrent.retry.AttemptNotMade`, and the wrapped model moves
  no position past the event and delivers it again on a later resume or start. That happens once a pause or cancel of
  the subscription, or a stop of the wrapped model, has returned, once the shutdown of the wrapped model has begun, and
  when an interrupt ends the wait once `shutdown()` has begun. A call that only asks about a subscription, or starts the
  wrapped model, leaves the event waiting for the lease.
* `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` declare it. `DurableSubscriptionModel`, the blocking
  catch-up models and `ManualStartSubscriptionModel` answer what the model they wrap answers.
  `InMemorySubscriptionModel` doesn't declare it.
* Over such a model, three rare cases still let the event through without the lease, and log a WARN each time. They are a subscription
  the competing consumer has forgotten, an interrupt before `shutdown()` has begun, and an event delivered on a thread
  that is inside a call the competing consumer makes into the wrapped model.
* Over a wrapped model that doesn't declare it, the event is let through as in 0.34.0, and the competing consumer logs a
  WARN that names `RedeliveringSubscriptions` the first time it does that.
* `hasLock` on `SpringMongoLeaseCompetingConsumerStrategy` and `NativeMongoLeaseCompetingConsumerStrategy` answers
  `false` while the strategy still owes its listeners a prohibition for that consumer, also when the node holds the
  lease again by then. The `CompetingConsumerStrategy.hasLock` contract asks the same of every strategy.
* A blocking catch-up replay that a subscribe started, and whose action withdrew an event, runs again from the last
  position it stored once the subscription is resumed or `start(true)` is called.
* `AttemptNotMade` moves from the nested `RetryExecution.AttemptNotMade` to `org.occurrent.retry.AttemptNotMade`.

The event is delivered again from the stored checkpoint only on a stack with a `DurableSubscriptionModel`, directly or
under a catch-up model. Over a bare MongoDB subscription model the node delivers again from its own position in memory.

Delivery stays at-least-once in these cases:

* A node that stalls after `hasLock` answered `true`, for example in a long GC pause, runs the action once it wakes up,
  also when its lease expired meanwhile.
* A MongoDB clock that runs ahead of the node's can make `hasLock` answer `true` after another node took the lease, as
  the `CompetingConsumerStrategy.hasLock` javadoc describes.
* An action still running when the lease is given up runs on, since the pause of a MongoDB subscription waits at most a
  second for it. See [#1513](https://github.com/johanhaleby/occurrent/issues/1513).
* A catch-up replay that loses the lease while it reads history can run an event on two nodes. A pause during a
  replay that a subscribe started takes effect only at the handover, so the replay hands the next event to the action
  after the pause has returned, and that event runs once the node gets the lease back. 0.34.0 did the same. See
  [#1528](https://github.com/johanhaleby/occurrent/issues/1528).
* A catch-up replay that runs again starts from this node's own last stored position, not the stored checkpoint, so
  after a failover it delivers again what another node delivered meanwhile. See
  [#1514](https://github.com/johanhaleby/occurrent/issues/1514). Without a failover, the node also delivers again the
  events it replayed since it last stored a position, up to 999 with the Spring Boot starter, which stores one every
  1,000 events.
* A catch-up replay built with `dontUseCheckpointStorage()` has stored no position, so when it runs again after a
  withdrawn event it starts from where the subscription asked to start, and this node delivers again the events it
  replayed before the withdrawn one.

With checkpoint fencing on, `occurrent.subscription.competing-consumer.fence-checkpoints` in Spring Boot, a checkpoint
write from the node that lost the lease is refused, so the stored checkpoint doesn't move backwards. The fencing
doesn't stop the action.

### What to do

* For a subscription model of your own, implement `RedeliveringSubscriptions` and answer `true` from
  `redeliversAnAttemptNotMade()` only when the model honours the contract in its javadoc. When the action throws
  `AttemptNotMade`, the model moves no position past the event, doesn't retry it or report it as a failure, and
  delivers it again when the subscription is resumed or started. It waits for the delivering thread only in a pause,
  cancel, stop or shutdown, and there only for a limited time. It never waits for it in any other call, a subscribe, a
  resume and a start included, since the competing consumer waits for such a call to return before it shuts the model
  down. If the model can't do that, leave the capability out. The competing consumer then lets the held
  event through as in 0.34.0 and logs a WARN.
* For a `SubscriptionModelWrapper` of your own, implement `RedeliveringSubscriptions` and answer what the model beneath
  answers, as `DurableSubscriptionModel` does, as long as the wrapper stores no position past an event whose action
  threw `AttemptNotMade`. `findIn` doesn't look past a wrapper for this capability, so without it the competing
  consumer treats the whole stack as one that doesn't deliver the event again.
* For a `CompetingConsumerStrategy` of your own, answer `false` from `hasLock` while the strategy still owes its
  listeners an `onConsumeProhibited` for a lock the subscriber lost, also when it holds the lock again by then. A
  strategy that answers `true` then lets the held event run under the new lock, although another node may have
  delivered it in between.
* Change a reference to `org.occurrent.retry.internal.RetryExecution.AttemptNotMade` to
  `org.occurrent.retry.AttemptNotMade`. `UpgradeToOccurrent_0_35` does that for you.

The code compiles unchanged unless it names `RetryExecution.AttemptNotMade`. Nothing stored changes. See
[ADR 161](../architecture/decisions/0161-a-held-event-is-withdrawn-instead-of-delivered-without-the-lease.md).

## A saga applies a stream version that arrives after a higher one

Up to 0.34.0 a saga kept the highest stream version it had applied for each stream, and skipped every later event at or
below it. That loses an event whenever a stream's events reach the saga out of order. `KafkaTopicPerTypeDestinationResolver`
orders one stream's events of one type and nothing across types, so a saga that listens to two event types of a stream
can see version 1000 of one type before version 2 of the other. The reason is in
[ADR 154](../architecture/decisions/0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md), and the
report is [#1305](https://github.com/johanhaleby/occurrent/issues/1305).

### Who is affected

A saga fed through a broker or a partitioner that does not keep a stream's events in version order. A saga fed straight
from an Occurrent event store, in order, applies events as before. Every saga on `SpringMongoSagaStateStore` is
affected by how 0.35.0 stores its instances, whatever feeds it. Stop every 0.34.0 node that runs sagas before any
0.35.0 node starts, and run an `updateMany` before you go back to 0.34.0, as [the stored saga state for late versions](#stored-saga-state-1) describes.

### What changes

The saga keeps the highest applied version per stream and event type. For an event of a stream:

| The event's stream version | What the saga does |
| --- | --- |
| Above the highest applied version of its event type | Applies it |
| Equal to that version, or one the saga recorded as applied | Skips it as a redelivery |
| Below it by less than the reorder window and not recorded | Applies it, since it arrived late |
| Below it by the reorder window or more, and not pending | Skips it and logs a warning once per stream |
| Pending, however far below it | Applies it and removes it from pending |

A reaction that ran for a late event runs once. A redelivery of that event is skipped.

An event is pending when it was handed to the saga and failed. The instance records its stream version before the
exception reaches the subscription, whether or not quarantine is on, so a broker that parks the event afterwards does
not lose it. An operator who replays the parked event after the instance has applied more than a window of later
versions still gets it applied, once. An instance keeps every pending version until it applies, and logs a warning
naming the stream and the type when a failure takes one stream's pending versions of one type past the window.

An instance that failed on an event under 0.34.0 or earlier holds that failure in its failure record, which names the
event but not its type. If another event fails before the first is offered again, the failure record is replaced, and the
instance keeps the first event's stream version in `legacyFailed` for its stream. The version is applied when it comes
back however far behind it is, and it is removed from the list once it is applied or fails again with its type known.

### The reorder window

The window is 256 by default. Change it with `SagaRunnerConfig.withReorderWindow(int)` when the feed reorders further:

```java
SagaRunnerConfig config = SagaRunnerConfig.defaults().withReorderWindow(1024);
```

A saga declared with `@Saga` always uses the default, since there is no annotation attribute or Spring property for it.
An event further behind than the window that did not fail in the saga is lost in the way it was lost before 0.35.0, and
the warning says which stream it came from. That is the precondition for exactly-once. The feed keeps a stream's order to
within the window per event type, or the event failed in the saga. Feeds that can reorder deeper than 256 versions are a
listener that delivers from several threads, a Kafka topic whose partition count was raised after events were produced,
and a RabbitMQ `prefetchCount` larger than the window. A larger window keeps more numbers in every instance. Raising the window later does not bring back what an
instance has already forgotten, because the `horizon` keeps every version at or below it skipped. The larger window
applies to the versions that arrive after the change.

### Stored saga state

A saga instance document gains a `streamMarks` field. It holds, per stream, a `floor` and a list of event types, and
each type has its highest applied `mark`, the `applied` versions below it and a `horizon`, the highest version it has
stopped remembering, and a `pending` list of event types with the `versions` of each that failed. A document without
`pending` reads as nothing pending. A `legacyFailed` list of stream versions, kept per stream beside `floor`, holds the
failures described above, and a document without it reads as an empty list. `pending` is a list rather than a map because
stream ids and event types can contain dots. An instance holds at most 256 `applied` versions per stream and event
type, so a document grows by some kilobytes for an instance that listens to a few streams, and every save rewrites them.
A release of a quarantined instance also keeps every version it applies, and the write that ends the release, which
makes the instance `ACTIVE`, completes it or quarantines it again, trims them back to the reorder window.

A document saved by 0.31.0 to 0.34.0, or by an earlier 0.35.0 build, has no `streamMarks`. It reads as a floor equal to
each stream's watermark, so everything the instance has already seen stays skipped, and a version above the watermark
is applied as before. Late versions are recognised from the instance's next save on.

`streamWatermarks` is still written with the highest version of each stream. 0.34.0 knows nothing of `streamMarks`,
and a 0.34.0 save that applies an event replaces the whole document and drops them. The next 0.35.0 node would then skip a
version that failed and waits to be applied again, or a late version, as applied already. To stop that, 0.35.0 stores
an `ACTIVE` instance that holds `streamMarks` with the status `ACTIVE_MARKED` (`SpringMongoSagaStateStore.ACTIVE_MARKED`)
and reads it back as `SagaStatus.ACTIVE`. Every event with a stream version adds marks, so that is every instance a
0.35.0 node has handled such an event for, and the instance keeps the status while it holds marks. 0.34.0 decodes the
status with `SagaStatus.valueOf` and throws on `ACTIVE_MARKED`, so a 0.34.0 node fails on such an instance instead of
saving it. The subscription, or the source's delivery failure policy, decides what then happens to the event.

`ACTIVE_MARKED` is there so that a deploy that runs a 0.34.0 node next to a 0.35.0 node fails loudly instead of
dropping the marks, and it doesn't make that deploy safe. A 0.34.0 node can park or lose the event, as the rest of
this section describes, and nothing a 0.35.0 node does stops that. So stop every 0.34.0 node that runs sagas before any
0.35.0 node starts, which takes the service that hosts the sagas down between the two, as
[Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it) says.

If a 0.34.0 node runs anyway, a saga fed from a Kafka or RabbitMQ source gets that failure in its handler on that node, and the source's
`DeliveryFailurePolicy` decides what happens to the message. With `REDELIVER`, the default, the source delivers the
message again once per poll for as long as a 0.34.0 node consumes it, and the messages behind it on that partition or
queue wait. With `PARK`, the source publishes the message to its parking topic or exchange and acknowledges it, so the
instance never gets it. Occurrent has no tool that moves a parked message back. Once every saga node runs 0.35.0,
publish each parked message again to the topic or exchange the saga consumes, with the headers and body it was parked
with. A message with a stream id and version that the instance applied already is skipped, as on any
redelivery. A 0.34.0 node recorded nothing for a message it parked, so 0.35.0 handles the republished message like any
late version. When its version is `reorderWindow` (256 by default) or more behind what the instance has applied of
that stream and event type, the saga skips it and logs a warning once per stream. So republish soon after the
deploy. An event that a 0.35.0 node parked after it failed in the saga is different, since that failure recorded it
as pending, and the saga applies it whenever it arrives again.

What a 0.34.0 node that runs next to a 0.35.0 node does with an event for an `ACTIVE_MARKED` instance depends on what
feeds the saga:

- An Occurrent subscription, or a Kafka or RabbitMQ source with `REDELIVER`, delivers the event again and again, so the
  saga's subscription, or the partition or queue, stalls on it, the same way it stalls on a quarantined instance, as
  [Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it) describes.
  With a competing-consumer strategy, or a partition or queue that a 0.35.0 consumer takes over, the stall ends once a
  0.35.0 node gets the event. Without competing consumers every node gets every event, so the 0.34.0 node's own
  subscription stalls until that node stops. Stopping every 0.34.0 node ends it either way.
- A Kafka or RabbitMQ source with `PARK` parks the event and acknowledges it, as the previous paragraph says. It parks
  every event it gets for such an instance, which is every instance a 0.35.0 node has handled an event with a stream
  version for. In a Kafka consumer group each restart rebalances the partitions, so a partition a 0.35.0 node consumed
  can move to a 0.34.0 consumer, which then parks every event for the instances that 0.35.0 node handled. Publish the
  parked messages again once every saga node runs 0.35.0. A message whose version is `reorderWindow` or more behind
  what the instance has applied is skipped with a warning.
- A `PushSubscriptionModel` that your own code pushes events into throws the exception back to that code. When your
  code doesn't push the event again, the instance never gets it. Push each event that failed again once every saga
  node runs 0.35.0. One whose version is `reorderWindow` or more behind what the instance has applied is skipped with a
  warning.

A 0.34.0 node's timer poller asks for `ACTIVE` instances only, so the timers of an `ACTIVE_MARKED` instance wait while a 0.34.0 node
holds the timer lease.

`findByStatus(ACTIVE, ..)` matches both strings. A query you wrote by hand for `status: "ACTIVE"`, and an alert built
on one, doesn't find an `ACTIVE_MARKED` instance, so match both strings:

```javascript
db.getCollection("saga-order-fulfilment")
  .find({ status: { $in: ["ACTIVE", "ACTIVE_MARKED"] } }, { state: 0 })
```

With quarantine on, which is the default, an instance may be stored as `QUARANTINED_RECORDING` too, so go back to
0.34.0 with the steps in
[Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it), which include
the ones here. To go back to 0.34.0, stop every 0.35.0 node. Then list the `ACTIVE_MARKED` instances that hold a version
that failed and waits to be applied again, in `pending` or `legacyFailed`. 0.34.0 skips every version at or below the
stream's watermark, so it never applies such a version once a later version of its stream was applied. The query lists
every instance that holds such a version, so it lists more instances than the ones that lose one:

```javascript
db.getCollection("saga-order-fulfilment")
  .find({ status: "ACTIVE_MARKED", $or: [
      { "streamMarks.pending.versions.0": { $exists: true } },
      { "streamMarks.legacyFailed.0": { $exists: true } } ] }, { state: 0 })
```

Give every `ACTIVE_MARKED` instance back its plain status before a 0.34.0 node starts:

```javascript
db.getCollection("saga-order-fulfilment")
  .updateMany({ status: "ACTIVE_MARKED" }, { $set: { status: "ACTIVE" } })
```

0.34.0 ignores the marks, and a 0.34.0 save that applies an event replaces the whole document and drops them. A version
that failed and waits to be applied again is then skipped when a later version of its stream was applied, and a late
version below the stream's watermark is skipped too, as on 0.34.0. Upgrading again reads an instance whose marks were
dropped as an old document.

### A record pattern over `SagaEnvelope` or `SagaRunnerConfig` stops compiling

A record pattern has to name every component of the canonical constructor. `SagaEnvelope` gains `streamMarks` as its
14th component and `SagaRunnerConfig` gains `reorderWindow` and `quarantineAfterDefaulted` as its 6th and 7th:

```java
// Before, stops compiling
case SagaRunnerConfig(var pollInterval, var batchLimit, var maxCas, var detection, var quarantineAfter) -> ...

// After
case SagaRunnerConfig(var pollInterval, var batchLimit, var maxCas, var detection, var quarantineAfter, var reorderWindow,
                      var quarantineAfterDefaulted) -> ...
```

The same applies to `SagaEnvelope`, which needs a `var streamMarks` after its `failure` binding and a
`var quarantineSkipCount` after that, and to an `instanceof` pattern as much as a `switch` case. `SagaCodec` stores
`SagaEnvelope.StreamMarks` and `SagaEnvelope.TypeMark` with the rest of the instance, so a `SagaStateStore` needs
nothing for them.

The five-argument `SagaRunnerConfig` constructor stays and fills in a window of 256. The 13-argument `SagaEnvelope`
constructor is removed, as
[The shorter `SagaEnvelope` and `SagaFailure` constructors are removed](#the-shorter-sagaenvelope-and-sagafailure-constructors-are-removed)
describes. `SagaExecutionSupport.EventMeta` gains an `eventType` component too, and it lives in an `internal` package.

### Why there is no recipe

`UpgradeToOccurrent_0_35` does not rewrite these patterns. A record pattern's arity is a compiler-enforced fact, so
the way `UpgradeToOccurrent_0_34` appends `var appendId` to a `WriteResult` pattern could be extended to these two
records, but the 0.34.0 change to the same two records got a guide entry and no recipe, and this follows that.

## A saga's quarantine budget runs per failing event

Up to 0.34.0 the quarantine budget belonged to the instance. The first failure wrote a `firstFailedAt`, and a later
failure of a different event kept it. Under a broker's PARK action a failure parked and forgotten days ago then made the
first failure of an unrelated event quarantine the instance. 0.35.0 gives each failing event its own clock. The reason
is in [ADR 157](../architecture/decisions/0157-a-sagas-quarantine-budget-runs-per-input.md), and the report is
[#1318](https://github.com/johanhaleby/occurrent/issues/1318).

### Who is affected

A saga with quarantine turned on through `SagaRunnerConfig.withQuarantineAfter(Duration)`, and a saga you turn it on for
while some of its events are failing. An event that failed while quarantine was off keeps the instant it first failed.

### What changes

The saga measures how long an event has been failing from the instant on its pending entry, or from the failure record's
`firstFailedAt` when the record names that event, or from now. So `SagaFailure.firstFailedAt()` is when the event the
record names first failed.

| Sequence | 0.34.0 | 0.35.0 |
| --- | --- | --- |
| Versions 7, 8, 7, 8 fail in turn | measured from the first failure | each measured from its own first failure, so 7 still reaches the budget |
| Version 7 fails and is parked, version 8 fails once a week later | 8 quarantines the instance on its first failure | 8 starts its own clock |
| Version 7 is parked, replayed after `quarantineAfter`, and fails again | measured from the first failure | quarantines the instance on that first replay failure |
| Version 7 fails while quarantine is off, quarantine is turned on a day later, and 7 fails again | a fresh budget from that failure | quarantines the instance on that failure when a day is past `quarantineAfter` |

The last two rows are intended, since the event has been failing for longer than the budget. Fix the cause before
replaying a parked event on an instance with quarantine on, or before turning quarantine on for a saga with failing
events.

### Stored saga state for the clock

Each pending version records when it first failed, in a `pendingSince` list beside `pending` in `streamMarks`. A
document without it falls back to the failure record's `firstFailedAt` for the event the record names, and any other
event starts its clock on its next failure.

## A quarantined saga instance can be released, and quarantine is on by default

In 0.34.0 nothing brought a quarantined saga instance back, so `SagaStateStore.delete(sagaId)` was the only way out, and
that is why quarantine was off by default. 0.35.0 adds `SagaSubscription.release(sagaId)`, which replays what the
instance skipped, and turns quarantine on by default again. The design is in
[ADR 178](../architecture/decisions/0178-a-quarantined-saga-instance-records-what-it-skips-and-is-released-by-replaying-it.md),
and the issue is [#1259](https://github.com/johanhaleby/occurrent/issues/1259).

### Who is affected

- A saga that never configured quarantine, built from `SagaRunnerConfig.defaults()`, run with a `SagaRunner.run`
  overload that takes no `SagaRunnerConfig`, or a `@Saga` with `occurrent.saga.quarantine-after` left out. It now
  quarantines an instance whose event keeps failing.
- A Spring Boot application with at least one `@Saga` that set `occurrent.saga.quarantine-after` blank to keep
  quarantine off. It fails at startup unless it sets `occurrent.saga.quarantine-enabled=false`.
- A `SagaRunner` you build yourself without an `EventStoreQueries`, when quarantine is on. It fails at startup.
- A Spring Boot application with no `EventStoreQueries` bean, or several and none of them `@Primary`, when quarantine
  is on. It fails at startup.
- A `SagaStateStore` of your own.
- An instance that 0.34.0 quarantined.
- A record pattern over `SagaFailure`, `SagaEnvelope` or `SagaRunnerConfig`.
- A `SpringMongoSagaStateStore` on a collection whose name ends in `_skipped_inputs`, which includes a `@Saga` whose
  id makes the default `saga-<id>` name end in it. The constructor throws an `IllegalArgumentException`, with
  quarantine on or off.
- A `SpringMongoSagaStateStore` whose `MongoOperations` reads from secondaries. The store now reads saga instances and
  skipped inputs from the primary.
- Every `SpringMongoSagaStateStore`, which creates a `<collection>_skipped_inputs` collection with two indexes on it
  when it is constructed, with quarantine on or off.
- An upgrade from 0.34.0, which stops every 0.34.0 node that runs sagas before any 0.35.0 node starts, and a rollback
  to 0.34.0, as [Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it) describes.

### What changes

`SagaRunnerConfig.defaults()` quarantines an instance once one event has kept failing for 5 minutes. The Spring Boot
starter does the same when `occurrent.saga.quarantine-after` is left out, and
`occurrent.saga.quarantine-enabled=false` turns it off. In 0.34.0 both left quarantine off. The 3- and 4-argument
`SagaRunnerConfig` constructors still never quarantine. With quarantine on, a 0.34.0 node that runs next to a 0.35.0
node can stall a saga's subscription or lose an event, so every 0.34.0 node that runs sagas stops before any 0.35.0
node starts, as [Upgrading from 0.34.0 and rolling back to it](#upgrading-from-0340-and-rolling-back-to-it) describes.
Turning quarantine off doesn't make it safe to run a 0.34.0 node next to a 0.35.0 node, since a 0.34.0 node also fails
on every `ACTIVE_MARKED` instance, as [the stored saga state for late versions](#stored-saga-state-1) describes.

A blank `occurrent.saga.quarantine-after` kept quarantine off in 0.34.0. In 0.35.0 the application fails at startup
on one, and the message says to set `occurrent.saga.quarantine-enabled=false` to keep quarantine off.

Quarantine still applies only on a subscription model that retains every event it delivers. On any other, and on a
push feed, `SagaRunner` turns it off each time the saga starts. It logs that at `WARN` when you set the budget, with
`withQuarantineAfter(..)`, a `SagaRunnerConfig` constructor or `occurrent.saga.quarantine-after`, as 0.34.0 did, and at
`DEBUG` when the budget is the default. `SagaRunnerConfig.disableQuarantine()` or
`occurrent.saga.quarantine-enabled=false` turns quarantine off without either line.

On a model that retains every event, `SagaRunner.run` throws an `IllegalStateException` when quarantine is on and the
runner has no `EventStoreQueries`, since `release` reads the skipped inputs back with it. That holds whether quarantine
comes from `SagaRunnerConfig.defaults()` or you set it yourself. Two kinds of application fail at startup this way:

- A plain Java application that builds its `SagaRunner` without an `EventStoreQueries`, with the defaults.
- An application that set quarantine explicitly in 0.34.0 and builds its `SagaRunner` without an `EventStoreQueries`.

The message says to pass the event store with `SagaRunner.eventStoreQueries(..)`, since every blocking event store
Occurrent ships implements `EventStoreQueries`, or to turn quarantine off with `SagaRunnerConfig.disableQuarantine()`.

In Spring Boot, a `@Saga` with quarantine on fails at startup when the context has no `EventStoreQueries` bean, or
several and none of them is `@Primary`. With none, the message names the saga id and says to define the event store as
a bean. With several, it names the saga id and the beans, and says to mark the one the saga reads from `@Primary`. Both
say to set `occurrent.saga.quarantine-enabled=false` to turn quarantine off. With quarantine off, or on a subscription model
that doesn't retain every event, the application starts.

While it is quarantined, an instance records the key of every input it skips, in the order they arrive. A skip used to
write nothing, and now writes the instance and the record together, in one transaction on MongoDB. The records are kept
outside the instance's document, so a quarantine can record any number of inputs.

`SagaSubscription.release(sagaId)` reads the event the instance stopped on from the event store and applies it, and
then does the same for each recorded input, in order. The instance stays `QUARANTINED` while that runs, and becomes
`ACTIVE` once no recorded input is left. It returns a `SagaSubscription.ReleaseResult`, and
[the quarantined saga runbook](../runbooks/quarantined-saga-instance.md#5-you-release-the-instance) says what each one
means and what to do next. In Spring Boot the saga's `SagaSubscription` is the `sagaSubscription-<id>` bean.

The timer poller goes on with a release that failed on an input or was interrupted. One poll replays at most
`timerBatchLimit` records, 100 by default, shared evenly among the releasing instances it lists, at least one each, and
the next poll goes on where the last one stopped. A `release` call has no such bound. The poller goes on with an
instance only once its `updatedAt` is a whole `timerPollInterval` old. A release writes the instance for every input it
applies and the first time an input fails, and an input the instance skips doesn't change `updatedAt`, so the poller
skips a release that is still running on any node. The poller and a release, or two polls, can still replay the same
input and dispatch its commands twice, in five cases, which
[the quarantined saga runbook](../runbooks/quarantined-saga-instance.md#5-you-release-the-instance) lists:

- A release takes longer than `timerPollInterval` over one input.
- A poller's clock is `timerPollInterval` or more ahead of the clock of the node that wrote.
- A `release` call made while the poller replays the instance goes on beside the poller.
- A release spends longer than `timerPollInterval` on inputs the stream marks show as applied. Its writes for them
  leave `updatedAt` as it is, so the poller goes on beside it, though neither dispatches anything until the first input
  that isn't applied.
- Two polls read the instance at the same time and both go on with it. Without a competing-consumer strategy every
  node polls, and with one, two nodes can poll at once while the timer lease moves from one to the other.

A timer that came due while the instance was quarantined fires after every recorded input has been replayed, because a
saga's timers run on processing time. For a saga that uses a timer as a deadline, a replayed input from after the
deadline that cancels or re-arms the timer means the timeout never fires.

Release reads the events with an `EventStoreQueries`. You give a `SagaRunner` one with `eventStoreQueries(..)`, and the
Spring Boot starter passes its own `EventStoreQueries` bean when the context has one, or several with one of them
primary.

`release` throws an `IllegalStateException` for an instance that 0.34.0 quarantined, since it recorded none of the
inputs it skipped, and for an instance whose state no longer decodes, with a message that names the decoding failure.
An instance whose state stops decoding while it is releasing stays releasing. On a store that can list the releasing
instances, each poll logs a `WARN` and tries again. The runbook covers what is left for those.

### Stored saga state for a release

`SagaCodec` stores a release in three fields, `failureReleasedAt`, `failureQuarantineId` and `quarantineSkipCount`,
which `SpringMongoSagaStateStore` writes as top-level fields of the instance's document. The skipped inputs are documents in a collection
of their own, named after the saga collection with `_skipped_inputs` appended, one per skipped input, each with
`sagaId`, `quarantineId`, `seq` and `key`. A document 0.34.0 wrote has none of these fields, and reads as an instance
that is not releasing, with no quarantine id. The constructor now throws an `IllegalArgumentException` for a saga
collection whose name ends in `_skipped_inputs`, because a store on the name without that suffix would keep its skipped
inputs in the same collection. Rename such a collection before you upgrade.

An instance that 0.35.0 quarantined is stored with the status `QUARANTINED_RECORDING` instead of `QUARANTINED`. Both
read back as `SagaStatus.QUARANTINED`, and the store's own queries match both. A query you wrote by hand for
`status: "QUARANTINED"`, and an alert built on one, doesn't find such an instance, so match both strings:

```javascript
db.getCollection("saga-order-fulfilment")
  .find({ status: { $in: ["QUARANTINED", "QUARANTINED_RECORDING"] } }, { state: 0 })
```

Quarantine needs a store that implements the new interface `SagaStateStoreSkippedInputs` next to `SagaStateStore`.
The in-memory store and `SpringMongoSagaStateStore` implement it. On a subscription model that retains every event,
`SagaRunner.run` throws an `IllegalStateException` when quarantine is on and the saga's store doesn't implement it.
The message names the interface and `SagaRunnerConfig.disableQuarantine()`. On the annotation path, a `@Saga` with
quarantine on, on a model that retains every event, whose `SagaStateStore` bean doesn't implement it fails at startup
with a message that names the interface and `occurrent.saga.quarantine-enabled=false`.

A store that implements it keeps the skipped inputs. It reads them with `findSkippedInputs`, and changes them only in
four compare-and-sets on the instance, `compareAndSaveSkipped`, `compareAndSaveReleaseStarted`,
`compareAndSaveReplayed` and `compareAndSaveQuarantineEnded`. Each writes the instance and its records in one atomic
write, and on a version conflict returns `false` and writes neither. In `delete(sagaId)` it deletes the records under
the quarantine id of the instance it removes, in the same atomic write as the instance. A call that returns normally
has to have made its write survive a failover, since the runner acknowledges the input right after it. It implements
`findReleasingSagaIds(after, limit)`, which returns the ids of the instances that are `QUARANTINED` with `releasedAt`
set, in ascending order, after the id `after` (all of them when it is `null`), and at most `limit` of them. The timer
poller asks for them on every poll and goes on with their release, starting each poll after the last id the previous
one listed and from the first id again once a poll lists fewer than `limit`. None of the six methods has a default.

A store that keeps the `StoredSaga` it's given as it is keeps the quarantine id and the skip count with it. A store
that maps each field to a column of its own has to persist both, or reading an instance it quarantined throws an
`IllegalStateException` that names the missing field.

### What to do

If you don't want quarantine, turn it off with `SagaRunnerConfig.defaults().disableQuarantine()` or
`occurrent.saga.quarantine-enabled=false`. A blank `occurrent.saga.quarantine-after` no longer keeps it off, and an
application with at least one `@Saga` fails at startup on one while quarantine is enabled.

When you build a `SagaRunner` yourself with quarantine on, call `eventStoreQueries(..)` with the event store the saga's
subscription reads from, or the runner refuses to start.

When your Spring Boot context has no `EventStoreQueries` bean, define the event store as one. When it has several,
mark the one the saga reads from `@Primary`. Otherwise the application fails at startup while quarantine is on.

When you have a `SagaStateStore` of your own and quarantine on, on a model that retains every event, implement
`SagaStateStoreSkippedInputs` on it, as [Stored saga state for a release](#stored-saga-state-for-a-release) describes,
or turn quarantine off with `SagaRunnerConfig.disableQuarantine()` or `occurrent.saga.quarantine-enabled=false`.
Otherwise the application refuses to start.

An instance that 0.34.0 quarantined can't be released. Delete it as
[the quarantined saga runbook](../runbooks/quarantined-saga-instance.md#6-you-delete-the-instance-once-you-have-decided-not-to-recover-it)
describes, once you have decided not to recover it.

Add the new bindings to a record pattern over `SagaFailure`, `SagaEnvelope` or `SagaRunnerConfig`. No recipe does it.

Rename a `SpringMongoSagaStateStore` collection whose name ends in `_skipped_inputs`, or the `@Saga` id behind it. Let
the MongoDB user the store connects with create the `<collection>_skipped_inputs` collection and its indexes. When you
sent saga reads to secondaries to spare the primary, expect that load on the primary now.

### Upgrading from 0.34.0 and rolling back to it

Upgrade the nodes that run sagas with a stop-then-start deploy. Stop every 0.34.0 node that runs sagas before any
0.35.0 node starts. That takes the service that hosts the sagas down between the two.

A 0.35.0 node can quarantine an instance and store it as `QUARANTINED_RECORDING`. A 0.34.0 node throws on every event
addressed to such an instance, because it decodes the status with `SagaStatus.valueOf`. Its timer poller never selects
such an instance, since it asks for `ACTIVE` ones only, and `findByStatus` on 0.34.0 never returns one, while
`SagaInstances.find(sagaId)` throws for it. The same holds for an `ACTIVE_MARKED` instance, which
`findByStatus(ACTIVE, ..)` on 0.34.0 never returns. Both statuses are there so that a deploy that breaks the rule
fails loudly on the 0.34.0 node, instead of that node skipping the input without a record or dropping the instance's
stream marks, but neither keeps the input. When the instance gets its events from a Kafka or RabbitMQ source with
`PARK`, the source parks the event the 0.34.0 node fails on and acknowledges it. A `PushSubscriptionModel` throws the
failure back to the code that pushed the event, and the instance never gets it unless that code pushes it again. The
0.34.0 node records nothing, so a release doesn't replay such an event. Publish each parked message again, or push
each event that failed again, once every saga node runs 0.35.0, as
[the stored saga state for late versions](#stored-saga-state-1) describes.

The rest of this section says what happens when a 0.34.0 node runs next to a 0.35.0 node anyway, and after a rollback.
Every subscription model a saga can quarantine on writes its position or checkpoint only after the action returns, so
the event the 0.34.0 node fails on, and everything after it, is still ahead of the stored position and is delivered
again.

On a live MongoDB subscription the 0.34.0 node retries the event in place with the subscription model's retry
strategy, which by default backs off from 100 ms to 2 s and never gives up. In a catch-up replay that a subscribe
started, the replay fails and the subscription doesn't go live on that node until the node restarts, which its
upgrade does. A replay that a resume started runs again from its last stored checkpoint with the same backoff.

The 0.34.0 node keeps the saga's competing-consumer lease while it retries, because the lease is refreshed on a
schedule of its own whatever the subscription is doing, so the saga's subscription stalls. The node gives the lease up
when it is stopped for its upgrade, but the next node to take it may still run 0.34.0 and stall on the same event, so
the stall ends once a 0.35.0 node holds the lease. It also ends when a 0.35.0 node deletes the instance, because the
0.34.0 node reads the instance again on every retry and a deleted one is not there to decode. A release doesn't end
it, because a released instance keeps the stream marks of every event with a stream version it has handled, so it is stored
as `ACTIVE_MARKED`, which 0.34.0 can't decode either, as [the stored saga state for late versions](#stored-saga-state-1) describes. A catch-up
replay that a subscribe started is not retried, so a node whose replay failed doesn't go live until it restarts,
whatever happens to the instance.

Without a competing-consumer strategy every node runs the saga's subscription. The 0.35.0 nodes handle the event, and
only the 0.34.0 node's own subscription stalls, until the node is upgraded or the instance is deleted.

The subscription delivers one event at a time, so the stall holds up every later event of that saga subscription, for
every instance of the saga, not only the events of the quarantined instance.

After a rollback to 0.34.0 there is no 0.35.0 node left to take the lease, so the stall lasts until the instance is
stored with a status 0.34.0 reads, as step 6 does, or deleted, with competing consumers or without. It ends then
because the 0.34.0 node reads the instance again on every retry, and no 0.35.0 node is left to store it as
`QUARANTINED_RECORDING` or `ACTIVE_MARKED` again. A node whose catch-up stopped on the event also has to restart before
its subscription goes live. To downgrade without that stall, do steps 1 to 5 while the nodes run 0.35.0:

1. Turn quarantine off on every node, with `SagaRunnerConfig.disableQuarantine()` or
   `occurrent.saga.quarantine-enabled=false`, and have that running on every node before step 2. A node that still has
   quarantine on can quarantine another instance after you have listed them.
2. List the quarantined instances and call `release` on each whose failure record has a quarantine id. On MongoDB,
   `db.getCollection("saga-order-fulfilment").find({ status: "QUARANTINED_RECORDING" }, { _id: 1 })`, with your saga's
   collection, lists exactly those, and its cursor reaches every one. On another store,
   `findByStatus(QUARANTINED, ..)` returns at most `limit` instances and has no cursor, so you know a call listed them
   all only when it returns fewer than its `limit`. Raise the limit until it does.
3. When a release returns `STILL_RELEASING`, an input failed again. With quarantine off there is no budget to run out,
   so the instance stays releasing and the timer poller retries the input until it succeeds. Fix what makes the input
   fail and call `release` again. Don't delete the instance while a 0.35.0 node runs. `SagaStateStore.delete(sagaId)`
   takes no expected version, so a delete that reaches the store after the poller has finished the release deletes
   the released instance.
4. When a release throws, the instance stays quarantined. Fix the cause the exception names, such as a state that no
   longer decodes or an event store read that failed, and release the instance again.
5. Repeat steps 2 to 4 until every instance step 2 lists has a release that returned `QUARANTINED_AGAIN` or
   `QUARANTINED_EVENT_MISSING`, or that keeps failing for a cause you can't fix.
6. Stop every 0.35.0 node. Then, from a process that runs no `SagaRunner`, store every instance that is still
   `QUARANTINED_RECORDING` as `QUARANTINED`, without its quarantine id, skip count and release time, and every
   `ACTIVE_MARKED` instance as `ACTIVE`. A released instance is `ACTIVE_MARKED` when it holds stream marks. On MongoDB,
   run this on each saga's collection:

   ```
   db.getCollection("saga-order-fulfilment").updateMany(
     { status: "QUARANTINED_RECORDING" },
     { $set: { status: "QUARANTINED" }, $unset: { failureQuarantineId: "", quarantineSkipCount: "", failureReleasedAt: "" } })
   db.getCollection("saga-order-fulfilment").updateMany({ status: "ACTIVE_MARKED" }, { $set: { status: "ACTIVE" } })
   ```

   Before the second update, list the instances that hold a version 0.34.0 may never apply, with the query in
   [the stored saga state for late versions](#stored-saga-state-1), which also says what else 0.34.0 loses. A store of your own needs the
   same change in the form it stores. With no `SagaRunner` running, nothing releases an instance or delivers it an
   input while the updates run.
7. Start the 0.34.0 nodes.

No step deletes an instance. 0.34.0 reads an instance that step 6 updated as one it quarantined itself, with its state
and the input it stopped on. It skips every input addressed to the instance and fires none of its timers, and the
instance stays quarantined until you delete it, as in 0.34.0. After a later upgrade to 0.35.0, `release` refuses it,
because 0.34.0 skipped inputs without recording them. The records of the inputs it skipped on 0.35.0 stay in
`<sagaCollection>_skipped_inputs`, where nothing reads them. They take up storage and nothing else.

## A saga timer that keeps failing waits behind the timers already due

Up to 0.34.0 the timer poll took at most `timerBatchLimit` due instances in any order, and a timer whose reaction threw
stayed due at the same `firesAt`. Once `timerBatchLimit` instances kept failing their timers, no other instance's timer
fired. The reason for the change is in
[ADR 173](../architecture/decisions/0173-a-failing-saga-timer-is-tried-again-after-the-timers-already-due.md), and the
report is [#1003](https://github.com/johanhaleby/occurrent/issues/1003).

### Who is affected

- Code that uses a record pattern over `SagaEnvelope.TimerEntry`, or compares entries with `equals`.
- A `SagaStateStore` of your own.
- Anything that reads the MongoDB field `nextTimerFiresAt` directly, for example a monitoring query.

### What changes

- `TimerEntry` gains `nextAttemptAtEpochMilli`, which is null until the timer fails. When a timer's reaction throws, the
  poller writes the instant just after its poll there. A timer is due at the later of `firesAt` and that instant.
- `findWithDueTimers` returns the earliest due instance first. Instances due at the same instant come back in no
  particular order.
- On MongoDB, a failed timer's subdocument holds `nextAttemptAtEpochMilli`, and `nextTimerFiresAt` is the earliest due
  time across the instance's timers. For a timer that keeps failing that is its last failed attempt, not its `firesAt`.
- `SagaInstance.nextTimerAt()` still returns the earliest `firesAt`, so an instance stuck on a failing timer still
  shows a time in the past.
- The write that records a failed timer keeps the instance's version. It reaches `compareAndSaveWithoutState` with a
  `StoredSaga` whose version equals `expectedVersion`. Every other save still raises the version by one.

### What to do

- Add the third component to a record pattern over `TimerEntry`. The two-argument constructor still compiles and
  sets it to null.
- In a store of your own, return `findWithDueTimers` ordered by `SagaIndex.nextTimerDueAtEpochMilli()`. A store that
  ignores the order keeps working, with the 0.34.0 order.
- In a store of your own, store the version of the `StoredSaga` as it comes, even when it equals `expectedVersion`. A
  store that refuses that save refuses every record of a failed timer, so the poller logs a `WARN` for each failed fire
  and the instance keeps the 0.34.0 order. A store that sets the version itself, for example with a JPA `@Version`
  column, raises it on the record too, and an event that takes longer than the poll interval between its read and its
  save can then lose its compare and set on every attempt and fail with `SagaConcurrencyException`.
- Point a monitoring query at `SagaInstance.nextTimerAt()` rather than at `nextTimerFiresAt`, if it looks for instances
  whose timers have been due for a long time.

### Stored saga state

Nothing to migrate. A 0.34.0 document has no `nextAttemptAtEpochMilli`, which reads as a timer that has not failed,
and its `nextTimerFiresAt` is already the earliest due time. 0.34.0 reads only `name` and
`firesAtEpochMilli` from a timer subdocument, so it ignores the new field, and a 0.34.0 write drops it, which gives
that instance the 0.34.0 order again. A 0.34.0 node never saves an instance stored as `ACTIVE_MARKED`, see
[the stored saga state for late versions](#stored-saga-state-1).

## A `SagaStateStore` stores an encoded saga

Up to 0.34.0 a `SagaStateStore` took a `SagaEnvelope` and decided how to write each of its fields. Every field a
release added, such as the stream marks and the quarantine id of 0.35.0, was then one more field a store of your own
had to save and read back. In 0.35.0 `SagaCodec` turns the instance into a `StoredSaga`, and the store persists that
as it is. The design is in
[ADR 180](../architecture/decisions/0180-occurrent-owns-the-stored-form-of-a-saga-instance.md).

### Who is affected

- A `SagaStateStore` of your own. Its methods take and return `StoredSaga` instead of `SagaEnvelope`, and it has to
  implement the new `stateCodec()`, so it stops compiling.
- Code that calls `find`, `findWithoutState`, `findWithDueTimers` or `SagaStateStoreQueries.findByStatus` on a store
  directly, for example a test that reads an instance back from `SagaStateStore.inMemory()`. These return `StoredSaga`.
- Code that calls `compareAndSave` or `compareAndSaveWithoutState` on a store directly, for example a test that seeds
  an instance. These take a `StoredSaga`, and `compareAndSaveWithoutState` throws an `IllegalArgumentException` at
  version 0, where 0.34.0 inserted the instance. Insert it with `compareAndSave`.
- A flow saga on `SagaStateStore.inMemory()`.
- A test on `SagaStateStore.inMemory()` that compares an instance's timestamps with a clock finer than a millisecond.
  The store now keeps `createdAt`, `updatedAt`, `completedAt` and the failure's timestamps to the millisecond, and
  `findByStatus` doesn't return an instance updated in the same millisecond as `updatedBefore`.
- A flow saga on a `SpringMongoSagaStateStore` you build yourself with a `CloudEventConverter` other than the runner's.
  0.34.0 wrote the received events with the store's converter, and 0.35.0 reads them with the runner's, so an instance
  0.34.0 wrote is read through a converter that never wrote it. The Spring Boot starter gives both the same converter
  bean.
- A saga whose initial state is a `FlowState` of your own, which `SagaRunner.run` now refuses.

An application that only hands `SpringMongoSagaStateStore` or `SagaStateStore.inMemory()` to the runner compiles
unchanged.

### What changes

`SagaCodec` is in the `org.occurrent.dsl.saga.codec` package of `occurrent-saga-dsl-common`. Occurrent encodes every
instance with it before a save and decodes it after a read, on every store. A `StoredSaga` has four components:

| Component | What it holds |
| --- | --- |
| `sagaId` | The saga instance id |
| `version` | The compare-and-set version |
| `encoded` | An `EncodedSaga`, the fields of the instance by name, in the order a store should write them |
| `index` | A `SagaIndex`, the values a store queries on, `storedStatus`, `nextTimerDueAtEpochMilli`, `updatedAtEpochMilli`, `releasing` and `quarantineId` |

A store decides only how the state of a core saga is represented, and returns that as a `SagaStateCodec` from
`stateCodec()`. `SagaCodec` writes the state of a flow saga itself, with its received events as `CloudEvent` values
that the runner's `CloudEventConverter` makes.

The methods change like this:

| 0.34.0 | 0.35.0 |
| --- | --- |
| No such method | `SagaStateCodec<S> stateCodec()` |
| `Optional<SagaEnvelope<S>> find(String sagaId)` | `Optional<StoredSaga> find(String sagaId)` |
| `default Optional<SagaEnvelope<S>> findWithoutState(String sagaId)` | `Optional<StoredSaga> findWithoutState(String sagaId)`, with no default |
| `boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion)` | `boolean compareAndSave(StoredSaga saga, long expectedVersion)` |
| `default boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion)` | `boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion)`, with no default |
| `List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit)` | `List<StoredSaga> findWithDueTimers(Instant now, int limit)` |
| `List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit)` on `SagaStateStoreQueries` | `List<StoredSaga> findByStatus(SagaStatus status, Instant updatedBefore, int limit)` |

`delete(String sagaId)` is unchanged.

`SagaStateStore.inMemory()` keeps the state object as it is and every other field in the form `SagaCodec` gives it. A
flow saga on it converts every received event to a `CloudEvent` on each save and back on each read, so a step that
keeps many events costs more per save than in 0.34.0.

`SagaRunner.run` throws an `IllegalArgumentException` for a saga whose initial state is a `FlowState` other than the
one the flow executor creates, before it subscribes, on `SagaStateStore.inMemory()` too. 0.34.0 ran such a saga on the
in-memory store.

The `CloudEventConverter` you pass to the `SpringMongoSagaStateStore` constructor only marks the store as a flow
saga's store. The runner's converter writes and reads the received events. A `null` converter with `FlowState.class`
as the state type still throws an `IllegalArgumentException`.

`SagaCodec` logs the warning for a flow saga instance that retains 1,000 received events, so you get it on every store.
A logger setting for that warning goes on `org.occurrent.dsl.saga.codec.SagaCodec`, not on `SpringMongoSagaStateStore`.

### What to do

Port a store of your own in these steps. `InMemorySagaStateStore` and `SpringMongoSagaStateStore` implement each of
them.

1. Implement `stateCodec()`. Return `SagaStateCodec.identity()` when the store keeps the state object as it is, or a
   codec that turns the state into a value your database can write and back, the way `SpringMongoSagaStateStore` does
   with the application's `MongoConverter`. The codec must not return `null` for a state, because a save whose codec
   does throws a `NullPointerException` before anything is stored.
2. In `compareAndSave`, keep the compare-and-set rule, where an `expectedVersion` of 0 inserts. Store `saga.sagaId()`,
   `saga.version()` and every field of `saga.encoded().fields()`. Store the version as it comes, also when it equals
   `expectedVersion`.
3. Store each value so it reads back as the same type. A value is a `String`, `Integer`, `Long`, `Boolean`, `Instant`
   or `CloudEvent`, or a `List` or `Map` of those, and the `state` field holds whatever your `SagaStateCodec` returned.
4. In `compareAndSaveWithoutState`, under the same compare-and-set rule, set each field of
   `SagaCodec.fieldsExceptState()` that `saga` has a value for, remove the ones it has none for, and touch no other
   field. The stored state and `currentStep` stay as they are. Throw an `IllegalArgumentException` for an
   `expectedVersion` of 0, since the runner inserts with `compareAndSave`.
5. Build what you read as `new StoredSaga(sagaId, version, encoded, SagaCodec.indexOf(encoded))`. `find` reads every
   field. `findWithoutState` needs only `SagaCodec.fieldsWithoutState()`, and `findWithDueTimers` and `findByStatus`
   need only `SagaCodec.instanceFields()`, so those three can leave the state unread.
6. Query on the index. `findWithDueTimers` returns the instances whose `storedStatus` is in
   `SagaCodec.storedStatuses(SagaStatus.ACTIVE)` and whose `nextTimerDueAtEpochMilli` is at or before `now`, earliest
   first. `findByStatus` matches `SagaCodec.storedStatuses(status)`, with the same `updatedBefore` filter and order as
   before. `findReleasingSagaIds` returns the instances whose `releasing` is `true`, and `delete` removes the skipped
   inputs under the deleted instance's `quarantineId`.

Each index value comes from one or two of the encoded fields, so a store can index and query those fields instead.
`SagaCodec.indexOf` shows which ones, and `SagaCodec` has a constant for each field name, such as `SagaCodec.STATUS`
and `SagaCodec.NEXT_TIMER_FIRES_AT`.

Code that read an instance from a store directly can read its lifecycle through `SagaInstances` instead. When it needs
the `SagaEnvelope`, decode the `StoredSaga` with a codec built from the store:

```java
SagaCodec<OrderFulfilment> codec = SagaCodec.of(stateStore.stateCodec(), null);
Optional<SagaEnvelope<OrderFulfilment>> envelope = stateStore.find("order-4711").map(codec::decode);
```

Code that wrote an envelope to a store directly encodes it with the same codec first:

```java
stateStore.compareAndSave(codec.encode(envelope), 0);
```

`encode` throws an `IllegalStateException` for an envelope quarantined with a quarantine id and no `quarantineSkipCount`, with the
message a read of such an instance gives, since no read could take that instance back.

For a flow saga, pass the application's `CloudEventConverter` instead of `null`. A codec built with `null` throws
`IllegalArgumentException` when it encodes a `FlowState`.

### Stored saga state

`SpringMongoSagaStateStore` writes the same document as before, with the same field names, value types and field
order, so there's nothing to migrate. A store of your own that holds data in a layout of its own has to read that data
into the fields `SagaCodec` expects, or migrate it.

### Why there is no recipe

`UpgradeToOccurrent_0_35` doesn't port a store, since how a store keeps the fields and queries the index depends on its
database.

## Saga writes on MongoDB wait for a majority to journal them

`SpringMongoSagaStateStore` writes every saga instance at write concern `majority` with `j: true`, whatever write
concern your `MongoTemplate` has, and reads it at read concern `majority`. 0.34.0 wrote and read the instance at the
template's concerns. A saga acknowledges its input as soon as the write returns, so a write that a failover rolls back
loses the input.

A quarantined instance and the inputs it skips now change together, in one MongoDB transaction, so quarantine on
MongoDB needs a replica set.

### Who is affected

- An application whose `MongoTemplate`, client, database or server default sets a write concern below `majority`, such
  as `w: 1`. Saga writes no longer use it.
- A replica set of a primary, a secondary and an arbiter. With the secondary down, a majority can't journal a write, so
  every saga write fails, where `w: 1` succeeded.
- An application that runs sagas with quarantine on, the default in 0.35.0, against a standalone MongoDB server. The
  first input a quarantined instance skips fails, since the server refuses the transaction.
- Code that builds a `SpringMongoSagaStateStore` with a `MongoOperations` that isn't a `MongoTemplate`, such as a
  wrapper or a mock.
- An application of the Spring Boot starter with a `MongoOperations` bean of its own that isn't a `MongoTemplate`,
  such as a decorator marked `@Primary`. The starter now builds the store of a `@Saga` on the `MongoTemplate` bean, as
  it builds the event store, so saga reads and writes no longer go through the decorator. Spring Boot creates no
  `MongoTemplate` bean when the application declares a `MongoOperations` bean, so an application with no
  `MongoTemplate` bean of its own fails to start its sagas.

### What changes

`compareAndSave` and `compareAndSaveWithoutState` stay a single-document write each, and wait until a majority of the
replica set has the write in its on-disk journal. When a majority doesn't acknowledge it within 10 seconds, the write
throws, and outside a transaction of yours the store's `RetryStrategy` tries it again. On a client with a `timeoutMS` above 0, where the driver sends
no `wtimeout`, your `timeoutMS` bounds each attempt instead. A write that timed out can still be on the primary. Reads
at read concern `majority` don't return it until a majority has it, so the saga doesn't act on it before then.

`find` and `findWithoutState` read an instance they don't find at `majority` again at read concern `linearizable`,
with a `maxTimeMS` of 10 seconds. MongoDB documents that such a read returns every write a majority acknowledged before
it started, in [read concern "linearizable"](https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/). Right after a failover, a
majority read on the new primary can miss an instance the old primary wrote, and the saga would then skip an input
addressed to it. So the first input of a new instance costs a second read.

The four writes of `SagaStateStoreSkippedInputs` and the `delete` of a quarantined instance run in a transaction that
commits at `majority` with `j: true` and reads at read concern `snapshot`. A commit that a majority doesn't acknowledge
fails after 10 seconds, or after your `timeoutMS` when it is above 0. On a client with a `timeoutMS` of 0 the commit
waits without limit, since the driver then sends no `wtimeout` and `MongoTransactionManager` passes it no transaction
timeout. The store retries a transaction that fails with the `TransientTransactionError` label, and commits again after
an `UnknownTransactionCommitResult`, both with its `RetryStrategy`. The store throws any other failure of the
transaction at once, whatever the strategy retries, and that includes a strategy you implement yourself. A standalone
server refuses the transaction, and the store throws an `IllegalStateException` on the first attempt that says it
needs a replica set and that a single-member one is enough.

Inside a MongoDB transaction of yours on the template's `MongoDatabaseFactory`, those writes join your transaction
instead of starting their own. In such a transaction the store sends each read and write once, whatever `RetryStrategy`
it has, and a failure reaches you as MongoDB reported it. MongoDB aborts a transaction when an operation in it fails, so
the same operation sent again in it fails too. 0.34.0 retried every read and write with the store's `RetryStrategy`,
inside your transaction too. Retry by running your transaction again. That is a Spring transaction run by a
`MongoTransactionManager`, a Spring transaction of another resource in which a template with
`SessionSynchronization.ALWAYS` has started a MongoDB transaction, or a transaction on the session of a template from
`MongoTemplate.withSession`. Your commit's write concern then decides whether they survive a failover. The store can't
read that concern, so it doesn't check it. When a store call fails inside your transaction, abort the transaction, since
a commit after the failure may keep what the store wrote before it. With a `MongoTransactionManager` set to
`SYNCHRONIZATION_NEVER`, each store call takes part in your transaction with synchronization on for the call, so it
reads and writes in your transaction as well.

In such a transaction, `find` and `findWithoutState` read at your transaction's snapshot, which misses an instance
stored after it began. When the read finds no instance, the store inserts a document that holds only the saga's id and
deletes it again in your transaction. An
instance another writer stored after your snapshot began makes the insert fail with a `WriteConflict` labelled
`TransientTransactionError`. Run your transaction again on that label, as you would for any write conflict, and the next
attempt reads the instance. An instance your transaction deleted reads as absent. The two writes leave no document
behind when your transaction commits, and a change stream on the saga collection may see them. The saga collection
must accept a document that holds only `_id`, so a validator that requires other fields makes such a read of an absent
instance fail.

Every constructor throws an `IllegalArgumentException` for a `MongoOperations` that isn't a `MongoTemplate`, since the
store runs its transactions on the template's `MongoDatabaseFactory`. The message says to pass the `MongoTemplate`
bean.

### What to do

- On a replica set of a primary, a secondary and an arbiter, replace the arbiter with a data-bearing member, or expect
  saga writes to fail while the secondary is down.
- On a standalone server, run MongoDB as a replica set, which can have a single member, or turn quarantine off with
  `SagaRunnerConfig.disableQuarantine()` or `occurrent.saga.quarantine-enabled=false`.
- Pass the `MongoTemplate` bean to `SpringMongoSagaStateStore`. The Spring Boot starter already does.
- With the starter and a `MongoOperations` decorator, declare the `MongoTemplate` it wraps as a bean.
- Run the transitions outside a transaction of yours, or commit yours at `majority` with `j: true`.

## The shorter `SagaEnvelope` and `SagaFailure` constructors are removed

`SagaEnvelope` and `SagaFailure` keep only their canonical constructors.

### Who is affected

Code that builds a `SagaEnvelope` or a `SagaFailure`, which is usually a test. Three calls written against 0.34.0 stop
compiling:

- the 13-argument `SagaEnvelope` constructor, which was the canonical one in 0.34.0
- the deprecated 11-argument `SagaEnvelope` constructor
- the 5-argument `SagaFailure` constructor, which was the canonical one in 0.34.0

### What to do

Pass the components the removed constructor filled in:

| Call | Add | For an instance with nothing in them |
| --- | --- | --- |
| `SagaEnvelope` with 13 arguments | `streamMarks` and `quarantineSkipCount`, after `failure` | `Map.of()` and `null`, or a count for a `QUARANTINED` envelope whose failure has a quarantine id |
| `SagaEnvelope` with 11 arguments | `started`, `failure`, `streamMarks` and `quarantineSkipCount`, after `currentStep` | `true`, `null`, `Map.of()` and `null` |
| `SagaFailure` with 5 arguments | `releasedAt` and `quarantineId`, after `failureMessage` | `null` and `null` |

```java
// Before, stops compiling
new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, createdAt, updatedAt, null, null, true, null);

// After
new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, createdAt, updatedAt, null, null, true, null, Map.of(), null);
```

No recipe adds them.

## The deprecated `accept(..)` on the projection feeds is removed

`accept(E)` and `accept(EventMetadata, E)` are gone from the blocking and the reactor `DomainEventFeed` and
`CatchupProjectionFeed`. 0.34.0 deprecated them for removal and made each forward to `acceptAndWait(..)`, as
[section 12 of the 0.34.0 guide](upgrading-to-0.34.0.md#12-a-projection-feeds-accept-waits-until-the-event-is-applied-and-fails-when-it-is-not)
describes. The removal is [#1276](https://github.com/johanhaleby/occurrent/issues/1276).

### Who is affected

Code that calls `accept(..)` on one of those four types. `accept(..)` on a `PushSubscriptionModel` or a
`CatchupThenPushSubscriptionModel` is a different method and stays.

### What to do

Run `UpgradeToOccurrent_0_34` while the project still depends on 0.33.0 or 0.34.0, and then move the dependency to
0.35.0. The 0.34.0 guide shows how to run the recipe. It renames both `accept(..)` overloads on the four types to
`acceptAndWait(..)`, and does not touch an `accept(..)` on any other type. The renamed call does what the deprecated one
did in 0.34.0. Section 12 of the 0.34.0 guide covers how that differs from 0.33.0.

`acceptAndWait(..)` first appears in 0.34.0. A project on 0.33.0 stops compiling after the rename until the dependency
moves up, so change the dependency right after the recipe has run.

If the dependency is already 0.35.0, the build fails on each `accept(..)` call and the recipe no longer finds it, so
rename each call to `acceptAndWait(..)` by hand. A broker listener that can have a message delivered again can call
`acceptOrDefer(..)` instead, which the same section covers.

### Why there is no recipe in `UpgradeToOccurrent_0_35`

`UpgradeToOccurrent_0_34` renames a call only when the compiler has resolved it to `accept(..)` on one of the four
feeds. Once the type has no `accept(..)`, the call resolves to nothing and the recipe does not change it.
`ProjectionFeedAcceptRenameTest` tests this against a feed that declares only `acceptAndWait(..)` and
`acceptOrDefer(..)`. A recipe that runs after the upgrade has no type to match on, so `UpgradeToOccurrent_0_35` has
nothing to add, and the four entries stay in `UpgradeToOccurrent_0_34`.

## A RabbitMQ source consumes again after a recovery without topology recovery

A `RabbitMqCloudEventSource` or `RabbitMqDomainEventSource` on a connection with automatic recovery and topology
recovery turned off stopped consuming after a recovery in 0.34.0. From 0.35.0 it registers a new consumer
once the recovery has finished and consumes again. The reason is in
[ADR 153](../architecture/decisions/0153-a-rabbitmq-source-registers-a-new-consumer-when-a-recovery-leaves-it-without-one.md),
and the report is [#1066](https://github.com/johanhaleby/occurrent/issues/1066).

Nothing in your code changes. If you restart the source after a recovery, or turned topology recovery on only to keep
the source consuming, you can stop doing that. With topology recovery on, the source keeps the consumer the client
registered and starts no second one.

A source now stops, with a line at `error`, in three cases where 0.34.0 kept it running. Closing its channel puts the
delivery back on the queue.

- **A `TrafficListener` or `MetricsCollector` throws while the source sends a negative acknowledgement for a held
  delivery.** The source can't tell whether it went out, and sending it twice makes the broker close the channel. In
  0.34.0 the next poll sent it again.
- **A negative acknowledgement or an acknowledgement fails with an `IOException` on a connection with automatic
  recovery, and a poll interval later the connection still hasn't dropped.** A `TrafficListener` or `MetricsCollector`
  written in Kotlin can throw one while the connection is fine. So can NIO, which throws `Frame enqueuing failed` on a
  connection a memory alarm has blocked, for example one a source shares with a sink that publishes during the alarm.
  In 0.34.0 the source kept running. It sent the negative
  acknowledgement again on the next poll, and left a delivery whose acknowledgement never went out unanswered, so at
  the default `prefetchCount` of one the broker sent the source nothing further.
- **Its channel is closed for good.** The broker closed the channel on its own, for example with
  `406 PRECONDITION_FAILED` after a delivery outlived `consumer_timeout`, or a connection without automatic recovery
  closed the client's own channel. In 0.34.0 a source holding a delivery tried to release it again and logged a warning
  on every poll, and one holding none consumed nothing and logged nothing. A source holding none stops too, and when the
  application closed the channel or its connection, the line is at `warn`. On a channel that wraps the client's own and
  isn't `Recoverable`, a connection close that amqp-client's default recovery-triggering condition accepts doesn't stop
  the source, as in 0.34.0, whether or not the connection under the wrapper recovers, since a recovery there may reopen
  the channel. The source logs a warning when it's built on such a channel, and if no recovery reopens it, the source
  stays up and consumes nothing.

An `IOException` because the connection dropped doesn't stop the source. It keeps running and consumes again after the
recovery.

The source judges recovery by amqp-client's default recovery-triggering condition, since the client doesn't expose the
one a connection was built with. If you give a `ConnectionFactory` a condition of your own with
`setConnectionRecoveryTriggeringCondition`, the source stays up and consumes nothing after a close your condition
rejects and the default accepts. [#1353](https://github.com/johanhaleby/occurrent/issues/1353) proposes letting you give
the source your condition.

Two log lines of the delivery failure policy have new text, and a search or an alert on the old one stops matching.

| Before | From 0.35.0 |
|---|---|
| `Parked delivery tag {} to exchange "{}" routing key "{}" and acknowledged the original; nothing consumed it.` | `Parked a copy of delivery tag {} to exchange "{}" routing key "{}" and sent the acknowledgement for the original.` |
| `Redelivered delivery tag {} with requeue; nothing consumed it.` | `Sent a negative acknowledgement with requeue for delivery tag {} after it failed.` |

Both are still logged at `warn`. After a recovery the client ignores an acknowledgement for a tag from before it, so the
old text said more than the source knew.

## The reactive `executeAndReturnState` emits a non-null state

`executeAndReturnState` on `ReactiveSnapshotDeciderApplicationService` and `ReactiveSnapshotDcbDeciderApplicationService`
returns `Mono<@NonNull S>` instead of `Mono<S>`. The state type `S` may still be nullable. The new signature
changes nothing at runtime. A `null` state still writes none of the command's events and fails with a
`NullPointerException`. See
[#1119](https://github.com/johanhaleby/occurrent/issues/1119).

### Who is affected

Kotlin code that writes the result type of the call by hand as `Mono<State?>`. Kotlin reads the annotation, and a
`Mono<State>` can't be assigned to a `Mono<State?>`. Java code, and Kotlin code that lets the compiler infer the type,
compile unchanged.

### What to do

Drop the `?` from the hand-written `Mono<State?>`. There is no OpenRewrite recipe, since the compiler reports every
place that needs it.

## DCB reads can resume from marker versions

On MongoDB a DCB append reserves its position before its transaction commits, so an append holding 41 can commit after
the append at 42. A read after position 42 never returns 41, and a decider or view that resumes from a position it saw
loses that append ([#1230](https://github.com/johanhaleby/occurrent/issues/1230)). 0.35.0 lets a read resume from
marker versions instead, as [ADR 148](../architecture/decisions/0148-dcb-snapshots-resume-from-marker-versions-not-positions.md)
describes.

What is new:

- `read(criteria, DcbReadOptions.withVersions())` reports `DcbEventStream.markerVersions()`, the epoch and version of
  each marker of the criteria, read at the same point in time as the events. It reports them only for a full forward
  read, with no limit, skip or position bound, and criteria other than `MatchAll`.
- A conditional append with a consistency token reports the same in `DcbAppendResult.markerVersions()`.
- `read(criteria, DcbReadOptions.afterVersions(saved))` returns the matching events appended after `saved`, including
  one that committed late below a position an earlier read saw. A failover can make it miss an event, as [What else a
  token holds on MongoDB](#what-else-a-token-holds-on-mongodb) says. When the store can't compare `saved` with its own
  markers, for example after they were pruned, it throws `DcbSnapshotInvalidException`, and you read the boundary in
  full. The versions hold the criteria they were read with, and a read on other criteria throws the same exception.
  Criteria are the same when they hold the same alternatives in any order, the rule a consistency token uses, so
  versions from `tags(a)` are refused on `tags(a).excludingTypes("Y")`, and versions from `types("A", "B")` on
  `anyOf(type("A"), type("B"))`.
- `DcbAppendConfirmation.isConfirmed(result)` answers whether an append is committed at majority, read outside any
  transaction of yours. The MongoDB stores and `InMemoryEventStore` implement it.

What changes on MongoDB without you doing anything:

- Every DCB append counts its marker increments in a new field `s`, gives each marker document an `epoch`, and writes an
  `mv` field into each event with the epoch and version of each of its markers. A read never returns `mv` as part of a
  CloudEvent. The store creates an index on `mv` at start, together with the other DCB indexes.
- A read that asks for versions runs in a short read-only transaction of its own on the primary. Its commit waits at
  most `markerVersionsCommitTimeout`, 5 seconds by default, a new setting in each MongoDB `EventStoreConfig`. When the commit fails,
  `withVersions()` returns the events without versions and `afterVersions(..)` throws `DcbSnapshotInvalidException`.
- `resumeFromMarkerVersions(false)` turns resuming off. No read or append reports versions, `afterVersions(..)` throws,
  and the store logs a warning at start. `s`, `epoch` and `mv` are still written, so turning it back on is safe.
- Inside a session or transaction of yours, such as Spring's `@Transactional` or a native ambient session, a read
  reports no versions and `afterVersions(..)` throws `DcbSnapshotInvalidException`. Your transaction may still abort,
  and versions that counted its increments would skip the append that takes those versions next. Read the boundary in
  full there.

The upgrade needs no manual step, provided nothing replaces the document of an event a 0.35 node wrote. That rules out
a 0.34 node's `updateEvent`, a 0.34 tool, or your own code replacing event documents, during a rolling upgrade, after a
rollback to 0.34 followed by a new upgrade, and from a tool that shares the event collection. Such an event loses its
`mv` and is no longer returned by `afterVersions(..)`. A full read still returns it. On a replica set, if that event
matches an append condition and was committed after the condition's read, the append no longer fails, though it should.
Replacing an event document isn't supported, and no setting turns this off. A store built on
`MongoTemplate.withSession(..)` or `ReactiveMongoTemplate.withSession(..)` doesn't support reading versions.

Three things break, and two of them are compile errors.

- A record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions` with the old number of components stops
  compiling. `UpgradeToOccurrent_0_35` adds `var markerVersions` to the first two, and `var versionsRequested`,
  `var afterVersions`, `var confirmsAppendBeforeSaving` and `var firstOperationOfTransaction` to the third.
- `DcbEventStream(List, long)` is removed, so a store or a test double that builds a stream with it stops compiling.
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with)
  says what to call instead.
- An `isEqualTo(...)` on a whole `DcbAppendResult` or `DcbEventStream` that a store returned still compiles, and fails
  when the store reported versions. Compare the components you mean instead. Every other existing constructor stays,
  and sets the new components to empty.

## A consistency token holds the criteria it was read with

A `DcbConsistencyToken` holds the criteria of the read that returned it, and a `DcbAppendCondition` throws
`IllegalArgumentException` when it's created with a token read with other criteria. In 0.34.0 the token was a bare
number that a condition could pair with any criteria. On MongoDB that number was a sum of marker versions, so a
condition with other criteria than the read compared sums over other markers, and could let an append through after an
event matching its criteria had committed. The in-memory store refused the same code. The change is
[#1228](https://github.com/johanhaleby/occurrent/issues/1228), and
[ADR 147](../architecture/decisions/0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md)
describes it.

### What changes

- `DcbConsistencyToken` is a final class instead of a record. A store creates one with
  `DcbConsistencyToken.of(criteria, version)`, and `criteria()` and `version()` read it back. The public constructor,
  `DcbConsistencyToken.of(long)` and `value()` are removed.
- `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)` are removed, since they create a token without
  criteria.
- `DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken)` takes its criteria from the token.
- `failIfEventsMatch(DcbCriteria, DcbConsistencyToken)` is deprecated. It still works, and throws
  `IllegalArgumentException` when the criteria aren't the same as the token's.
- `wholeStoreLock(DcbConsistencyToken)` throws `IllegalArgumentException` for a token not read with `DcbCriteria.all()`.

Two criteria are the same when they hold the same set of alternatives. `anyOf(a, b)` is the same as `anyOf(b, a)`, and
a `DcbCriterion` is the same as an `anyOf(..)` with only that criterion in it. Criteria with different alternatives
aren't the same, even when they select the same events, so `types("A", "B")` isn't the same as
`anyOf(type("A"), type("B"))`.

The check runs in the condition's constructor, before any store is called, so it works the same on every store, a
`DcbEventStore` of your own included. `GenericDcbApplicationService` reads and appends with the same criteria on both
stacks, so it isn't affected on a store that returns the criteria it was given.

### Who is affected

- **A `DcbEventStore` of your own.** It stops compiling where it calls `new DcbConsistencyToken(..)`,
  `DcbConsistencyToken.of(long)`, `value()` or `new DcbEventStream(events, head)`.
- **Code that creates a token, or a stream with a two-argument constructor**, such as a test double that returns a
  `DcbEventStream` or a `DcbDomainEventStream`. It stops compiling the same way. A stream created with a token you
  already hold, `new DcbEventStream(events, head, token)`, still compiles. A call to `value()` stops compiling too, and
  so does a record pattern over `DcbConsistencyToken`, since the token is no longer a record.
- **A `DcbEventStore` decorator that rewrites criteria**, adding a tenant tag for example, and passes the inner store's
  token through. It compiles, but the token holds the rewritten criteria, so a condition on the caller's criteria throws
  `IllegalArgumentException`. In `read`, return
  `new DcbEventStream(inner.events(), inner.lastSequencePosition(), DcbConsistencyToken.of(criteria, innerToken.version(), innerToken.markers()), inner.markerVersions())`,
  since the three-argument constructor sets `markerVersions` to empty. Those versions hold the rewritten criteria's
  markers, the added tag's included, so pass them back to the inner store unchanged with the rewritten criteria. A store
  reports no versions for `all()`, on a read or on a conditional append, and throws `DcbSnapshotInvalidException` on a
  read of `all()` with `afterVersions`. So when `all()` becomes a tag, use the three-argument constructor on that read,
  drop the versions from that append's result, and throw on that read yourself. In `append`, rewrite the criteria the
  same way, and give the inner store a condition with
  `DcbConsistencyToken.of(rewrittenCriteria, token.version(), token.markers())`.
- **Code that reads with one criteria and appends with another**, passing the token the read returned. Creating the
  condition throws `IllegalArgumentException` on every store. `failIfEventsMatch(criteria, token)` compiles with a
  deprecation warning, and `wholeStoreLock(token)` and `new DcbAppendCondition(criteria, Optional.of(token))` compile
  with no warning, so the compiler doesn't find all of this code.
- **Code that saved a token value**, in an ETag or a queued command for example. A token value saved under 0.34.0 isn't
  valid on 0.35.0.

### What to do

In a store, pass the read's criteria when you create the token, and read `version()` where you read `value()`:

```java
return new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, version));
```

Compute the version the same way you computed the value for `DcbConsistencyToken.of(long)`. `DcbEventStream(List, long)`
used the head as the token, so `new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, head))` does what it
did. For a `DcbDomainEventStream`, pass the token of the `DcbEventStream` it was built from. A test double with no
`DcbEventStream` behind it creates the token with `DcbConsistencyToken.of(criteria, version)`.

Build a conditional append from the token alone:

```java
DcbEventStream stream = eventStore.read(criteria);
// decide on stream.events()
eventStore.append(newEvents, DcbAppendCondition.failIfEventsMatch(stream.consistencyToken()));
```

Where you passed the criteria the read was given and the token separately, drop the criteria. If the append really needs
other criteria than the read, read with those criteria before you decide, and decide from that read. A token read after
the decision can hide an event committed in between, so don't swap in a fresh token for the one the decision was made with.

Don't save a token value to use later. Read again when the command runs, and decide from that read. That includes a
client's ETag. Replacing a stale ETag with a fresh token makes the append check against a read the client's decision
wasn't made from. To refuse a client's stale write, keep a version of your own in your events, and compare the client's
ETag against it in the decision.

### Why there is no recipe

A recipe could rewrite `value()` to `version()`. That rename would compile, and it would keep a token value saved under
0.34.0 in use, although such a value isn't valid on 0.35.0. `DcbConsistencyToken.of(criteria, version)`
needs the criteria the read was given, and a recipe can't tell from the call site which criteria those were. Rewriting
`failIfEventsMatch(criteria, token)` to `failIfEventsMatch(token)` would compile too, but a condition that used other
criteria than its read would then append on the read's criteria instead of throwing.

### What else a token holds on MongoDB

On MongoDB, a token also holds what the read saw of each marker. On a replica set, an event a 0.35.0 node appended that
doesn't match a condition's criteria no longer fails it, so you'll see fewer `DcbAppendConditionNotFulfilledException`s
on a busy tag. A condition can still fail with no matching event, for example after a marker was deleted, when a
0.34.0 node incremented a marker, or when the read saw a marker only 0.34.0 had written.

Through a mongos, an event that doesn't match the criteria doesn't fail it either. A transaction through a mongos can
read each shard at another point in time, so the store doesn't query the events in the append's transaction there. It
starts a read-only transaction of its own on a second session, at read concern `snapshot` and write concern `majority`,
reads the markers of the criteria again in it, and queries the events in it. When that snapshot shows a marker under
another epoch or at an earlier version than the append's transaction read, or when the store can't run that
transaction, the condition fails with no matching event. It also fails there for the reasons it does on a replica set,
after a marker was deleted, when a 0.34.0 node incremented a marker, or when the read saw a marker only 0.34.0 had
written.

The check works the same in the store's own transaction, in one that `SpringTransactionExecutor` or
`SpringReactiveTransactionExecutor` opened and in a transaction of your own. The store doesn't change the read concern
or the write concern of any of them. The commit of its own read-only transaction waits for majority for at most
`markerVersionsCommitTimeout`, 5 seconds by default.

The read-only transaction takes a second connection from the client's pool while the append's transaction is open. With
`loadBalanced=true` the driver keeps every transaction on one connection until it ends, so when as many conditional appends
as `maxPoolSize` reach the query at once, each one waits for a connection that none of the others gives back. Each check
then fails after about `markerVersionsCommitTimeout`, and its append is refused. On a load-balanced deployment, set
`maxPoolSize` above the number of conditional appends you run at the same time.

A condition can also miss a matching event when the read's token covers that event and the read's events don't hold it.
On any deployment, a read whose events are behind its token can do this, for
example when a read preference or a read concern sends the events read to a lagging secondary or to an older snapshot. A
token also doesn't depend on a read's limit, skip or position range, so after such a read a condition doesn't fail for a
matching event the read left out. 0.34.0 has the gap this paragraph describes too, except that it refused the append
when the read caught only some of the event's markers, or when a later event moved a marker of the criteria, even one
that didn't match. On 0.34.0 a failover can cause it as well, when it loses a position reservation made below majority.
So can a rollback of your transaction on the Spring and reactor stores, when the store's own transaction manager uses
the same database factory as yours and its transaction is set to `REQUIRES_NEW` or `NOT_SUPPORTED`. The event then
commits above the last reserved position. 0.35.0 reserves positions outside every transaction and waits for majority on
the reservation, as [the next section](#an-append-on-mongodb-waits-for-majority-on-its-position-reservation) says.

Through a mongos, a read without `withVersions()` reads the markers and the events in separate operations, which can
reach different shards. Outside a transaction or session of yours, the store reads the markers, the position counter
and the events in a causally consistent session of its own. When the markers read sees a marker that a matching event
moved, the events read waits for that event's commit on the shard that holds the events, and returns the event. I tested
that only with no read concern or read preference set, so that the reads go to the primary. With one set on the client,
the database, the template or in a Spring store's query options, the read may still miss the event.

Such a read can take as long as that commit. A marker in its token that's on the shard holding the events can still be
from before the event.

The store doesn't make a read in a transaction or session of yours wait, as before. On the Spring stores it reads the markers, the
counter and the events in that session, and so does one in a transaction that `SpringTransactionExecutor` or
`SpringReactiveTransactionExecutor` opened. That includes a read while transaction synchronization is active, since the
template then reads in the session bound there, or binds one. On `MongoEventStore`, a read with a session bound in
`ClientSessionHolder`, as `NativeMongoTransactionExecutor` binds one, reads the markers and the counter outside that
session and only the events in it, as 0.34.0 did. No test checks whether such a read can miss the event. A read with `withVersions()`
doesn't miss it when its stream reports `markerVersions()`, because it read the markers and the events in one snapshot
transaction. On a replica set the read is the same as before.

If you delete marker documents from the `_dcb_checkpoints` collection, delete one only in a transaction that also
finds no event with its tag or type, and not during a rolling upgrade. On a sharded cluster, run that transaction at
read concern snapshot. Earlier versions said pruning during quiescence was harmless. It isn't, because a token can be
held across the prune. A type has eight marker documents, `marker:type:<type>` and `marker:typestripe:<n>:<type>` for
`<n>` from 1 to 7, and the rule holds for each of them.

A `DcbEventStore` of your own needs no change, and `DcbConsistencyToken.of(criteria, version)` keeps working for it.
A MongoDB store throws `IllegalArgumentException` for a token built that way.

## An append on MongoDB waits for majority on its position reservation

An append on a MongoDB event store reserves its positions with an update of a counter. That update now runs outside
every transaction, one of yours included, and waits for write concern majority, whatever write concern you configured.
A failover could lose an update made below majority while the append's event committed above the counter, and reads
then missed the event.

### What changes

A w:1 on the client, on the database or as the server's default no longer applies to this update, and neither does a
`wtimeout` you configured. The update waits for majority for at most `markerVersionsCommitTimeout`, 5 seconds by
default, the setting that also bounds the commit of a read that asks for versions. When a majority doesn't acknowledge
the update in that time, the append throws and writes no event, and the positions it reserved stay unused. On a client
with a `timeoutMS` above 0, where the driver sends no `wtimeout`, your `timeoutMS` bounds the update instead. A
`timeoutMS` of 0 waits forever, so there `markerVersionsCommitTimeout` bounds the whole update, the wait to select a
primary included. The rest of the append keeps the write concern it had.

Every append waits for as long as a majority takes to replicate the update. On MongoDB 5.0 or later, on a set without an
arbiter and with no write concern configured, the update already waited for majority. What changes there, unless your
client sets a `timeoutMS` above 0, is that a wait that used to have no limit now fails the append after
`markerVersionsCommitTimeout`. On a local three-member set at w:1, a single writer's latency stayed within the noise
between runs. With 8 writers, throughput went from about 990 to 1030 appends a second on the native store, 1050 to 875
on the Spring store and 990 to 670 on the reactor store. These appends ran outside a transaction, and all members ran on
one host, so the numbers show no network. Across a network, expect each append to wait about one more round trip to a
secondary.

### Inside a transaction of yours

On the Spring and reactor stores, the update used to join a transaction already open when the append started, and
committed with it. It now commits on its own, before the append writes its event.

- After a rollback of your transaction, the reserved positions stay unused. Positions already have gaps, so nothing that
  reads them has to change.
- Two appends in transactions of yours no longer conflict on the counter, only on a marker or a partition stream they
  share. An append that loses throws a `DataIntegrityViolationException` or a `TransientDataAccessException`, and your
  transaction has to be retried. `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry it when they
  opened it outside a Spring transaction scope of yours.
- In a Spring scope of yours with no transaction, a read through the store's template can bind a MongoDB session to
  that scope. A stream `write` or a DCB append in that session no longer retries a conflict, since the conflict aborted
  the session's transaction. It fails at once, as in your transaction.
- On a replica set, with transaction synchronization active, as it is by default, a read in your transaction reports, in
  `lastSequencePosition()`, the head as of the first operation in your transaction's MongoDB session on the store's
  database factory. It also returns your transaction's own appends, and those it started after that operation are at
  positions above that head. If you keep that position and read after it later, you get those events again. On a sharded
  cluster, unless your transaction reads at read concern snapshot, the shards aren't guaranteed to be read from one
  snapshot, so the head and the events can come from different points in time.
- An append joins your transaction when that's a Spring-managed MongoDB transaction on the template's database
  factory, and the `TransactionTemplate` or `TransactionalOperator` you give the store, if any, keeps the default
  propagation, `REQUIRED`.
  [ADR 149](../architecture/decisions/0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md)
  lists what happens in other setups.

### What to do

On a replica set of a primary, a secondary and an arbiter with the secondary down, a majority can't acknowledge the
update. In a probe on all three stores, appends there used to complete in 6 to 18 ms. The store now bounds the update
by `markerVersionsCommitTimeout`, so such an append throws after 5 seconds by default. To wait longer or
shorter, set `markerVersionsCommitTimeout` in the store's `EventStoreConfig`. On a client with a `timeoutMS` above 0
that setting doesn't bound the update, and the client's `timeoutMS` does.

## Criteria with tags conflict on their tags

On the MongoDB stores, the check of a conditional append with a token reads only the tag markers of an item with tags,
and no longer the markers of its types. Sharing a type with such an item is then no longer a reason for the check to
refuse. Such an item also no longer adds its type markers to what a conditional append increments.
Say you read `DcbCriteria.type("SeatReserved").tags(Tag.of("show", "1"))`, and another command appends a
`SeatReserved` tagged `show:2`. A conditional append with the read's token used to throw
`DcbAppendConditionNotFulfilledException`, and now succeeds. The in-memory store already answered that way for this
example. With a token, an item without tags still reads its type markers. The change is the first part of [#1229](https://github.com/johanhaleby/occurrent/issues/1229), and
[ADR 147](../architecture/decisions/0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md)
describes it.

### What stays the same

- A conditional append on criteria with tags is still refused when an event that matches them committed after the
  read. On a replica set and through a mongos the check reads the events as they are when the append runs, so an event deleted by then, or
  changed so that it no longer matches, may not fail it. A read can also miss such an event and let the append through,
  as [What else a token holds on MongoDB](#what-else-a-token-holds-on-mongodb) says.
- Every append still increments a marker for each type and each tag of its events, so by default two appends of one
  type still can't commit at the same time on MongoDB, whatever their tags.
  [Appends of one type can spread over eight marker documents](#appends-of-one-type-can-spread-over-eight-marker-documents)
  says how to change that.
- This change adds nothing on disk, so it doesn't stop 0.34.0 and 0.35.0 nodes appending to the same store, within the
  limits in [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). A conditional
  append from a 0.34.0 node still increments the type markers of its criteria, also for an item with tags. A conditional
  append from a 0.35.0 node doesn't add them for such an item. A 0.35.0 check reads a type marker only for an item
  without tags, so a 0.34.0 increment can make that check refuse an append and the command retry, and it can't let an
  append through. This change doesn't stop you going back to 0.34.0 either, within the same limits.

### Who is affected

- **Code that counted on the refusal.** A command whose conditional append failed because an event of the same type
  with other tags was appended can now succeed. If the decision depends on every event of that type, read with criteria
  that hold only the type. A conditional append on `type("X").tags(a)` that appends an event of another type also no
  longer makes an append with a token read on `type("X")` alone refuse, since it no longer increments that type marker.
  When many commands append one type with different tags, an append that used to be refused can now wait for seconds in
  the store's retries of a write conflict. When those retries run out, it fails with the store's write conflict exception,
  `DataIntegrityViolationException` on the Spring stores, and not with `DcbAppendConditionNotFulfilledException`, so the
  default retry of `GenericDcbApplicationService` doesn't retry it.
- **A `DcbEventStore` of your own** that checks conditions on markers, as the MongoDB stores do. The TCK reads
  `type("X").tags(a)`, appends an `X` tagged only `b`, and then appends with the read's token. That append has to
  succeed, so a store that still reads the type marker for an item with tags fails the case.
- **Code that switches over the TCK's `DcbAppendConditionModel`.** The enum gains `MARKERS_THEN_EVENTS`, which the
  fixtures of the MongoDB stores declare, so a `switch` that lists every case and has no `default` stops compiling until
  it handles the new one. A fixture that returns `EXACT_CRITERIA` or `TAG_MARKER` still compiles.
- **Code that saved a token value.** For an item with both types and tags, a 0.34.0 token value is a sum over its tag
  and type markers, and 0.35.0 no longer counts the type markers of that item. An old value can then let an append through
  that should have been refused. Read again instead, as
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with)
  says.
- **Code or an operator that deletes marker documents.** Follow the rule at the end of
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).

## Appends of one type can spread over eight marker documents

On MongoDB every DCB append increments a marker document for each type of its events, so two appends of one type
can't commit at the same time, even when their tags differ and no condition could ever match both. 0.35.0 splits the
marker of a type into eight documents. By default an append increments the first one for its events, the document
0.34.0 uses.

To spread the writes, set `spreadTypeMarkerWrites(true)` on the `EventStoreConfig` of the native, Spring blocking or
Spring reactor store, or this in either Spring Boot starter:

```properties
occurrent.event-store.mongodb.spread-type-marker-writes=true
```

An append then increments one of the eight documents for each type of its events, picked from its `AppendId`. Two
appends of one type with different tags share a document only when they pick the same one. The change is the second part of
[#1229](https://github.com/johanhaleby/occurrent/issues/1229), and
[ADR 162](../architecture/decisions/0162-a-dcb-type-marker-is-split-into-stripes.md) describes it.

### What changes whatever the setting

- A read of a criteria item without tags, such as `DcbCriteria.type("TicketSold")`, reads all eight documents of each
  of its types, in the same query.
- A conditional append on such an item compares all eight documents of each type and increments them, so it still
  conflicts with every append of the type.
- The `DcbMarkerVersions` of such an item hold eight entries per type. An entry for a document that doesn't exist yet
  has no epoch and version 0, and it equals `MarkerVersion.absent()`. Such a boundary reports versions before
  any event of its types was appended.
- A criteria item with tags reads no type marker, as
  [Criteria with tags conflict on their tags](#criteria-with-tags-conflict-on-their-tags) says, so none of this
  touches it.

### Turning the setting on or off

Every 0.35.0 node reads all eight documents, whatever its own setting. So nodes on different settings can share a
store, and a rolling restart that turns the setting on or off is safe.

Turn it on only when no 0.34.0 node appends to or reads from the store. A 0.34.0 node reads only the first document of
a type. Say a 0.34.0 node reads `type("TicketSold")`, and a 0.35.0 node with the setting on then appends a
`TicketSold` that picks the fourth document. The 0.34.0 node's conditional append sees no change in the first document
and commits, although a matching event was appended after its read. The same holds for any 0.34.0 condition with a
token and an item without tags. A 0.34.0 condition without a token checks the events in its own transaction, so it
misses only a matching append on another document that commits at the same time.

With the setting off, 0.34.0 and 0.35.0 nodes can share a store in both directions, as before.

To go back to 0.34.0 after turning the setting on, turn it off and restart every process that appends to the store,
including a separate service, a migration or a tool with its own `EventStoreConfig`. Then downgrade. The marker documents
beyond the first of each type stay in the store, and 0.34.0 doesn't use them.

### Who is affected

- **An application whose appends are mostly of one type.** It can turn the setting on, once no 0.34.0 node uses the
  store.
- **An application that reads or appends with conditions on criteria items without tags.** Each such read and
  conditional append uses eight marker documents per type instead of one, whatever the setting.
- **Code that inspects `DcbMarkerVersions`.** `MarkerVersion.epoch()` is `null` for an absent entry. A snapshot store
  of your own has to keep the entry without an epoch, as `SpringMongoSnapshotStore` and
  `ReactiveSpringMongoSnapshotStore` do.
- **A `DcbEventStore` of your own that reports marker versions.** The TCK expects the keys
  `DcbMarkerVersionsSupport.queryMarkerKeys` gives, which are eight per type for a criteria item without tags, with an
  absent entry for each document that doesn't exist.

## The default transaction executors retry on a MongoDB error label

The default `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry a transaction they opened outside
a Spring transaction scope of yours when it loses a conflict. In 0.34.0 that meant any `DataIntegrityViolationException`, because Spring translates MongoDB's write
conflict to it. That also matched a duplicate key and a validation failure, which no new transaction can get past, so
the unit of work ran 15 times on the blocking executor and 16 on the reactive one before the error reached the caller. The 0.35.0 budget is 240 attempts, so the same
predicate would have run it for two minutes.

0.35.0 retries a `TransientDataAccessException`, or any exception with the `TransientTransactionError` label anywhere
in its cause chain. MongoDB puts that label on a write conflict, and the driver's own `withTransaction` retries on it.
A duplicate key and a validation failure inside the transaction now run the unit of work once and reach the caller.
A commit that fails with the `UnknownTransactionCommitResult` label is not retried either, since the commit may have
gone through, even when the same error also has the `TransientTransactionError` label. The 0.34.0 predicate did not
retry it.

The two executor modules now depend on `mongodb-driver-core` as an optional dependency and look for the label only when
`com.mongodb.MongoException` is on the classpath. Without the driver you still get the executor, and it still retries a
`TransientDataAccessException`. You add nothing.

### Who is affected

Two cases changed. The first retried in 0.34.0 and doesn't now. A unit of work that writes outside the transaction, for example through a
`MongoTemplate` with `SessionSynchronization.NEVER`, can get a duplicate key that MongoDB does not label. Say it checks
whether a document exists, a rival inserts it, and the write then fails with the duplicate. The second run saw the
rival's document and skipped the write, so the command succeeded. In 0.35.0 it fails with the `DuplicateKeyException`
after the first run.

The second did not retry in 0.34.0 and does now. A commit that fails with the `TransientTransactionError` label reaches
the executor as a `TransactionSystemException` with a `MongoException` cause, which 0.34.0 did not match. The executor
now runs the unit of work again, up to three runs in all, which is safe when the transaction was rolled back, as that
label says it was. A failover and a transaction that outlives `transactionLifetimeLimitSeconds` fail the commit this way,
and a second run gets past both.
The case to watch is a unit of work that catches a duplicate key from a write inside the transaction and returns normally.
MongoDB has aborted the transaction by then, so the commit fails with the label and the executor runs the unit of work
three times, where 0.34.0 failed once. The third failure is a `TransactionSystemException` whose message says the commit
failed with `NoSuchTransaction` after 3 failed commits.

An executor you build with a retry of your own uses that retry as given, as before, so it has no cap on failed commits.
An executor built on a template or operator that opens no transaction has no commit to fail, so the cap never applies to it.

### What to do

Handle the duplicate in the unit of work only when its write is outside the transaction, and let a duplicate from a write inside the transaction reach the caller instead of catching it. Or make the write through a template that joins the transaction, so that a
rival's insert surfaces as a labelled conflict and is retried.

### In a Spring scope of yours with no transaction

In a Spring transaction scope of yours that has no transaction, such as a `TransactionTemplate` on
`PROPAGATION_SUPPORTS`, `SpringTransactionExecutor` no longer runs a unit of work again after it ran in your scope's
synchronization. The conflict reaches you, as it already did with `SpringReactiveTransactionExecutor`. A MongoDB session
that a read binds to your scope stays bound until your scope ends, so a second run would use the session the conflict
aborted. With a retry policy that isn't a `RetryStrategy.Retry`, the executor runs the unit of work once whenever your
scope is active. To get the retry back, retry around your scope, so the next attempt binds a new session.

## DCB snapshots resume from marker versions

`SnapshotDcbDeciderApplicationService`, `ReactiveSnapshotDcbDeciderApplicationService` and a `@Snapshot` view of a
`DcbSnapshotView` save a DCB snapshot at the marker versions of its boundary, and then read only the events appended
after them. In 0.34.0 they saved the position of the last event and lost an append that committed late below it
([#1230](https://github.com/johanhaleby/occurrent/issues/1230)). The reads they make are the ones
[DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions) describes.

### What changes

- A decider executes with `DcbExecuteOptions.fromVersions(saved, position)`, or `withVersions()` when it has no snapshot,
  and saves at the versions and the last position its append reported. It saves only when `DcbApplicationService.isAppendConfirmed(..)` returns
  `true`. `GenericDcbApplicationService` asks the event store, and returns `false` when the store doesn't implement
  `DcbAppendConfirmation`.
- A `@Snapshot` DCB view reads the events after the saved versions on each matching event, and saves at the versions of
  that read. `everyNEvents` counts the events of that read.
- When the store refuses to read after the saved versions, for example because a marker was pruned, the decider and the
  view read the whole boundary from the initial state and save a new snapshot.
- They do the same when that read returns an event below the highest position folded into the snapshot. It committed
  late, and folding it last could give another state than a read in position order.
- When the store reported no versions, or didn't confirm the append, the snapshot isn't saved. The first skip for each
  reason logs a warning, and later ones log at debug.
- A boundary reports no versions until each tag of an item with tags has a marker and every marker of the boundary
  that exists has an epoch. A decider reads the whole boundary, an attempt whose read reported no versions saves nothing, and its append creates the missing markers
  and gives the others an epoch, so its next command can save. A `@Snapshot` view never appends, so it reads the whole
  boundary until other appends have created its tag markers and given every marker that exists an epoch. 0.34.0 saved at a position there.
- A `@Snapshot` view over `DcbCriteria.all()` keeps no snapshot and logs a warning at start, since a read of every event
  has no marker versions. A snapshot 0.34.0 saved for it stays in the store and stops changing, so delete it or give the
  view a boundary. `dcbSnapshotView { }` in Kotlin uses `DcbCriteria.all()` unless you set tags or criteria. A decider over
  `DcbCriteria.all()` saves no snapshot either, so every command reads every event of the store, and the first skip logs
  a warning.
- A custom key function on a decider should stay keyed on the criteria. If you widen the criteria and keep the key, and
  the marker keys stay the same, for example when you change only the types of an item with tags, the next execute
  resumes after the saved versions and doesn't read the older events the wider criteria now matches, and nothing reports
  it. A widening that changes the marker keys is refused, and the decider reads the whole boundary.
- Inside a transaction of yours a read reports no versions, so a decider that runs in one reads the whole
  boundary and saves no snapshot. This is a known limitation, tracked in
  [#1307](https://github.com/johanhaleby/occurrent/issues/1307).
- A DCB application service with synchronous subscriptions reads and decides inside its transaction executor, as in
  0.34.0. When the executor opens the transaction, the service tells the store through `DcbReadOptions` that its read
  is the first operation of that transaction, and a MongoDB store reports the marker versions, so a decider there saves
  snapshots. On a replica set the store reads them in that transaction. On a sharded cluster it reads them in a
  read-only transaction of its own, on a second session, and the decider's append still runs in the executor's
  transaction.
- On a Spring executor that opens no transaction, outside a Spring transaction scope of yours, a Spring MongoDB store
  on a replica set reads them in the session transaction its read binds. On a sharded cluster it reads them in a
  transaction of its own and then binds a session transaction. The decider's append joins the bound transaction, so a
  failing handler undoes the append.
- In a Spring scope of yours with no transaction, on an executor that opens none, the service doesn't mark the read,
  so with no session bound for the store's database factory yet, a decider there reads in a transaction of the
  store's own, and its append runs in one too, which the store retries on a conflict and which doesn't
  join your scope, so a failing handler doesn't undo it.
- In a Spring scope of yours with no transaction and a session already bound there, a read that asks for marker
  versions, such as a snapshot decider's, reads in a transaction of the store's own until the store writes in that
  session, with an append for example. After that it reads in that session and gets no versions, and a read after
  marker versions is refused there. The store doesn't see writes to event or marker documents that go through your
  template in that session, so it relies on there being none.
- A Spring MongoDB store no longer retries a DCB append or a stream write in a session bound to the synchronization. A
  conflict fails it at once, as in your transaction, instead of a retry in the transaction the conflict aborted.
- `DcbReadOptions.confirmingAppendBeforeSaving()` and `asFirstOperationOfTransaction()` say something about the caller
  that the store can't check. Set them on a read of your own only when you keep the contract their javadoc states.
- In a transaction you own, on a blocking service, an exception from your domain function no longer marks that
  transaction rollback-only when the executor joins it and its `isTransactional()` answers `true` there.

### Stored snapshots

- `Snapshot` gains two components, `Optional<DcbMarkerVersions> markerVersions` and `long highestFoldedPosition`. The
  three-argument constructor stays and sets them to empty and 0, and `Snapshot.atMarkerVersions(..)` creates a DCB
  snapshot, whose `version` is 0.
- `SpringMongoSnapshotStore` and `ReactiveSpringMongoSnapshotStore` store a DCB snapshot with a `markerVersions` document,
  which holds the criteria the versions were read with and their entries, a `highestFoldedPosition` field and no
  `version` field. A stream snapshot is stored as before.
- A DCB snapshot that 0.34.0 saved at a position is never used. After the upgrade an execute or delivery reads the whole
  boundary until one can save over it, so nothing needs migrating. On a boundary whose markers 0.34.0 wrote, a decider's
  append gives them an epoch, and its next command can save.
- A 0.34.0 node that reads a snapshot 0.35.0 saved, during a rolling upgrade or after a rollback, finds no `version`. It
  reads the whole boundary and saves a snapshot at a position, which 0.35.0 then doesn't use.
- A snapshot store of your own has to persist `Snapshot.markerVersions()` and `Snapshot.highestFoldedPosition()`. If it
  drops the versions, every execute and delivery reads the whole boundary. If it loads a lower position than it was
  given, a resumed state can differ from a read in position order.

### What to do

- If you have a `DcbApplicationService` of your own that wraps another one, override `isAppendConfirmed(..)` and
  delegate it. Without that the deciders save no snapshot. Pass the domain function on as it is, and to that one service
  only, since a decider knows a refusal of its own read by that function. If you wrap it, a refused read after a conflict
  reaches the caller instead of reading in full. If you give it to a second service as well, a refused read there can
  run the command again.
- Replace `DcbExecuteOptions.fromPosition(..)`, which is deprecated for removal, with `fromVersions(..)`. Save the
  `DcbAppendResult.markerVersions()` and `lastSequencePosition()` of an execute with `withVersions()` and pass both on
  the next one.
- `DomainEventQueries.afterPosition(..)` is deprecated, since a position isn't a safe place to resume from on MongoDB.
  To process every event, resume a catch-up subscription from the checkpoint it stores.
- A record pattern over `Snapshot` with three bindings stops compiling. `UpgradeToOccurrent_0_35` adds
  `var markerVersions, var highestFoldedPosition`.
- A `SnapshotPolicy` of your own that reads `SnapshotDecision.newVersion()` gets 0 from a DCB decider, where 0.34.0 gave
  it the position of the append. Use `eventsSinceSnapshot()`.

## A failure after the write doesn't run the command again

With `noTransaction()` and no transaction of yours, a synchronous subscription runs after the write has committed. In 0.34.0, a handler that threw what
the application service's retry strategy retries on made the service read, decide and write again. By default that's
`WriteConditionNotFulfilledException` or `DcbAppendConditionNotFulfilledException`, so a handler that failed once stored the
events twice and `execute` returned normally. A retry strategy that matches `RuntimeException` did the same for any handler
failure and for a failed re-read of the written events. It applies to the stream and the DCB `GenericApplicationService`,
blocking and reactive.

### Who is affected

You use synchronous subscriptions and the attempt doesn't open a transaction. That's `noTransaction()`, the default, an
executor of your own that overrides neither `isTransactional()` nor `inTransactionTellingIfOpened`, or that answers a fixed
`true`, a Spring executor on `PROPAGATION_NOT_SUPPORTED`, `PROPAGATION_SUPPORTS` or `PROPAGATION_NEVER`, and an executor that
joins a transaction already open, whether yours or an outer service's. A Spring executor that opens its own transaction
under yours, on `PROPAGATION_REQUIRES_NEW` or on another transaction manager, isn't affected. A service without synchronous
subscriptions behaves as before.

### What changes

- `execute` fails, and the service's retry strategy doesn't run the command again. On the reactive stack the caller
  gets the original failure. On the blocking stack a strategy made by the factory methods of `RetryStrategy` still
  gives the failure to its `onError` listener and throws it as its `mapError` maps it, as for a failure its predicate
  doesn't match. A strategy of your own that ignores the shutdown predicate gets an empty attempt from any later run,
  before it reads or writes. When that strategy returns, the failure is thrown, and when it throws, the caller gets
  what it throws.
- The `sideEffect` doesn't run.
- The retry strategy can still run the command again for a failure of the write itself, and for any failure when the
  attempt opened a transaction. The
  service asks through `inTransactionTellingIfOpened`, a new default method on `TransactionExecutor` and
  `ReactiveTransactionExecutor`. The Spring executors answer from the transaction status, and the default infers it from
  `isTransactional()` answering `false` before the executor and `true` inside it. The service can't see whether the store
  joined, so a store that doesn't join keeps the first write and the retry writes again. The native MongoDB store under
  `SpringTransactionExecutor` does that.
- The service asks `isTransactional()` outside the executor too. An executor whose answer fails when no transaction is open
  makes an `execute` with synchronous subscriptions fail before the write.
- An executor that opens a transaction and overrides neither `isTransactional()`, which answers `false` by default, nor
  `inTransactionTellingIfOpened` still sees an exception pass through and can roll back, but the command no longer runs again. A rollback rule keyed on the
  handler's exception type sees a different exception now.
- `NativeMongoTransactionExecutor.isTransactional()` answers whether the session bound to the thread has a transaction in
  force, where it used to answer `true` always.

### What to do

- If you catch nothing around `execute` because a retry used to hide a handler's failure, handle it now. What's stored
  by then depends on your executor, its propagation and the store, so read it back before you run the command again.
  With `noTransaction()` and no transaction of yours the events are stored. With a Spring executor over the Spring
  MongoDB stores a write to a stream often rolled back, even on `PROPAGATION_NOT_SUPPORTED`, `PROPAGATION_SUPPORTS` or
  `PROPAGATION_NEVER`, and there a retry that used to work is gone. When `execute` runs outside every Spring transaction
  scope of yours, a DCB append on `PROPAGATION_NOT_SUPPORTED` rolls back as a write to a stream does, and a snapshot
  decider's does too. In a Spring scope of yours on `PROPAGATION_SUPPORTS` or `PROPAGATION_NOT_SUPPORTED`, which opens
  no transaction, the append of a plain `execute` rolls back too when you don't catch the failure inside that scope.
  [ADR 165](../architecture/decisions/0165-a-failure-after-the-write-is-retried-only-in-a-transaction-the-attempt-opened.md) has a table of the cases I ran.
- If your executor opens a transaction, override `isTransactional()` so the retry stays. Answer for the moment of the call,
  so `false` until the transaction is open. An executor that answers a fixed `true` counts as joining one. If it can open
  one with another already open, override `inTransactionTellingIfOpened` too, since the default counts that as joining.
- If a retry predicate was how a handler's work got done again, retry inside the handler, or use a transaction so the
  write and the handler commit together.
- If a handler on a service with no transaction writes to a stream through a service with a Spring executor on one of
  those three propagations, over a Spring MongoDB store, that handler can lose its reaction. The first command's events
  are stored, the handler's write rolls back, nothing runs the handler's command again, and a synchronous subscription
  has no replay. Use an executor that opens a transaction on the handler's service, which keeps the retry, or retry
  inside the handler.

## A handler failure in a joined transaction doesn't stop the other handlers

In 0.34.0 a synchronous dispatch stopped at the first handler that threw whenever the transaction executor reported a
transaction, including one it had only joined. With `NativeMongoTransactionExecutor` or `SpringReactiveTransactionExecutor`
inside a transaction of yours, a caller that caught the failure and committed kept the events, and the handlers behind the
failing one never got them. It applies to the stream and the DCB `GenericApplicationService`, blocking and reactive.

### Who is affected

You use synchronous subscriptions, and `execute` runs in a transaction the executor joins instead of opening its own:

- `NativeMongoTransactionExecutor` with a session already bound to the thread, yours or an outer service's.
- `SpringReactiveTransactionExecutor` in a transaction of yours, on a propagation that joins it such as `PROPAGATION_REQUIRED`.
- An executor of your own that answers `true` from `isTransactional()` before it opens its transaction, for example one
  that answers a fixed `true`. The default `inTransactionTellingIfOpened(..)` then reports every transaction as joined,
  also one the executor opened itself.

`SpringTransactionExecutor` isn't affected unless its transaction manager has `setGlobalRollbackOnParticipationFailure(false)`
or isn't an `AbstractPlatformTransactionManager`.

### What changes

- In a transaction the attempt joined, every handler gets the event and the failures are reported together, unless the
  executor's `rollsBackAJoinedTransactionOnFailure()` answers `true`. It's a new default method on `TransactionExecutor`
  and `ReactiveTransactionExecutor`, and it answers `false`.
- `SpringTransactionExecutor` answers `true` when its transaction manager is an `AbstractPlatformTransactionManager`
  that isn't set with `setGlobalRollbackOnParticipationFailure(false)`, and `false` for any other manager.
- In a transaction the attempt opened, the dispatch still stops at the first failure.

### What to do

- If your executor opens its own transaction but answers `true` from `isTransactional()` before it does, override
  `inTransactionTellingIfOpened(..)` to tell the action whether this call opened the transaction. The dispatch then stops
  at the first failure in a transaction it opened, and a failure after the write reaches the retry strategy.
- If your executor joins a transaction that a failure marks rollback-only, so the caller can't commit it, override
  `rollsBackAJoinedTransactionOnFailure()` to return `true`. The dispatch then stops at the first failure as it did in
  0.34.0, and the handlers behind it don't do work that the rollback discards.
- Otherwise keep the default. If the transaction rolls back after all, the handlers behind the failing one did work that's
  discarded, and a side effect outside the transaction, an HTTP call say, still happened.

## A read by position makes no promise about resuming

`readInPositionOrder(..)` on `PositionOrderedReader` and `DomainEventQueries` is a range query. It returns the events in
the range it can see when it runs. On MongoDB an append takes its position before its transaction commits, so
an event can reserve a position below the one you kept and commit after you read it, and a read after your position
never returns it. See [#1297](https://github.com/johanhaleby/occurrent/issues/1297).

### Who is affected

A loop that keeps the last position it read, or the value of `currentPosition()`, and reads after it the next time
round. It compiles and runs on 0.35.0 as it did on 0.34, and it can skip an event the same way:

```java
try (Stream<DomainEvent> events = domainEventQueries.afterPosition(lastPosition)) {
    events.forEach(this::handle);
}
```

`DomainEventQueries.afterPosition(..)` gives you a deprecation warning and nothing more. The same loop written with
`readInPositionOrder(filter, PositionRange.afterPosition(lastPosition))` gives you no warning at all. The store can't
tell where a position came from, so it can't refuse the read the way a catch-up refuses a start after a position.

A read that doesn't continue from an earlier one, such as a report over a range, isn't affected.

### What to do

Replace the loop with a catch-up subscription and resume from the checkpoint it stores, as
[A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from) describes.
A read model the loop has already built can be missing such an event, and only a rebuild from the beginning finds it.
Keep `readInPositionOrder(filter, PositionRange.afterPosition(p))` for range reads.

## A catch-up needs the event store itself

A catch-up subscription reads the point where its live subscription takes over, and every event it replays, through
the event store, which on MongoDB runs those reads in one session. An event that commits late, below a position the
replay already read, is then replayed or delivered live, and not skipped. [ADR 155](../architecture/decisions/0155-a-catch-up-reads-its-live-start-from-the-event-store-and-refuses-to-resume-where-it-could-skip-an-event.md)
has the reasons. Only the event store itself can do that, so the catch-up checks what it was given when you construct
it.

### Who is affected

The four catch-up subscription models and what they check:

| Model | Checks |
| --- | --- |
| `StreamCatchupSubscriptionModel` | the `EventStoreQueries` |
| `DcbCatchupSubscriptionModel` | the `DcbEventStore` |
| `ReactorStreamCatchupSubscriptionModel` | the `PositionOrderedReader` |
| `ReactorDcbCatchupSubscriptionModel` | the `DcbEventStore` |

`MongoEventStore`, `SpringMongoEventStore` and `InMemoryEventStore` pass the two blocking checks, and
`ReactorMongoEventStore` passes the two reactor ones. A class that wraps one of them, a mock, and your own implementation of the interface do not, and the constructor throws an
`IllegalArgumentException` naming `CatchupReads` or `DcbCatchupReads`.

Both Spring Boot starters run a subscription through a catch-up by default, and build the catch-up over the event store
bean in the context. A context whose bean is such a class fails at startup with the same exception.

### What to do

Hand the catch-up the event store itself. Put a decorator around the subscription model, or around the code the
subscription calls, and no longer around the event store the catch-up reads. In a test, run the catch-up over
`InMemoryEventStore` or a MongoDB event store instead of a mock.

`CatchupReads` and `DcbCatchupReads` are in packages named `internal`. They are public only because the stores and the
catch-ups live in different modules, and they can change in any release, so implementing them yourself is not supported.

## A catch-up keeps its live start in the checkpoints it stores

A catch-up subscription reads its live start, the point where its live subscription takes over, before it replays,
and goes live from it once the replay is done. An event that commits while the replay runs, below what the replay
already read, is then delivered live. A catch-up that runs without the live delegate now does that too, and when it
restarts in the middle of its replay, it goes live from the live start its earlier run stored.

### What changes

The checkpoint a replay stores holds the live start in a new form when the catch-up runs without the live delegate, or
when the replay started after position 0 or after the beginning of time:

| Replay | Stored as |
| --- | --- |
| By position | `position:<position>;v:2;origin:<position>;replayTo:<position>;liveFrom:<live start>` |
| By time | `position:time;v:2;at:<time>;origin:<time>;liveFrom:<live start>` |

A blocking stream catch-up with checkpoint storage stores the time form too when a `resumeSubscription(id, startAt)` at
a time after the beginning, without a live start, stores that time. The present position is then the live start. In
0.34.0 such a resume stored nothing.
[A resume at a position through a `DurableSubscriptionModel` stores the checkpoint](#a-resume-at-a-position-through-a-durablesubscriptionmodel-stores-the-checkpoint)
and [When a catch-up loses its live start on every replay](#when-a-catch-up-loses-its-live-start-on-every-replay) say
when a resume stores a position.

The other checkpoints a catch-up stores keep the form they had in 0.34.

A catch-up runs without the live delegate when the model it wraps answers `null` to its start. Under the Spring Boot
starter that is the case for `resumeBehavior = SAME_AS_START_AT`, where `DurableSubscriptionModel` and
`CompetingConsumerSubscriptionModel` answer `null`. Such a catch-up now hands the innermost subscription model, the
MongoDB one, the live start, and stores nothing when it goes live. It fails with an `IllegalStateException` when neither
the event store nor the subscription model reports a live start.

### Rolling upgrades and rolling back

0.34 refuses a checkpoint in the new form. It throws when it reads one and writes nothing, so nothing is lost, but the
subscription doesn't start on that node. During a rolling upgrade, a 0.34 node that resumes a subscription a 0.35 node
stored in the new form fails to start it until it runs 0.35 too.

To roll back to 0.34, stop every instance of the application, delete the stored checkpoint of every subscription whose
checkpoint starts with `position:` and has `;v:2;` after its first value, and start every instance again on 0.34 with
the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.
Each of those subscriptions then starts from the start it is configured with. A 0.35 instance that still runs after the
delete can take such a subscription over and store a checkpoint in the new form again.

### When a catch-up loses its live start on every replay

After its replay, a catch-up checks that its live start is still in the change stream history. When it isn't, and the
replay started at position 0 or at the beginning of time, the catch-up replays again with a new live start, up to three
times. A replay that started later is refused the first time, as
[A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it)
describes. When it loses the live start after each of those
replays too, it fails with `CatchupCannotResumeException`, an `IllegalStateException`, and the message names the longest
replay, from reading its live start to checking it. Size the change stream history to reach back at least that far. On
MongoDB that is the oplog, which you can keep for a minimum time with the `oplogMinRetentionHours` setting.

A blocking catch-up and a named reactor catch-up then hold the subscription paused and log the failure once at error
level, and the `Flux` of an unnamed reactor catch-up fails with the exception. In 0.34.0 a blocking subscription ended
when its subscribe started the replay, and the replay ran again after a backoff when a resume started it. A named
reactor subscription ended and freed its id unless a takeover started the replay. `waitUntilStarted()` fails with the
exception, and the id stays taken, so a subscribe of the same id throws `DuplicateSubscriptionIdException` until you
cancel the subscription. With more than one instance, stop every instance and delete or replace the stored checkpoint
instead of cancelling, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
describes. Resuming the subscription replays again from the beginning, and so does `start(true)`. Over a
`CompetingConsumerSubscriptionModel` around a blocking catch-up the node gives up the lease and stops competing for it
until you resume the subscription.

A replay that started later and loses its live start is held paused the same way. A replay again from its start would
skip events, so `resumeSubscription(id)` throws `CatchupCannotResumeException` until you cancel the subscription, and so
does `start(true)` on the blocking stack. With more than one instance, stop every instance and delete or replace the
stored checkpoint instead of cancelling, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
describes. A reactor `start(true)` keeps it held paused and replays nothing. On a
blocking catch-up subscription model you call directly, `resumeSubscription(id, startAt)` at a position the change
stream history still reaches resumes it. With checkpoint storage that position replaces the one the catch-up stored
when a restart starts from it, so a resume after a later pause doesn't replay from where the lost replay got to. While
a replay that `subscribe(..)` started is still running for a subscription paused during that replay, the resume stores
nothing and reads no present position, since that replay reads the stored position to decide where it goes live from.

A resume token or another checkpoint the MongoDB model restarts at is stored as given, and so is a checkpoint the
catch-up replays from with a live start or at the beginning. A time after the beginning that a stream catch-up replays
from without a live start is stored with the present position as its live start, in the new form, which 0.34 refuses.
A DCB catch-up replays only from a position and the MongoDB model doesn't restart at a time, so on a DCB catch-up a
resume at a time stores nothing. A `GlobalCheckpoint` above 0 without a live start, and a checkpoint the MongoDB model
doesn't restart at, store nothing, so the position the catch-up stored stays. A resume that needs the present position
to store, such as one at `StartAt.now()`, throws `IllegalStateException` and stores nothing when neither the event
store nor the MongoDB model answers one.

A `CompetingConsumerSubscriptionModel`, which the blocking Spring Boot starter puts around its catch-up, can't resume a
subscription at a position, and its `cancelSubscription(id)` acts only on the instance you call it on. With checkpoint
storage every instance that gets the lease refuses the same stored checkpoint. Stop every instance of the application,
delete that checkpoint or replace it with a checkpoint you kept that the subscription model still has the history back
to, and start every instance again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes
and the `ERROR` the competing consumer logs says. Without checkpoint storage, stop every instance and start them all
again subscribed from the beginning, or from a checkpoint you kept that the subscription model still has the history
back to.

A DCB catch-up and the reactor catch-ups replay only by position, and throw the same exception when you start them from
a time replay's checkpoint in the new form.

## A replayed event holds the checkpoint to resume from

`StreamCatchupSubscriptionModel` and `DcbCatchupSubscriptionModel` now deliver each event they replay as a
`CheckpointAwareCloudEvent`, holding the checkpoint the catch-up stores after it. The live subscription and the reactor
catch-ups already delivered their events that way. An application that keeps its own state can store the checkpoint
with that state, in the same transaction, and resume from it. The state and the point the subscription resumes from
then never disagree. See [#1297](https://github.com/johanhaleby/occurrent/issues/1297).

### Who is affected

An action or a converter that compares a replayed event with the `CloudEvent` that was written, or checks its class. A
`CheckpointAwareCloudEvent` equals only another one with the same checkpoint, so such a comparison now fails. Compare
`getOriginalCloudEvent()` instead. `EventMetadata` equality doesn't include the checkpoint, so the metadata of an event from
the replay and of the same event from live compare equal.

### Resuming from a checkpoint you store

Read the checkpoint where your handler gets the event:

| Handler | Read the checkpoint with |
| --- | --- |
| The DCB DSL, blocking and reactor | `DcbEventMetadata.checkpoint()` |
| The Kotlin stream DSL, a `@Subscription` method with an `EventMetadata` parameter, a `@Projection` view | `EventMetadata.getCheckpoint()` |
| An action that takes the `CloudEvent` | `EventMetadata.from(cloudEvent).getCheckpoint()` |

Each returns an empty `Optional` for an event delivered with no checkpoint. Store the string with your state, in the
same transaction, and start the subscription from it again with
`StartAt.checkpoint(new StringBasedCheckpoint(checkpoint))`, or `DcbStartAt.checkpoint(checkpoint)` for a DCB
subscription.

From the checkpoint of a live event, or of an event that a catch-up replays by position, the subscription resumes
after that event. A catch-up that replays by time resumes at the event's time instead, so it delivers the events with
that time again, that event included. An event without a time holds the checkpoint of the event before it, and resumes
where that one does. When its `catchupPhaseSortBy` doesn't start with the time ascending, an event from its first read
resumes the whole replay from where it started.

To rewind a read model to a point you know is good, keep the checkpoint of the event at that point, and start the
subscription from it.

The checkpoint of a replayed event means something only to a catch-up subscription. A MongoDB subscription model with
no catch-up wrapping it refuses that checkpoint, in a `StartAt` that names it or as a position a `StartAt.dynamic(..)`
returns before its change stream has opened, as
[A MongoDB subscription model refuses a catch-up's checkpoint](#a-mongodb-subscription-model-refuses-a-catch-ups-checkpoint)
describes.

The checkpoint of a live event holds a change stream resume token, so a restart from it needs the change stream
history to still reach back to it. On MongoDB that is the oplog. Once it no longer does, a catch-up subscription
doesn't restart the subscription from the present, as
[A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now)
describes. A MongoDB subscription model with no catch-up subscription wrapping it stops the subscription with an error
in the log, or restarts it from the present when it is set to restart after lost history, which skips the events in
between.

### Under a competing consumer

`CompetingConsumerSubscriptionModel` hands your start to the model it wraps when the node first gets the lease, not
when you subscribe. A `StartAt.checkpoint(..)` you build when you subscribe is as old as the node's start, so a node
that takes over an hour later delivers that hour's events again. Read the checkpoint when the node takes over instead:

```java
competingConsumerSubscriptionModel.subscribe("my-read-model", filter, StartAt.dynamic(context -> {
    String checkpoint = myStore.checkpoint();
    return checkpoint == null ? StartAt.subscriptionModelDefault() : StartAt.checkpoint(new StringBasedCheckpoint(checkpoint));
}), action);
```

Return a start of your own, such as `StartAt.subscriptionModelDefault()`, while your store holds no checkpoint yet,
since `StringBasedCheckpoint` refuses `null`.

When a node gets the lease back after losing it, the model it wraps resumes from where that node last ran. Events can
be delivered again. None are skipped, unless the change stream history no longer reaches back to that position and the
model is set to restart after lost history, which restarts the subscription from the present.

### Spring Boot

Declare a `SubscriptionResumePoints` bean, and Spring Boot asks it where each `@Subscription`, `@StreamSubscription`,
`@DcbSubscription`, `@Projection`, `@Saga` and `@Snapshot` subscription resumes, whatever start position the
annotation gives, `NOW` and `DEFAULT` included:

```java
@Bean
SubscriptionResumePoints subscriptionResumePoints(MyReadModelRepository repository) {
    return subscriptionId -> repository.findCheckpoint(subscriptionId).orElse(null);
}
```

A non-null answer is the checkpoint the subscription resumes from, ahead of the one Spring Boot's `CheckpointStorage`
holds. When that stored checkpoint is newer than the answer, the events between the two are delivered again. `null`
starts the subscription from the annotation's start, as it starts without the bean. Spring Boot reads the bean each time
a subscription model reads the start, also when a node of a competing consumer takes over, and can ask more than once
for one start, so return the same answer each time.

With `resumeBehavior = SAME_AS_START_AT` on a start that replays, `BEGINNING` or an explicit position, or on `NOW`, the
answer only moves where the subscription starts. The subscription keeps no checkpoint in `CheckpointStorage` once it is live, and the
competing consumer model does not compete for it. A catch-up that stores checkpoints during its replay still does, and
deletes the last one when the replay goes live.

Spring Boot decides from the annotation, not from the answer, whether application startup waits for the subscription.
When `occurrent.subscription.mode` is `manual`, nothing waits. Otherwise, under the default `startupMode`, a subscription whose annotation starts at `NOW` or `DEFAULT` waits, also when the
answer makes it replay. One whose annotation replays, from the beginning, a position or a time, starts in the
background, also when the answer makes it go live. Set `startupMode` on the annotation to choose.

### An exhaustive `switch` over `DcbStartAt`

`DcbStartAt.checkpoint(..)` returns the new `DcbStartAt.AtCheckpoint`. `DcbStartAt` is sealed, so a `switch` over it
that lists every case and has no `default` no longer compiles. Add a case for `DcbStartAt.AtCheckpoint`.

### Why there is no recipe

What the new case should do depends on what the `switch` does with the others, so there is nothing for
`UpgradeToOccurrent_0_35` to write into it.

## A catch-up over both a stream and a DCB model refuses a position start without a filter

The blocking `CatchupSubscriptionModel` and `ReactorCatchupSubscriptionModel` hold a stream and a DCB catch-up when you
use both, as the Spring Boot starters do. They route a subscription with no filter by its start. A time or beginning
start goes to the stream catch-up, and a position checkpoint went to the DCB one, so a stream subscription restored from
its own checkpoint replayed only DCB events and skipped its stream events. Both models now throw an
`IllegalArgumentException` before they subscribe anything.

### Who is affected

Code that subscribes with no filter, or with a `null` one, at a position checkpoint over a model that holds both
catch-ups. That includes `subscribe(subscriptionId, startAt, action)`, which passes no filter, and the broker
`CloudEventForwarder.forward(subscriptionId, startAt)` and `DomainEventForwarder.forward(subscriptionId, startAt)`. The
DSLs and the Spring Boot annotations pass a filter, and a time start, a beginning start and a model with only one
catch-up are not affected.

Under a `CompetingConsumerSubscriptionModel`, `subscribe` throws when the node gets the lease straight away or the
competing consumer model is stopped. A node that gets the lease later logs a warning, gives the lease back, and fails the
same way every time it gets the lease.

### What to do

Pass a `StreamSubscriptionFilter` for stream events, a `DcbSubscriptionFilter` for DCB events, or an
`AgnosticSubscriptionFilter` for both. For a broker forwarder, use the `forward` overload that takes a filter.

## A catch-up refuses a start that could skip an event committed below it

On MongoDB an append takes its position before its transaction commits, so an event can commit below a position a
reader has already read past. A catch-up that went live from a live start read before that position still delivers
such an event, from the change stream. A catch-up started after a position with no such live start never delivers it.
A catch-up now refuses every start that could skip an event that way, before it delivers or stores anything, and fails
with `CatchupCannotResumeException`. A replay that loses its live start fails with the same exception, but only after
its handler has received the events the replay delivered, in this run or before a restart, and after any checkpoint
storage it uses has kept the progress. So the exception doesn't mean that nothing reached the handler. See
[#1230](https://github.com/johanhaleby/occurrent/issues/1230).

### Who is affected

| Start | 0.34 | 0.35 |
| --- | --- | --- |
| `startAtGlobalPosition` on `@Subscription`, `@Projection`, `@Saga` or `@Snapshot`, or `startAtDcbPosition` on `@DcbSubscription`, above 0 | Replays after the position | Refused |
| `DcbStartAt.afterPosition(n)` or `StartAt.checkpoint(GlobalCheckpoint.of(n))`, with `n` above 0 | Replays after `n` | Refused |
| A stored `position:<n>` checkpoint that holds no live start, with `n` above 0 | Resumes after `n` | Refused |
| A time checkpoint, `TimeBasedCheckpoint`, that a blocking catch-up resumes from and that holds no live start | Resumes from the time and logs a warning | Refused |
| A replay whose live start the change stream history no longer reaches, when it started above position 0 or after the beginning of time | Replays again from where it started, with a live start read then, up to three times | Refused the first time |

The table's last row also covers a catch-up from a specific time, `StartAt.checkpoint(TimeBasedCheckpoint)` or a time
start in Spring Boot. When its replay outlasts the change stream history, it now fails at once instead of replaying
again.

The same applies to a checkpoint 0.34 stored in the middle of a replay that started above position 0. When the
application is down for the upgrade for so long that the change stream history no longer reaches back to the live start
in that checkpoint, 0.35 refuses it. A blocking catch-up refuses it in `subscribe(..)`, before it replays anything, so
in Spring Boot with `startupMode` unset the application doesn't start, as when a stored resume token is gone. With
`startupMode = BACKGROUND`, or on a competing-consumer node that gets the lease later, the catch-up holds the
subscription paused at that live start instead.

A start at position 0, `StartPosition.BEGINNING`, `DcbStartAt.beginning()` or the beginning of time is not refused. A
replay from there whose live start is lost still replays again, as
[When a catch-up loses its live start on every replay](#when-a-catch-up-loses-its-live-start-on-every-replay)
describes.

On an event store that writes no positions, a blocking stream catch-up throws `UnsupportedStartAtException` instead for
`StartAt.checkpoint(GlobalCheckpoint.of(n))`, `startAtGlobalPosition` or a stored `position:<n>`, with `n` above 0, and
for a stored one also when it holds a live start. Over such a store none of them replayed from 0.30.0 to 0.34.0. They
went live at the present, as
[A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0)
describes.

The blocking catch-up throws `CatchupCannotResumeException` from `subscribe(..)` or from `waitUntilStarted()`. A replay
that loses its live start is held paused, whether a subscribe, a resume or a takeover started it, as
[When a catch-up loses its live start on every replay](#when-a-catch-up-loses-its-live-start-on-every-replay)
describes. The reactor catch-up fails its `Flux` with it, except that a named reactor catch-up whose replay loses its
live start holds the subscription paused the same way. The message names the subscription and what to do.

### What to do

To resume after an event, keep the checkpoint the subscription delivers with it, and start from that, as
[A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from)
describes. In Spring Boot, return it from a `SubscriptionResumePoints` bean. That checkpoint holds the live start, so a
start from it is not refused while the change stream history still reaches back to that live start. Once it no longer
does, a start from it is refused when the replay that delivered the event started at a specific time, or is a 0.34
replay that started above position 0. A checkpoint from a replay that started at position 0 or at the beginning
replays again from there instead.

A subscription that is refused, and has no such checkpoint stored with its state, can only start from the beginning
again: `StartPosition.BEGINNING`, `DcbStartAt.beginning()` or `StartAt.checkpoint(GlobalCheckpoint.of(0))`.

With one instance, cancel it first with `cancelSubscription(id)`, which deletes its stored checkpoint. With more than
one, stop every instance, delete the stored checkpoint, and start them all again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.

A blocking catch-up holds a refused resume, and holds the stored checkpoint it read with it, so deleting the checkpoint
and resuming is refused again. Deleting the stored checkpoint with every instance of the application stopped, and
starting them all again with the same start, is enough, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes. After that the subscription delivers a checkpoint with every event. For a refused replay whose live start was
lost, size the change stream history to reach back across the replay. On MongoDB that is the oplog.

`startAtGlobalPosition`, `startAtDcbPosition` and `DcbStartAt.afterPosition(long)` are deprecated for removal, since
every value above 0 is refused. Use `StartPosition.BEGINNING` or `DcbStartAt.beginning()` instead of 0.

## A stream catch-up over an event store without positions replays from position 0

A blocking `StreamCatchupSubscriptionModel` over an event store that writes no positions handed
`StartAt.checkpoint(GlobalCheckpoint.of(n))` to the subscription model it wraps, without a replay. The MongoDB
subscription model opened the change stream at the present for it, and skipped every event before it with no error.
Every release from 0.30.0 to 0.34.0 did that. Position 0 now replays from the beginning of time and then goes live. A
position above 0, whether passed, from `startAtGlobalPosition`, or read from the stored checkpoint, throws
`UnsupportedStartAtException` from `subscribe(..)`, since the store has no position to replay after.

### Who is affected

A Spring Boot application on the blocking MongoDB starter with `startAt = StartPosition.BEGINNING` or
`startAtGlobalPosition = 0` on `@Subscription`, `@Projection`, `@Saga` or `@Snapshot`, which Spring Boot starts at
position 0, over an event store that writes no stream positions. A store writes none when
`occurrent.event-store.stream.position` is `false`. It also writes none when stream position is on only by default and
the store started against a collection that already held events without a `position`, since it then turns stream
position off for itself and logs how to turn it on. On any release from 0.30.0 to 0.34.0 such a subscription got none
of the history, only the events written after it started. On 0.30.0 that is `@Subscription` alone, since `@Projection`,
`@Saga` and `@Snapshot` arrived in 0.31.0. An application that upgrades straight from 0.30, 0.31, 0.32 or 0.33 is
affected in the same way.

The same goes for code that starts a blocking stream catch-up at `StartAt.checkpoint(GlobalCheckpoint.of(n))` over such
a store. A DCB event store always writes positions.

### What to do

Under `resumeBehavior = SAME_AS_START_AT` there is nothing to do. The subscription keeps no checkpoint, and replays the
whole history the next time it starts.

Under the default `resumeBehavior`, a subscription that went live this way before 0.35 stored a checkpoint as it handled
events, and resumes from that checkpoint in 0.35, so it still never gets the history. To get it, stop every instance of
the application, delete the checkpoint stored for the subscription, and start them all again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.
The subscription then replays the whole history, including the events it already handled after it went live, so rebuild the read
model it feeds first, or make sure its handler can take an event it already handled.

For a start above position 0 in your own code, start from `StartAtTime.beginningOfTime()` or from a time instead.

## A MongoDB subscription model refuses a catch-up's checkpoint

`NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `ReactorMongoSubscriptionModel` opened the change
stream at the present when you subscribed at a `GlobalCheckpoint` or a `CatchupTimeCheckpoint`, the checkpoints a
catch-up stores, and skipped every event before it with no error. Their `subscribe(..)` now throws an
`UnsupportedStartAtException`, and the reactor model's unnamed `subscribe(..)` fails its `Flux` with it. See
[#1336](https://github.com/johanhaleby/occurrent/issues/1336).

A `StartAt.dynamic(..)` whose function returns one of these checkpoints opened the change stream at the present too,
in 0.34.0 and before. `DcbStartAt.dynamic(..)` returning `DcbStartAt.beginning()` is one. The models now refuse that
as well. They call the function only as the change stream opens, so `subscribe(..)` returns, and the subscription then
stops with the `UnsupportedStartAtException` instead of delivering events. `waitUntilStarted(..)` throws it on the
blocking models, which also log it at `ERROR`, and `waitUntilStarted()` fails with it on the reactor model, except on
the handle `subscribe(..)` returns while the model is stopped, which never completes. A reactor handle that has answered
that it started keeps that answer. It answers once the change stream is subscribed to, so a refusal on a retry after a
failed opening stops the subscription and is only logged at `ERROR`. On a subscription that reads through the
driver's cursor, a resume straight after the handle answered can be refused too, when the function returns one of these
checkpoints, since the cursor may not have opened. The handle that resume returns fails with it. See
[#1371](https://github.com/johanhaleby/occurrent/issues/1371).

The function is checked until a change stream of the subscription has opened. On the blocking models that means a run
has reported its cursor open, so an answer from an opening that a pause or a failure stopped doesn't end the check. The
reactor model counts a driver cursor that opened for a subscription with an id. A reactor `subscribe(..)` without an id,
and a subscription with an id when the model can't read through the driver's cursor, count the first document the
change stream reads instead.

Once a change stream has opened, the model opens it again at the same position and doesn't call the function again.
That covers a pause and a resume, an action that throws, and a failure reading the change stream before the first
event. A reactor subscription that counts the first document takes the position the function answered when its `Flux`
was subscribed to. It gives that position back to the function only when the change stream fails before it reads a
document with error code 9, 280 or 286, the codes the server rejects a start position with. That holds also when a
pause or another failure opened the change stream again at that position first. After a 9 or a 280 the model opens the
change stream again, and that opening calls the function again. After a 286 a subscription with an id first asks the
listeners added with `addHistoryLossListener`, with the checkpoint the change stream opened at. One of them can take the
subscription over. When one throws, the model logs it and opens the change stream again after its backoff, which calls
the function again and asks the listeners again if the history is still lost. Otherwise, with
`restartSubscriptionsOnChangeStreamHistoryLost(true)` the model restarts the subscription at the present without
calling the function, and with the default the subscription fails. A `Flux` from `subscribeFailingOnHistoryLoss`
fails with a `HistoryLostException` that names that checkpoint, whatever the configuration. In 0.34.0 the models could call the function again at an opening
before the first event, and a catch-up's checkpoint from
that later call opened at the present and skipped the events written in between.

Three cases still lose events, as they did in 0.34.0:

- A resume token of another collection as the function's first answer, on the blocking models and on a reactor
  subscription that reads through the driver's cursor. The server opens the change stream at it and rejects it only on
  the first read after an event is written to the watched collection. The subscription keeps running and can retry at
  that token without delivering anything.
- An operation time ahead of the server's clock as the function's answer. The server opens the change stream at it, and
  the events written before that time aren't delivered.
- A position the server rejects only after an event is written, such as a resume token of another collection, on a
  reactor subscription that counts the first document. The model calls the function again, and an answer that comes
  after that event skips it.

### Who is affected

Code that subscribes a MongoDB subscription model directly at a checkpoint a catch-up stored, or delivered with a
replayed event, in a `StartAt` that names it or from a dynamic start that returns it. A
resume at such a checkpoint, `resumeSubscription(subscriptionId, startAt)`, isn't refused. The models go on from the
position the subscription has read to, for such a checkpoint and each time a dynamic position returns one. In 0.34.0
the reactor model opened at the moment of the resume instead, and skipped what was written while the subscription was
paused.

The function `subscribe(..)` was given is another matter. When no change stream has opened yet, a resume can still ask
that function, and it's refused when the function returns one of these checkpoints. That includes
`resumeSubscription(subscriptionId, startAt)` at such a checkpoint, or at a dynamic position that returns one.

A blocking `DurableSubscriptionModel` hands the MongoDB model a dynamic start that returns the checkpoint in its
`CheckpointStorage`. A subscription whose storage holds a catch-up's checkpoint now stops in the same
way, where it skipped the history before. A blocking Spring Boot subscription with `startAt` unset doesn't get there,
since it resumes from such a checkpoint, see
[A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint).
`ReactorDurableSubscriptionModel` hands the
reactor model the stored checkpoint itself, so a subscription at the model default whose storage holds one of these
stops too, its handle fails with the `UnsupportedStartAtException`, and the checkpoint stays in storage.

A `TimeBasedCheckpoint` isn't refused. A catch-up writes one when it can't get a global position, and reads it back. A
`DurableSubscriptionModel` with no catch-up around it and one of these in its storage still opens at the present.

A `CompetingConsumerSubscriptionModel` wrapping `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` hears
of a refusal of a dynamic start through `StartRefusalReportingSubscriptions`, also through a `DurableSubscriptionModel`
in between, although the refusal comes after `subscribe(..)` has returned. The node that got the lease gives it back,
frees the subscription id and logs the refusal at `ERROR`. The handle that node's `subscribe(..)` returned answers
`true` from `waitUntilStarted(..)` while it waits for the lease, as a waiting competing consumer always does, and throws
the `UnsupportedStartAtException` once the refusal has come. A refused node doesn't compete for the subscription again,
so once every node that subscribed it has been refused, the subscription runs on no node and its lease stays free.
Subscribe it again with the same id, on any node, with a start position the model accepts.

### What to do

Subscribe through the catch-up subscription model that stored the checkpoint. For a `DcbStartAt.dynamic(..)` that
returns `DcbStartAt.beginning()`, subscribe through a model composed with `DcbCatchupSubscriptionModel` or
`ReactorDcbCatchupSubscriptionModel`, which replays the history before going live, or return `DcbStartAt.now()` when
the subscription doesn't need the history.

For a subscription behind a catch-up whose `CheckpointStorage` holds a catch-up's checkpoint, restore the start it had
when the replay stopped, so the catch-up finishes the replay, or remove the stored checkpoint to start from the new
start position without the history. With more than one instance, stop every instance before you remove it, and start
them all again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.
A Spring Boot starter subscription at `DEFAULT` waits for its start by default, so the exception can stop the
application from starting.

### A record pattern over `NativeMongoSubscription` stops compiling

The `Subscription` that `NativeMongoSubscriptionModel.subscribe(..)` returns is a `NativeMongoSubscription`, and it
now holds a supplier of the failure that `waitUntilStarted(..)` throws, as a third record component called
`startFailure`. A record pattern has to name every component of the canonical constructor:

```java
// Before, stops compiling
case NativeMongoSubscription(var id, var latch) -> ...

// After
case NativeMongoSubscription(var id, var latch, var startFailure) -> ...
```

`org.occurrent.MigrateNativeMongoSubscriptionRecordPattern_0_35`, which `UpgradeToOccurrent_0_35` runs, adds the
binding to a `switch` case and an `instanceof` pattern alike. Constructor calls aren't affected. The two-argument
constructor stays, and `waitUntilStarted(..)` on an instance built with it only waits.

## A subscription whose resume token is gone stops instead of restarting from now

A MongoDB subscription continues from a resume token, a position in the oplog, which is MongoDB's log of recent writes.
When a subscription has been stopped or paused for longer than the oplog keeps its writes, the token is older than the
oldest entry and MongoDB can't resume from it. Before 0.35.0 the Spring Boot starters then restarted the subscription
from now, since `restart-on-change-stream-history-lost` defaults to `true`, and every event committed between the
token and the restart was never delivered. A subscription that a catch-up subscription runs now stops instead.

### Who is affected

Every subscription made through `CatchupSubscriptionModel`, `StreamCatchupSubscriptionModel`,
`DcbCatchupSubscriptionModel` or their reactor counterparts on MongoDB, including subscriptions that start at `NOW` or
`DEFAULT` and never replay. Both starters run every subscription through one by default:

- the blocking starter when `occurrent.event-store.capabilities` contains `STREAM`, or contains `DCB` and the context
  has a `DcbEventStore`
- the reactive starter when `occurrent.event-store.capabilities` contains `DCB` and the context has a DCB store, or
  when it contains `STREAM` and the stream store writes positions, which is the default

`restart-on-change-stream-history-lost` keeps its meaning only where the starter composes no catch-up subscription.

A catch-up subscription you build yourself is affected too, also when the MongoDB subscription model under it doesn't
restart, which is how that model is built by default. There the subscription used to end with an error in the log,
and the `Flux` of an unnamed reactor subscription failed with MongoDB's own error, code 286. It now stops as below.

On the reactor stack, an action that a catch-up subscription model runs, a `doOnCancel` on the `Mono` an action
returns and a handler on `waitUntilStarted()` can subscribe, cancel, stop or shut down through that model, from the
thread that runs the code or from another thread. A subscribe of an id throws `DuplicateSubscriptionIdException` until
the model the catch-up wraps has completed its cancel of that id. `ReactorMongoSubscriptionModel` completes it before
`cancelSubscription` returns.

A `StartAt.dynamic(..)` function that answers `null` to the catch-up can call the catch-up model too. The catch-up then
goes straight to live and hands the function to the model it wraps, and `ReactorMongoSubscriptionModel` can call it
again inside its own `subscribe(..)`, on the thread that called the catch-up. While that `subscribe(..)` runs, a
subscribe of the id throws `DuplicateSubscriptionIdException` until the id is cancelled. A cancel of the id then, from
that thread or another, goes to the wrapped model at once, and the action gets no event after it.

When that cancel reaches `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel` before the cancelled
`subscribe(..)` took the id there, the model registers nothing for it, and `waitUntilStarted()` on the
`Subscription` that `subscribe(..)` returned fails with `IllegalStateException`. In 0.34.0 that subscription
ran and `waitUntilStarted()` completed. When the model was shut down by the time it asks whether the `subscribe(..)` was
cancelled, `subscribe(..)` throws `SubscriptionModelShutdownException` instead and returns no `Subscription`. Both models take the id before they call the function, so only a cancel from
another thread can get there first. Only an instance of either exact class takes this path. A subclass or a proxy of
either model, a Spring AOP proxy for example, registers the cancelled `subscribe(..)` as 0.34.0 did, and the catch-up
then cancels the id there again, as described below. The catch-up can't tell whether a subclass overrides
`subscribe(String, SubscriptionFilter, StartAt, Function)` or whether a proxy advises it, so it calls that method on a
subclass or a proxy. A Mockito spy of either model is an instance of the exact class with Mockito's default inline mock
maker, so it takes this path, and a stub or a `verify` on its `subscribe(String, SubscriptionFilter, StartAt, Function)`
doesn't see a catch-up subscribe that goes straight to live.

If another thread can cancel a subscribe whose start you wait for, `waitUntilStarted()` can fail with an
`IllegalStateException` because of that cancel. That happens when:

- the cancel reached a `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel` of that exact class before
  it took the id
- the cancel, or the catch-up's second cancel of a subclass or a proxy, reached `ReactorMongoSubscriptionModel` while
  the subscription was starting there
- the subscribe replays history first and the cancel came before the replay handed over to the wrapped model

When the cancel reached `ReactorDurableSubscriptionModel` after it took the id and before it started the subscription
or handed it to the model it wraps, the exception is `CancellationException`, which extends `IllegalStateException`.

`IllegalStateException` also comes from failures that have nothing to do with a cancel, so treat it as the cancel only
when your application cancelled the id.

Another wrapped model can still register the cancelled `subscribe(..)` after the cancel. So can a subclass or a proxy
of `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel`, whether or not it overrides or advises
`subscribe(String, SubscriptionFilter, StartAt, Function)`. When the
wrapped `subscribe(..)` returns holding the id, the catch-up cancels the id there again, unless a subscribe of the id
made after the cancel took it there. That cancel waits until no other subscribe of the id is under way. Until that
second cancel has completed, a subscribe of the id throws `DuplicateSubscriptionIdException` whenever the wrapped model
holds the id for the cancelled `subscribe(..)`, also after the first cancel has completed. When the cancel came before
the wrapped model had taken the id for the cancelled `subscribe(..)`, the wrapped model can refuse a subscribe of the id
made after the cancel with `DuplicateSubscriptionIdException`, since the cancelled one can take the id there first. A
subscribe that replays history first gets that exception from `waitUntilStarted()` once the replay hands over. The
catch-up then cancels the id there again. A wrapped model that takes the id only after it called the function makes that
likely.

A `CatchupListener` given to `listenForCatchup` can still run while the catch-up holds a lock, so don't call the
catch-up model from it. The listener is told that a catch-up started from inside an operation on a map the catch-up
keeps.

### What happens

- A subscription that has gone live stops. That is every subscription that started at `NOW`, `DEFAULT`, a time or a
  position, whether it replayed first or not, and every subscription that resumed from a stored resume token after the
  application restarted.
- A replay whose live start is already gone when the subscription opens the change stream from it runs again from
  where it started, as it has since 0.34.0. A handler that sends mail or calls another service can then get events of
  the unfinished first replay again.

The catch-up doesn't replay from the lost token instead. On MongoDB an append takes its position before its
transaction commits, so an event can still commit below a position the subscription has already read. A replay from
any position other than the beginning could skip that event.

### What you see when a subscription stops

- An error in the log each time MongoDB reports the loss, naming the subscription and the resume token that is gone.
  For a subscription with an id, the MongoDB subscription model logs a warning next to it, that it paused the
  subscription and a listener took it over, as it has since 0.34.0. On a blocking catch-up subscription model you call
  directly, the warning can be all you get when a `resumeSubscription(id, startAt)` at another position runs before the
  error is logged. When such a resume clears a recorded loss, an information line says that an error logged earlier
  for the subscription no longer applies to the position it lost. When you paused or resumed the subscription just
  before MongoDB reported the loss, neither is logged and `resumeSubscription(id)` isn't refused. A subscription you
  resume with `resumeSubscription(id)` opens at the lost token again, MongoDB reports the loss again, and the
  subscription stops as described here, without skipping an event. One you resume with
  `resumeSubscription(id, startAt)` opens at `startAt` when it is a resume token.
- On the reactor stack `waitUntilStarted()` of a subscription with an id completes before the MongoDB subscription
  model finds the token gone, so it doesn't tell you the subscription stopped. Check `isPaused(id)` instead.
- The subscription is paused, `isPaused(id)` answers `true`, and it delivers nothing.
- After the error, `resumeSubscription(id)` throws `CatchupCannotResumeException` until you cancel the subscription,
  see [How to recover](#how-to-recover). With more than one instance, stop every instance and delete or replace the
  stored checkpoint instead of cancelling, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes. On a blocking catch-up subscription model you call directly, a
  `resumeSubscription(id, startAt)` can end that refusal too, even at a position whose history is gone, and even when
  the error is logged while it runs. A `resumeSubscription(id)` after the pause but before the error isn't refused
  yet. A resume that isn't refused never delivers from a position whose history is gone. When it opens the
  subscription there, MongoDB reports the loss again and the subscription is paused again with another error, so no
  event is skipped. On the blocking stack `start(true)` throws `CatchupCannotResumeException` too. On the reactor
  stack `start(true)` resumes the subscription in the MongoDB subscription model, which finds the token gone again and
  pauses it again with one more error.
- On the blocking stack, a subscription you start from a resume token that is already gone throws
  `CatchupCannotResumeException` from `subscribe`, and nothing is stored. Under competing consumers the start runs
  when a node acquires the lease. When it fails there, that node pauses the subscription, logs an error that names
  it, and gives up the lease. The node doesn't compete for the lease again until you resume the subscription, with `resumeSubscription(id)` or
  `start(true)`.
  Another node that waits for the lease can then get it, and pauses the subscription the same way. On the reactor stack such a subscription with an id is paused as
  above once the MongoDB subscription model opens it, and nothing is stored.
- In Spring Boot the blocking starter throws `CatchupCannotResumeException` when it registers a subscription whose
  start reads the stored resume token and that token is gone, which can keep the application from starting. Once a
  subscription has gone live and stored a resume token, these starts read it:
  - `startAt = BEGINNING` and `startAtGlobalPosition` on `@Subscription`, `@Projection`, `@Saga` and `@Snapshot`,
    under the default `resumeBehavior`. `@Snapshot` starts at `BEGINNING` when you don't set `startAt`, so a
    `@Snapshot` with no start attribute is one of them.
  - `startAt = BEGINNING_OF_TIME`, `startAtISO8601` and `startAtTimeEpochMillis` on `@StreamSubscription`, under the
    default `resumeBehavior`.
  - `startAt = DEFAULT` on every annotation, whatever the `resumeBehavior`. So every subscription with no start
    attribute is one of them, apart from a `@Snapshot`, which starts at `BEGINNING` and is in the first item. On a
    `@DcbSubscription`, a `@Projection` that returns a `DcbProjection` and a `@Snapshot` that returns a
    `DcbSnapshotView`, under the default `resumeBehavior`, `startAt = BEGINNING` reads it too, and so do
    `startAtDcbPosition` on `@DcbSubscription` and `startAtGlobalPosition` on the other two.
  - Every start, `NOW` and `DEFAULT` included, when a `SubscriptionResumePoints` bean answers a resume token for the
    subscription.

  A `NOW` start that no `SubscriptionResumePoints` bean answers for doesn't read the stored token and opens at the
  present. Every reactive subscription is paused as above.

  A subscription with `startupMode = BACKGROUND` on the annotation whose stored resume token is gone is paused
  instead, with an error in the log that names it, and the rest of the application starts. With
  `occurrent.subscription.mode=manual` that happens in `start(true)`, which goes on to start the other subscriptions,
  or in the `resumeSubscription(id)` that first starts the subscription, which doesn't throw.
  The node doesn't compete for the lease again until you resume the subscription, as the competing consumers item
  above describes. A later `resumeSubscription(id)` competes again, and throws `CatchupCannotResumeException` when this node
  gets the lease during the call and the token is still gone. With `startupMode`
  unset the application doesn't start on a node that holds the lease when the subscription registers. While the
  subscription model runs, a node that waits for the lease starts in either mode, and pauses the subscription once it
  gets the lease, as above.
- The `Flux` of an unnamed reactor subscription fails with `CatchupCannotResumeException`.

### How to recover

Cancelling a subscription whose checkpoint Spring Boot stores deletes the checkpoint. When the subscription is
registered again, it starts where its annotation says. So cancelling and subscribing again with `NOW` or `DEFAULT`
skips every event since the lost token. For a `NOW` or `DEFAULT` subscription the only ways back without a gap are to
switch it to `BEGINNING`, or to resume it from a checkpoint the application kept that the subscription model still
has the history back to. Outside Spring Boot, on a blocking catch-up subscription model you call directly, the second
can be a `resumeSubscription(id, StartAt)` at that checkpoint, as described below.

A step below that cancels the subscription is for one instance. With more than one, stop every instance, delete the
stored checkpoint where a step cancels, and start them all again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
describes. Do the same with one instance when the application doesn't start, or you would rather not call
`cancelSubscription`.

A subscription that the blocking starter paused under `startupMode = BACKGROUND` recovers the same way. Delete its
stored checkpoint, or replace it with a resume token the oplog still has, with every instance stopped and started
again with the same start attribute, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.
It then starts where its annotation says, or from the resume token you stored.
Only a token the application kept with its own state starts it without a gap. A newer one skips the events between
the last one the handler handled and that token.

To replay from the beginning, when the read model or saga can take every event again:

1. Set `startAt = BEGINNING` on the annotation. In code, subscribe from the beginning, for example
   `DcbStartAt.beginning()` for a DCB subscription.
2. With one instance, cancel the subscription with `cancelSubscription(id)`, which deletes its stored checkpoint.
3. Register it again, for example by restarting the application. With nothing stored, it replays from the beginning
   and then goes live.

To rebuild the read model, when it can't take an event twice:

1. Stop every instance of the application, or with one instance cancel the subscription.
2. Delete the read model's data and the subscription's stored checkpoint. Stopping the application keeps the
   checkpoint, and the next start would refuse it again.
3. Replay from the beginning as above, and start every instance again with the same start, as
   [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
   describes.

To resume from a checkpoint the application kept, when it stores the checkpoint delivered with each event:

1. With one instance, cancel the subscription.
2. Subscribe again from that checkpoint, or answer it from a `SubscriptionResumePoints` bean. A checkpoint from a
   replayed event never resumes with a gap. When the oplog no longer has the live start that checkpoint holds and its
   replay didn't start at the beginning, the subscribe throws `CatchupCannotResumeException`. Under
   `startupMode = BACKGROUND` or `occurrent.subscription.mode=manual` the subscription is paused instead, as above. A
   resume token from a live event resumes only while the oplog still has it.

On a blocking catch-up subscription model you call directly, you can keep the subscription instead of cancelling it,
when the token you kept is from a live event and the oplog still has it. Call
`resumeSubscription(id, StartAt.checkpoint(new StringBasedCheckpoint(token)))` on the catch-up model. It runs the
paused subscription again from that token, and a later `resumeSubscription(id)` no longer throws. This resume never
replays, so a checkpoint from a replayed event doesn't work here. When the catch-up model wraps a
`DurableSubscriptionModel`, that model stores the token as the subscription's checkpoint.

The blocking Spring Boot starter puts a `CompetingConsumerSubscriptionModel` around the catch-up model, and that model
has no resume at a position, see [#1304](https://github.com/johanhaleby/occurrent/issues/1304). So in Spring Boot,
recover as above.

To continue from now, when skipping the events since the lost token is acceptable:

1. With one instance, cancel the subscription.
2. Subscribe again with `StartAt.now()`, or register it again with `startAt = NOW` or the default. The events
   committed between the lost token and now are never delivered.

#### Changing a checkpoint or marker that several instances share

When more than one instance of the application runs a subscription, every instance reads what is stored under its id.
For a durable or catch-up subscription that is its checkpoint, and for a subscription with `source = PUSH` it is its
catch-up marker. Two things about how the application runs decide how to change it, and what else a new subscription
id needs:

- Where the handler keeps what it builds. Either every instance writes to one store, such as a read model in MongoDB,
  or each instance keeps its own, for example in memory.
- For a subscription with `source = PUSH`, which RabbitMQ queue or Kafka consumer group each instance consumes.
  Instances that consume one queue or group share its events, and each event goes to one of them. The application
  names the queue or the group when it builds the source, and a new subscription id doesn't change it.

When every instance writes to one store, delete or replace it like this instead of cancelling the subscription on one
instance:

1. Stop every instance of the application that can write the checkpoint or marker.
2. Delete the stored checkpoint or marker, or replace the checkpoint with the one to start from. Spring Boot stores
   both in the collection `occurrent.subscription.mongodb.collection` names, `subscriptions` by default, as the
   document whose `_id` is the subscription id.
3. Start every instance again with the same start. In Spring Boot that is the same start attribute on the annotation.

The instance that starts first runs the subscription from that start, and what it writes to the store is there for
every instance. An instance that starts once the first one has stored its checkpoint or marker finds it, and resumes
after it or skips its replay. The store already has those events from the first instance. On the reactive starter and
for `source = PUSH`, an instance that starts before that runs from the same start too, so the store gets those events
from more than one instance, as on any start that finds nothing stored. Deleting the checkpoint of a subscription that
starts at the present loses the events since that checkpoint, even with every instance stopped, because the first
instance finds nothing stored and starts at the present.

When each instance keeps what it builds for itself, for example in memory, one checkpoint or marker can't serve every
instance. The one that starts first stores it, and every instance that starts after resumes after it or skips its
replay, so what that instance builds misses those events. That happens on every start, also when nothing was deleted.
So instances can share a subscription id only when they write to one store, and a step in this guide that stops every
instance and starts them all again with the same start is for instances that do. Give each instance a subscription id
of its own instead:

- The checkpoint and the catch-up marker are stored under the subscription id, so each instance then reads only what
  it stored itself, and a change to it needs only that instance stopped.
- Under the blocking starter the lease is taken per subscription id too, so each instance holds the lease of its own
  id and runs its subscription.
- For a subscription with `source = PUSH` the broker decides which live events reach an instance, not the id. Each
  instance needs a RabbitMQ queue or a Kafka consumer group of its own, or it gets only some of the live events.
- An annotation's `id` is a constant, so every instance of the application has the same one. Subscribe such a
  subscription in code instead.

When what an instance builds is gone after a restart, an id of its own still resumes from what that instance stored
before it. Store nothing for it then, so every start replays from its start. For example, build a
`CatchupThenPushSubscriptionModel` with `null` as its catch-up marker storage.

`cancelSubscription(id)` deletes what every instance shares, and the other instances keep running and can store it
again before the instance you recover subscribes again:

- On the blocking starter, the instance that holds the lease runs the subscription. A cancel gives up the lease only
  on the instance you call it on, so another instance waiting for the lease can take it, start from its own start, at
  the present for `NOW` or `DEFAULT`, and store a newer checkpoint. A delete while an instance still runs has the same
  effect, also after a refused start, since the instance that refused gives the lease back and another instance that
  still competes can take it. The events between the old checkpoint and the newer one are never delivered.
- On the reactive starter every instance runs the subscription and stores the checkpoint, so a newer one can be stored
  the same way.
- A subscription with `source = PUSH` runs on every instance, and each instance stores the marker once its own replay
  is done. When it does that after your cancel deleted the marker, the subscribe on the instance you recover finds the
  marker and skips its replay on the blocking stack, or replays only the events after it on the reactive stack.

A new subscription id has nothing stored under it, so the subscription starts from its start, and the catch-up of a
subscription with `source = PUSH` replays the whole history. Use one for a rebuild or a changed filter or handler. Once
no instance runs the old id, delete what is stored under it.

A new id keeps the new subscription apart from the checkpoint and the marker of the old one, but not from the store
the old one writes to, or from the queue or consumer group it consumes. Under the new id the subscription repeats
everything its handler does for every event from its start, the records it writes, the commands it dispatches and the
messages it sends. For a rebuild that is the whole history. Stop every instance on the old id before the first
instance on the new id starts. Stopping them doesn't prevent the repeats.

What else a new id takes depends on the deployment:

- When every instance writes to one store, give every instance the same new id.
- When each instance keeps what it builds for itself, a new id that every instance shares has the same problem as the
  old one, since every instance that starts after the first finds what the first one stored under it. Give each
  instance a new id of its own, and for `source = PUSH` keep each instance on a queue or consumer group of its own.

An instance on the old id that consumes the queue or consumer group of the new subscription gets some of the events
stored after the catch-up of the new subscription read the history. `RabbitMqCloudEventSource` acknowledges such an
event and `KafkaCloudEventSource` commits its offset once the old subscription has handled it, and also when the old
filter declines it. `RabbitMqDomainEventSource` and `KafkaDomainEventSource` do the same. So the new subscription
never gets that event.

A new RabbitMQ queue gets only the messages published after it is bound, so it has to exist before the subscription on
the new id starts. Building the `RabbitMqCloudEventSource` creates the queue and its bindings, unless it is built with
`declareTopology(false)`.

A step in this guide that says "with one instance" means that one process runs the subscription and nothing else
writes to its checkpoint storage under that id. A subscription model that the application calls directly doesn't
make that so. Two `DurableSubscriptionModel`s can share one `CheckpointStorage`, and a cancel on one deletes the
checkpoint the other one keeps writing. A subscription that stores nothing can always be cancelled and subscribed again
without stopping anything, such as one on a `SynchronousSubscriptionModel`, which takes no `CheckpointStorage`, or an
unnamed reactor `Flux`.

### How to keep it from happening

Keep the oplog window longer than the longest time a subscription can be stopped, paused or replaying. On the
blocking stack a subscription with an id that has handled an event since it subscribed saves its position about once a
minute by default while it matches no events, so for it the time that counts is the time it is down. One that resumed
from the position stored for it after the application restarted, or whose replay ended, saves no position while it
matches no events until it handles an event, so the position stored for it stays where it was. For it the time that
counts is the time since that position was written. On the reactor stack a subscription with an id saves its position
while it matches no events only while the MongoDB subscription model reads quiet positions. When it can't, it logs a
warning saying that a subscription that matches no event keeps the position of the last event it handled. An unnamed
reactor `Flux` saves no position, and its position moves only when it reads an event.

## A Spring Mongo event store sends a commit again when MongoDB can't say whether it succeeded

MongoDB labels a commit it could not answer `UnknownTransactionCommitResult`: the commit reached the server, or may
have, and the reply was lost. The native `MongoEventStore` sends such a commit again for up to two minutes, because it
runs through the driver's `withTransaction`. `SpringMongoEventStore` and `ReactorMongoEventStore` ran through Spring's
`MongoTransactionManager` and `ReactiveMongoTransactionManager`, which retry nothing, so an append that may have been
saved was reported as a failure. A caller that sent it again could write its events twice.

0.35.0 adds `CommitRetryingMongoTransactionManager` in `org.occurrent.eventstore.mongodb.spring.blocking` and
`CommitRetryingReactiveMongoTransactionManager` in `org.occurrent.eventstore.mongodb.spring.reactor`. Each extends
Spring's manager and sends the commit again for up to two minutes from the first attempt. It does not retry a
top-level `MaxTimeMSExpired` error, as the driver doesn't, and neither does it retry a write concern error with that
code, which the driver raises as the same exception.

### Who is affected

- With the Spring Boot starters and no `MongoTransactionManager` or `ReactiveMongoTransactionManager` bean of your own,
  nothing to do. The starters create the new managers.
- With a bean of your own, or an event store you wire by hand, the retry is off until you switch the manager. The blocking
  store logs a warning when it is created, and the reactive one when `EventStoreConfig.Builder.transactionConfig(..)` is
  called with a `ReactiveTransactionManager` that is not the new one. `transactionConfig(TransactionalOperator)` logs
  nothing, because the operator does not expose its manager.

### What to do

Replace the manager and keep its other arguments. Each new class has the constructors of the one it replaces that take a
database factory, with transaction options or with an options resolver and default options:

```java
// Before
MongoTransactionManager transactionManager = new MongoTransactionManager(databaseFactory, transactionOptions);
// After
MongoTransactionManager transactionManager = new CommitRetryingMongoTransactionManager(databaseFactory, transactionOptions);
```

```java
// Before
ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
// After
ReactiveMongoTransactionManager transactionManager = new CommitRetryingReactiveMongoTransactionManager(databaseFactory);
```

When your code starts a transaction and the event store joins it, the commit belongs to your transaction manager, so
that one has to be the new manager for the retry to apply.

An append whose commit is still unknown after two minutes fails as before, and it may or may not be saved.

## A restart follows `resumeBehavior` for `startAt = BEGINNING_OF_TIME` and `startAt = NOW`

Some subscriptions did not start where `resumeBehavior` says on a restart. On the reactive Spring Boot starter, a
`@StreamSubscription` ignored `resumeBehavior`. With `startAt = BEGINNING_OF_TIME` it started at position 0 on every
restart, and with `startAt = NOW` it started at the present on every restart. A reactive `@Subscription`,
`@DcbSubscription`, `@Projection` or `@Snapshot` with `startAt = NOW` did the same, while their replay starts already
followed `resumeBehavior`. On the blocking starter, `startAt = NOW` with `resumeBehavior = SAME_AS_START_AT` continued
from the checkpoint the subscription had stored, where the documentation says it starts at the present again. Both
starters now do what the javadoc and the documentation say.

### Who is affected

- Every application on the reactive starter with a `@StreamSubscription` that sets `startAt` to `BEGINNING_OF_TIME` or
  `NOW`. That includes one that sets `resumeBehavior = DEFAULT` explicitly. A `@StreamSubscription` that does not set
  `startAt` is not affected, and neither is one that sets `resumeBehavior = SAME_AS_START_AT`, except that
  `BEGINNING_OF_TIME` no longer stores a checkpoint.
- Every application on the reactive starter with a `@Subscription`, `@DcbSubscription`, `@Projection` or `@Snapshot`
  that sets `startAt = NOW`, including one that sets `resumeBehavior = DEFAULT` explicitly. One that does not set
  `startAt`, or sets `resumeBehavior = SAME_AS_START_AT`, is not affected.
- Every application on the blocking starter with `startAt = NOW` and `resumeBehavior = SAME_AS_START_AT` on a
  `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@Projection`, `@Saga` or `@Snapshot`.

### What changes

On the reactive starter:

- `BEGINNING_OF_TIME` with `DEFAULT` replays the history the first time. After a restart it continues from the
  checkpoint it stored, so the handler no longer receives every event again.
- `NOW` with `DEFAULT` starts at the present the first time. After a restart it continues from the checkpoint it
  stored, so the handler receives the events written while the application was down. Before, it never received them.
- `SAME_AS_START_AT` starts at `startAt` on every restart, as the documentation says. `BEGINNING_OF_TIME` with
  `SAME_AS_START_AT` now stores no checkpoint, the way a reactive `@Subscription` already behaves.

`NOW` with `SAME_AS_START_AT` still stores a checkpoint after each event, as before, and ignores it on restart. Changing
that subscription to `DEFAULT` later continues from the checkpoint it stored.

The checkpoint is the one the starter's durable subscription model stores. An application that supplies its own
`FluxSubscriptionModel` or `Subscribable` gets no durable model from the starter. A `RegisteringSubscribable`, such as
`SynchronousSubscriptionModel`, does not count. With its own model, the restart behaviour above is that model's: `NOW`
with `DEFAULT` starts where its default start does, and whether `BEGINNING_OF_TIME` with `DEFAULT` continues or replays
again depends on whether it stores a checkpoint.

With the starter's durable model, `NOW` with `DEFAULT` also records where the feed is when a subscription with no stored
checkpoint registers, the way any start from the model default does. The reactive MongoDB model reads it by asking the
server. It asks on every restart, and ignores the answer when a checkpoint is stored. A server that cannot be asked, or
that replies without a clock, therefore makes the model refuse the registration only when no checkpoint is stored. Such
a subscription waits for its start by default when `occurrent.subscription.mode` is `auto`, so the application does not
start. With `startupMode = BACKGROUND`, with mode `manual`, or on a bean built after startup,
the refusal does not hold up startup, and the subscription is not running.
`occurrent.subscription.start-when-no-start-position-can-be-recorded=true` does not change this for the reactive MongoDB
model, which fails when it cannot be asked or replies without a clock. The property is described in
[section 7 of the 0.34.0 guide](upgrading-to-0.34.0.md#7-durablesubscriptionmodel-refuses-a-first-subscription-when-no-start-position-can-be-recorded).

A subscription that handled an event before the upgrade normally has a checkpoint stored already, because the old start
stored one after each event too. The first restart after the upgrade continues from it, and `startAt` is not looked at
again for that subscription id. So changing `startAt` on an existing id has no effect until the id is new or its
checkpoint is deleted. If the position of that checkpoint is no longer in the oplog, the subscription stops, see
[a subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).

On the blocking starter, `NOW` with `SAME_AS_START_AT` starts at the present on every restart and stores no
checkpoint, so the events written while the application was down are not delivered. Before, it delivered them once it
had stored a checkpoint. When it's paused and resumed with no `SubscriptionResumePoints` bean answering for it, it
doesn't replay from a catch-up checkpoint in storage. As for any `NOW` start, an event written shortly before the
subscription starts can still be delivered, and a `SubscriptionResumePoints` bean that answers for the subscription id
still decides where it starts.
It also no longer competes with the other instances of the application, so every instance is delivered every event, as the
documentation says for `SAME_AS_START_AT`. A checkpoint it stored before the upgrade stays in storage and is not used.
`NOW` with `DEFAULT` is unchanged.

### What to do

Nothing, if the subscription is meant to behave as the documentation describes.

On the reactive starter, set `resumeBehavior = SAME_AS_START_AT` on a subscription that feeds an in-memory read model
that has to be rebuilt on every boot. Without it the read model now starts empty and receives only the events after the
last checkpoint. A `NOW` subscription that has to skip the events written during downtime also needs
`SAME_AS_START_AT`.

A reactive `NOW` subscription with the default `resumeBehavior` now needs the MongoDB server to answer when it
registers with no stored checkpoint, and refuses the registration when it cannot be asked.

On the blocking starter, set `resumeBehavior = DEFAULT` on a `NOW` subscription that relied on resuming from its
checkpoint after a restart, or on competing with the other instances. Without it the subscription now misses the events
written while the application was down, and runs on every instance. Delete the old checkpoint of a subscription you
later switch to `DEFAULT`, or it resumes from that position. Delete it with every instance stopped, and start them all
again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.

## A subscription with no start position resumes from a catch-up's checkpoint

On the blocking starter, a subscription with `startAt` unset didn't resume from a checkpoint that a catch-up stored. A
catch-up stores a `GlobalCheckpoint` or a `CatchupTimeCheckpoint` while it replays, and that checkpoint stays in storage
when the replay stops before it goes live. In 0.34.0 the subscription then opened the change stream at the present, and
skipped every event written before it, the ones written while the application was down included, with no error. It now
resumes the replay from that checkpoint and goes live after it.

### Who is affected

Every application on the blocking starter with one of these, when a catch-up stored its checkpoint:

- a `@Subscription` or `@StreamSubscription` with no `startAt`
- a `@Saga` with no `startAt` that reads the event store rather than a push feed
- a `@Projection` with no `startAt` that reads the event store and doesn't return a `DcbProjection`
- a `@Snapshot` with `startAt = DEFAULT` that doesn't return a `DcbSnapshotView`

Storage holds such a checkpoint when the subscription ran from `startAt = BEGINNING`, a global position or a time, the
application stopped before the replay went live, and the start was changed to `DEFAULT` before the next boot. A
competing-consumer node that takes such a subscription over reads the same checkpoint.

The reactive starter, a `@DcbSubscription`, and a projection or snapshot over DCB already resumed from it.

### What changes

- With a catch-up's checkpoint stored, the subscription replays from it and then goes live, so it's delivered the
  events written since. The catch-up refuses this start in the same cases as a `BEGINNING` start that restarts from
  that checkpoint. In `auto` subscription mode, the default, a `BEGINNING` start replays in the background, but a
  subscription with `startAt` unset can replay before the application finishes starting, unless the annotation sets
  `startupMode = BACKGROUND`. A `resumeSubscription(id)` of such a subscription after a pause replays from that
  checkpoint too, where it ran no replay before.
- With a `TimeBasedCheckpoint` stored that holds no live start, the start is refused, as
  [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it)
  describes.
- With nothing stored, it starts at the present without a replay, as before.
- With a live checkpoint stored, it resumes from it, as before. When MongoDB no longer has the history back to that
  checkpoint, the start throws `CatchupCannotResumeException` and the application doesn't start, the same as a
  `BEGINNING` start that restarts from its checkpoint. With `startupMode = BACKGROUND` the subscription is paused
  instead, see
  [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
- A `@Projection` with `recordAppliedAppends = true` and no `startAt` no longer gets the startup warning that its
  recordings never reset, since it replays when a catch-up stored its checkpoint.

### What to do

Nothing, unless the handler can't take the events the replay delivers. The replay can run long when the checkpoint is
far behind, and the application doesn't finish starting until it's done. Set `startupMode = BACKGROUND` on the
annotation to start the application first. To start at the present instead, delete the stored checkpoint with every
instance stopped, and start them all again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.

A subscription whose live checkpoint MongoDB no longer has stops the application from starting. Set
`startupMode = BACKGROUND` to have it paused instead, and see
[A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now)
for how to recover it.

## A handler method the bean inherits is registered

Both Spring Boot starters used to read a bean's handlers only from the methods its class declares. An annotated method
that the class inherits from a superclass, or gets as an interface default method, never registered when the class
didn't override it, and nothing was logged. Both starters now register it. Two kinds did register in 0.34.0. When a
public class inherits a public annotated method from a superclass that isn't public, the compiler adds a bridge
method to the class that calls the inherited method and has its annotation, and 0.34.0 found that bridge. Such a
handler keeps running under the same id. When it is declared with a type variable, its types now come from the bean's
class, see What changes below. The other is an annotated interface method, default or abstract, on a bean behind a
JDK proxy whose target can't be unwrapped. 0.34.0 read the proxy's class, which declares every method of its
interfaces, so it found that method, and both starters still register it. When it is declared with a type variable,
its types now come from what the proxy's interfaces bind, see What changes below.

When a superclass or interface of the bean has a method that names a class missing at runtime, such as a type from an
optional dependency, both starters read only the methods the bean's class declares, as 0.34.0 did. They read that
superclass's or interface's class file without loading it, and the methods of every other superclass and interface.
When one of them declares a handler, the application now fails at startup with an `IllegalStateException` that names
the handler and the missing class, where 0.34.0 started. It doesn't fail when the bean's class declares a method that
runs that handler, which is an override with a handler annotation, or the bridge method the compiler adds to a public
class for a public handler it inherits from a superclass that isn't public. Both keep running as in 0.34.0. It fails
the same way when that class file can't be read either. Put the missing class on the classpath. When none declares a
handler, both starters log at debug level that they skip the inherited methods.

This covers `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@SynchronousSubscription`, `@Projection` and
`@Snapshot`, and `@Saga` on the blocking starter. An inherited `@Projection`, `@Snapshot` or `@Saga` factory method,
which was never found either, runs too.

An override registered before and still does. Without an annotation of its own, it runs under the overridden method's
annotation, and when it repeats the annotation, it uses its own. A method with the same name and parameters as a
private superclass method, or as a package-private one in a superclass in another package, doesn't override it. When
both have a handler annotation, both now register, see What changes below.

### Who is affected

Applications on either starter with one of these:

- a bean whose class inherits an annotated method from a superclass and doesn't override it, other than the bridge
  kind above
- a bean whose class implements an interface with an annotated default method and doesn't override it
- two or more beans that run one annotated method without overriding it, either two subclasses of the class that
  declares it, or that class and a subclass, each a bean
- a bean whose class declares a method with the same name and parameters as a package-private annotated method that a
  superclass in another package declares
- a bean whose class repeats a handler annotation on a method with the same name and parameters as a private annotated
  superclass method
- a bean whose class declares a static method with the same name and parameters as an annotated static method of a
  superclass
- a bean whose class inherits a handler or factory method of the bridge kind above that is declared with a type
  variable
- a bean that is a JDK proxy of the kind above with a handler or factory method declared with a type variable on a
  generic interface, when one of the proxy's interfaces binds that variable
- a bean that is a JDK proxy of the kind above, when two of its interfaces declare a method with the same name and
  parameters, each with the same handler annotation but with different attributes, such as the id

### What changes

- An inherited handler, other than the two kinds above, starts running on the first start after the upgrade, from
  its own start settings. When nothing is stored for its id, `startAt = BEGINNING`, or `startAt = BEGINNING_OF_TIME`
  on `@StreamSubscription`, replays the whole history. So does a `@Snapshot`, whose `startAt` is `BEGINNING` by
  default, and a `@Projection` or `@Saga` with `source = PUSH`, which replays the event store's history before it goes
  live unless it sets `catchup = NONE`. When a handler with that id ran before, for example before the method moved
  into a superclass, its position is stored, and under the default `resumeBehavior` it resumes from there. It then
  gets every event written since that handler last ran.
- A handler declared with a type variable, such as `on(E event)` on `Base<E>`, takes its event type from the bean's
  class. A class that extends `Base<Placed>` subscribes to `Placed`. An inherited factory method declared with a type
  variable reads its state type the same way. This changes the bridge kind above too. 0.34.0 read its types from the
  bridge, which has only the erased types. So with `Base<E extends OrderEvent>`, such a handler subscribed to
  `OrderEvent` and now subscribes to `Placed` only, under the same id. When its annotation's `eventTypes` lists a
  type that isn't `Placed` or a subtype of it, the application now fails at startup with an `IllegalStateException`,
  where 0.34.0 subscribed it to those types. Such a factory method found no state type, so it failed at startup
  unless it named its store or the application had one bean of the store type. It keeps that store, and where it
  failed, it now gets the zero-config store.
- The JDK proxy kind above takes its types from what the proxy's interfaces bind. 0.34.0 read them from the proxy
  class's own method, which has only the erased types. So when `Handler<E extends OrderEvent>` declares the handler
  and the proxy implements `PlacedHandler extends Handler<Placed>`, the handler subscribed to `OrderEvent` and now
  subscribes to `Placed` only, under the same id. A proxy that implements `Handler` itself still subscribes to
  `OrderEvent`. When the annotation's `eventTypes` lists a type that isn't `Placed` or a subtype of it, such as
  another subtype of `OrderEvent`, the application now fails at startup with an `IllegalStateException`. A factory
  method of that kind found no state type either, so it failed at startup unless it named its store or the
  application had one bean of the store type. It keeps that store, and where it failed, it now gets the zero-config
  store for the type the interfaces bind.
- When two interfaces of the JDK proxy kind above declare a method with the same name and parameters, the proxy has
  one method for both. 0.34.0 found the annotation of the first interface the proxy lists on it, so only that handler
  registered, and the order of the interfaces decided which. When the two annotations differ in any attribute, such
  as the id, the application now fails at startup with an `IllegalArgumentException` that names both methods. When
  they're equal in every attribute, the handler registers once, as before.
- Two beans that run one annotated method register two handlers with the same id, so the application fails at
  startup with `DuplicateSubscriptionIdException`. That holds when both inherit it, and when one of them is a bean of
  the class that declares it. The id is the key the subscription's checkpoint is stored under,
  so two handlers can't share it.
- A bean's class can repeat a handler annotation on a method with the same name and parameters as an annotated
  superclass method that it doesn't override, because the superclass method is private, or package-private in a
  superclass in another package. In 0.34.0 only the class's method ran. Now both methods register. When the two ids
  differ, the superclass method starts running too, as an inherited handler does above. When the ids are the same,
  the application fails at startup with `DuplicateSubscriptionIdException`.
- An inherited subscription handler that is static fails at startup with `SubscriptionHandlerNotInvocableException`.
  So does one on a bean behind a CGLIB proxy when it is private, final, or package-private in another package than
  the bean's class, since the proxy can't override it. It also fails there when it is package-private and a class in
  another package, between it and the bean's class, declares a public or protected method with the same name and
  parameters, since the proxy runs that method instead. A handler method the bean's class declares already got the
  same refusal, except the two package-private cases, which can't happen there. Behind a JDK interface proxy, an
  inherited subscription handler that is private or package-private fails at startup the same way, since the proxy
  runs only methods of its interfaces. That holds when one of them declares a method with the same name and
  parameters too, since the bean's class implements that method with another body.
- An inherited `@Projection`, `@Snapshot` or `@Saga` factory method that is private, final, or package-private in
  one of the two cases above fails at startup with an `IllegalStateException` when the bean is behind a CGLIB proxy
  whose target can't be unwrapped, for example a proxy
  built with a custom `TargetSource` whose `isStatic()` returns `false`. A factory method the bean's class declares
  runs there as before, and a static factory method always runs, since it reads no bean.
- In Kotlin a superclass method is final unless it is marked `open`, and the `kotlin-spring` plugin opens a class only
  when the class itself has a Spring annotation such as `@Component`. So a subscription handler on a plain Kotlin
  superclass fails at startup when the bean that inherits it is behind a CGLIB proxy, for example because it has a
  `@Transactional` method. A `@Projection`, `@Snapshot` or `@Saga` factory method there fails only when the proxy's
  target can't be unwrapped, as above.
- A class that overrides a generic handler with a concrete parameter type, such as `on(String event)` in a class that
  extends `Base<String>` and overrides `on(E event)`, failed at startup with `DuplicateSubscriptionIdException`. The
  scan also found the bridge method `on(Object)` that the compiler adds, under the same id. The handler now registers
  once.
- A package-private method doesn't override a method of a superclass in another package, even with the same name and
  parameters, but both starters treated it as if it did. Say `Base` in package `a` declares a package-private
  `@Subscription(id = "h") void on(Placed event)`, and `Sub` in package `b` extends it and declares
  `void on(Placed event)` with no annotation. A `Sub` bean ran `Sub.on` under id `h`. The application now fails at
  startup, and the error names both methods. A `@Projection`, `@Snapshot` or `@Saga` factory method changes the same
  way.
- A static method doesn't override a static method of a superclass with the same name and parameters, it hides it,
  but both starters ran it under that method's annotation. Say `Base` declares `@Projection(id = "p")` on a static
  `projection()`, and `Sub` extends it and declares a static `projection()` with no annotation. A `Sub` bean ran
  `Sub.projection` as projection `p`. The application now fails at startup with an `IllegalStateException` that names
  both methods. When `Sub.projection` repeats `@Projection(id = "p")`, both methods register, so the application
  fails at startup with `DuplicateSubscriptionIdException`, and with different ids `Base.projection` starts running
  too. This covers `@Projection` and `@Snapshot`, and `@Saga` on the blocking starter. A static subscription handler
  failed at startup in 0.34.0 too, and when a subclass hides it, the error is now that `IllegalStateException`
  instead of `SubscriptionHandlerNotInvocableException`.

### What to do

Before you upgrade, search for Occurrent annotations on methods of classes your beans extend, and on interface default
methods. For each one a bean inherits without overriding it:

- If the handler should run, check where its `startAt` starts it. `BEGINNING` replays the history it missed, and
  `NOW` starts at the present.
- If it shouldn't run on any bean, remove the annotation from the method. A handler of the two kinds above keeps
  its annotation, since it ran in 0.34.0 and removing the annotation stops a subscription that has a stored
  checkpoint.
- If only some of the classes that inherit it should run it, move the annotation off the superclass method. Then
  override the method in each class that should run it and repeat the annotation there. An override without the
  annotation runs under the annotation of the method it overrides, so leaving the annotation on the superclass keeps
  it running everywhere.
- If it's a static subscription handler, make it an instance method. If the bean is behind a CGLIB proxy and the
  method is private, final, or package-private in another package, make it public or protected and not final. In
  Kotlin, mark it `open`. If it's package-private and the proxy runs a public or protected method of the same name and
  parameters from a class in another package, rename one of the two methods. For a `@Projection`, `@Snapshot` or
  `@Saga` factory method, this is needed only when the proxy's target can't be unwrapped. If the bean is behind a
  JDK interface proxy and the handler is private or package-private, make it public and declare it on an interface
  the proxy implements.
- If it's package-private and a subclass in another package declares a method with the same name and parameters and
  no handler annotation of its own, the application fails at startup. To keep the subclass's method as the handler,
  which is what ran before, make both methods protected or public, so the subclass's method overrides the superclass
  method. To run the superclass method instead, rename the subclass's method.
- If it's private, or package-private and declared in another package than a subclass, and the subclass declares a
  method with the same name and parameters that repeats the handler annotation, both methods run. To keep only the
  subclass's method, which is what ran before, make both methods protected or public, so the subclass's method
  overrides the superclass method, or remove the annotation from the superclass method.
- If it's a static `@Projection`, `@Snapshot` or `@Saga` factory method and a subclass declares a static method with
  the same name and parameters, the application fails at startup, or registers both when that method repeats the
  annotation. To run the subclass's method, which is what ran before, put the annotation on it and remove it from the
  superclass method. To run the superclass method instead, rename the subclass's method.
- If it's a handler of either kind above declared with a type variable, and the bean's class, or the proxy's
  interfaces, give the variable a narrower type than its bound, events of the bound's other types stop reaching it.
  To keep the types it subscribed to in 0.34.0, make one of these changes before you upgrade. For the bridge kind,
  bind the variable to its bound, for example `extends Base<OrderEvent>`, or extend `Base` raw. For the JDK proxy
  kind, have the proxy implement an interface that binds the variable to its bound, such as
  `OrderHandler extends Handler<OrderEvent>`, or have it implement `Handler` itself in place of `PlacedHandler`. To
  keep the narrower type instead, remove from its `eventTypes` the types that aren't that type or a subtype of it, or
  the application fails at startup. A factory method of either kind keeps the store it named, or the one bean of the
  store type it got.
- If two or more beans run it, override the method in each subclass and repeat the annotation with an id of its own.
  A bean of the class that declares it keeps the original id. If the method is private, or package-private and
  declared in another package than a subclass, the subclasses can't override it, so first make it protected or public,
  or move its annotation to the subclass methods.
- If two interfaces of a JDK proxy declare a method with the same name and parameters and their annotations differ,
  keep the annotation on the method of the first interface the proxy lists, whose handler ran in 0.34.0, and remove
  it from the other. Or give both the same attributes.

The code compiles unchanged. An inherited handler that starts running writes what a new handler writes, such as its
checkpoint and, for a `@Projection`, `@Snapshot` or `@Saga`, its state. There is no OpenRewrite recipe, because nothing in the code
says whether an inherited handler was meant to run. See
[ADR 172](../architecture/decisions/0172-how-the-spring-boot-starters-find-and-start-annotated-handlers.md).

## A write through the application service is refused while the application starts

The Spring Boot starters now refuse a write through the application service until their startup scan has registered
every handler. The scan runs once all singleton beans are created, and ends before any `ApplicationRunner` runs.
[ADR 172](../architecture/decisions/0172-how-the-spring-boot-starters-find-and-start-annotated-handlers.md#3-a-write-through-the-application-service-waits-for-the-synchronous-handlers)
has the reasons.

### Who is affected

The changes reach an application that uses a Spring Boot starter with `occurrent.subscription.mode` set to anything
but `DISABLED`, the default `AUTO` included, when one of these holds:

- A bean calls `execute` on the `ApplicationService` or `DcbApplicationService` that the starter creates while the
  application starts, from a `@PostConstruct` method, from `afterPropertiesSet`, from a `@Bean` method, or from a
  thread it starts then.
- A test starts a context whose event store bean is a Mockito mock of `SpringMongoEventStore` or
  `ReactorMongoEventStore`.
- The application sets `occurrent.subscription.mode=MANUAL` and has a `@SynchronousSubscription`, or a `@Projection`
  or `@Snapshot` with `mode = SYNCHRONOUS`. All three run on the synchronous subscription model.
- The application has a `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@Projection` or `@Snapshot`, or a
  `@Saga` on the blocking starter, that starts live, at `NOW` or at `DEFAULT` with nothing stored, and something
  writes directly to the event store while the application starts. `@Projection` and `@Saga` start at `DEFAULT`
  unless they set `startAt`.
- The application declares its own `CheckpointStorage` that refuses `CheckpointWriteCondition.ifAbsent()`, on the
  reactor starter, or on the blocking starter with `occurrent.subscription.competing-consumer.fence-checkpoints=false`.
- The application stops the synchronous subscription model, or pauses a synchronous subscription, and keeps writing
  through the application service.

The last one reaches an application without a starter too, when it passes a `SynchronousSubscriptionModel` to
`synchronousSubscriptions(..)` on the builder of `GenericApplicationService` or `GenericDcbApplicationService`, on
either stack.

### What changes

A `@SynchronousSubscription` runs on the thread that writes through the application service, and nothing replays a
write to it, so a write made before it registered reaches it only if your code passes that write's events to the
`SynchronousSubscriptionModel` while the handler is running, as the javadoc of `SynchronousSubscriptionModel`
describes. For such a write the blocking application service
now throws `WriteRefusedException` from `execute`, and the reactor one returns a `Mono` that fails with it when it is
subscribed. Nothing is stored. A `@PostConstruct` method that writes fails the creation of its bean, so the
application doesn't start, and the message says where to write instead:

```
The application is still starting and has not registered all of its handlers, so a synchronous handler it registers later would never see this write. Write from an ApplicationRunner or an ApplicationReadyEvent listener, not from a @PostConstruct method, an InitializingBean or a @Bean method.
```

On the reactor stack the refusal is an error signal on that `Mono`. A `@PostConstruct` method that blocks on it fails
the same way, but one that calls `subscribe()` without an error consumer stores nothing, and the application starts
with only Reactor's log line about a dropped error. The `Mono` asks whether to refuse each time it is subscribed, so a
`Mono` built while the application starts and subscribed once it runs is not refused.

An asynchronous handler, a `@Saga` and a push projection included, now starts once the startup scan has registered
every handler, in the order the scan registered them, and each `WAIT_UNTIL_STARTED` replay ends before the next handler
starts. In 0.34.0 each one started as the scan registered it. So a synchronous handler gets every write that a saga or a
startup replay makes, and a `WAIT_UNTIL_STARTED` replay still ends before any `ApplicationRunner` runs.

A write from a `SmartInitializingSingleton` may or may not be refused, depending on whether Spring calls it before or
after the starter's own `SmartInitializingSingleton`, which follows the order the beans were registered in. Write
from an `ApplicationRunner` there too.

The starter refuses the write also when the application declares no synchronous handler. A write made directly to
the event store, with `EventStore.write` or `DcbEventStore.append`, is not refused. A synchronous handler sees such a
write, whenever it is made, only if the application passes its events to the `SynchronousSubscriptionModel` while that
handler is running, with `dispatch` on either stack or `accept` on the blocking one. The javadoc of
`SynchronousSubscriptionModel` says when a handler is running.

A `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@Projection` or `@Snapshot`, or a `@Saga` on the
blocking starter, that starts live can now get that direct write. On its first start at `NOW`, or at `DEFAULT` with
nothing stored, it starts from where the event store was when the event store bean was created, except in the cases
listed further down. In 0.34.0 it started
from where the event store was once all singleton beans existed, and missed what was written before, as
[section 11 of the 0.34.0 upgrade guide](upgrading-to-0.34.0.md#11-a-subscription-handler-spring-cannot-invoke-is-now-refused-and-a-live-subscription-can-miss-a-startup-write)
describes. A `@Projection` and a `@Saga` start at `DEFAULT` unless they set `startAt`, so a default `@Saga` now acts
on, and sends commands for, an event written directly to the event store while the application starts, where 0.34.0
skipped it. A handler on a bean built after startup, such as a `@Lazy` bean, starts from where the event store is
when that bean is built, so it gets what the bean's own `@PostConstruct` method writes. For a `FactoryBean` built
after startup that is where the event store was when the factory was built, so its product's handler also gets what
the factory writes while it builds the product. The product's handler gets events written before the product was built
in three more cases:

- The factory is a singleton or scoped one created in an earlier call than the product, for example with
  `getBean("&name")` or by injecting it, or its `isSingleton()` returns false so each lookup builds a new product. The
  handler also gets what was written between the factory's creation and the product's.
- The factory is a prototype, and an older factory of the same name that the same thread created is still there,
  unclaimed by a product and not garbage collected, for example the factory whose `getObject` asked for this product.
  The handler starts from where the event store was when that older factory was built.
- The factory was built while the application started and its product only later, also a prototype factory whose
  `getObject` ran after startup. The handler starts from where the event store was when the event store bean was
  created, so it also gets what was written while the application started and while the factory built the product.

Except where a `SubscriptionResumePoints` bean answers for the subscription, a handler starts where the event store
is when it subscribes, which is once the startup scan is done, in these cases:

- Its bean is built on a Reactor non-blocking thread.
- The event store bean is a Spring AOP proxy without a single target. The starter logs a warning naming the bean.
  An event store behind a proxy with a single target is read through that target.
- The application uses neither MongoDB starter. Only those two turn where the event store is into a checkpoint, with
  the internal `LiveStartCheckpoints` bean they declare.
- It would store that position as its first checkpoint, which every live start but `NOW` with
  `resumeBehavior = SAME_AS_START_AT` does, and Spring can't pick one `CheckpointStorage` bean, as it does with a
  single one or a `@Primary` one among several.

Such a handler misses an event written directly to the event store while the scan runs, for example from the
`@PostConstruct` method of a bean the scan creates. In 0.34.0 it subscribed as the scan registered it, so it got that
event when the bean was created after it had subscribed.

The starter writes that first checkpoint with `CheckpointWriteCondition.ifAbsent()`. A `CheckpointStorage` of your own
that answers `false` from `evaluatesWriteConditionsFor` fails startup instead, unless
`occurrent.subscription.start-when-first-position-can-be-overwritten` is set, see
[A first position a checkpoint storage can only write unconditionally is refused](#a-first-position-a-checkpoint-storage-can-only-write-unconditionally-is-refused).

That position is a time in MongoDB's oplog, so it lasts only as long as the oplog window. An application that takes
longer than the oplog window to start, from the event store bean to the handler's subscribe, finds it gone. A handler
the starter runs through a catch-up subscription, which both starters do by default, then refuses to start instead of
skipping the events since that position. The blocking starter throws `CatchupCannotResumeException` and the
application doesn't start. With `startupMode = BACKGROUND`, and on the reactor starter, the handler is paused with an
error in the log that names it. With every instance of the application stopped, delete its stored checkpoint, or
replace it with a checkpoint the application kept that the subscription model still has the history back to, and start
every instance again with the same start, as
[Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share) describes.
A handler at `NOW` with `resumeBehavior = SAME_AS_START_AT` stores no checkpoint, so it is paused with an
error in the log, and the next start of the application reads a new position. Without a catch-up subscription, the
MongoDB subscription model restarts the handler from now when
`occurrent.subscription.mongodb.restart-on-change-stream-history-lost` is `true`, its default, and the events written
between that position and the restart are never delivered. With it set to `false` the subscription ends with an error
in the log.

The starter reads where the event store is from the event store bean as soon as that bean is created. A Mockito mock
returns `null` from `openCatchupReadScope`, so a context with one fails to start with an `IllegalStateException` that
names `openCatchupReadScope`.

Under `occurrent.subscription.mode=MANUAL` the synchronous subscription model, which runs `@SynchronousSubscription`
and a `@Projection` or `@Snapshot` with `mode = SYNCHRONOUS`, runs from the start. In 0.34.0 the starter kept it
stopped until you started the subscriptions, and a write made before that reached no synchronous handler. Now every
write through the application service runs the synchronous handlers.

The application service also throws `WriteRefusedException` for a write while the synchronous subscription model is
stopped and has a handler registered, and for a write whose events match a paused synchronous subscription. That
holds with or without a starter, as long as a dispatcher you implement yourself passes `beforeWrite()` on, see
[A synchronous event dispatcher you implement breaks on `beforeWrite()`](#a-synchronous-event-dispatcher-you-implement-breaks-on-beforewrite).
In 0.34.0 that write was stored and the stopped or paused handler never got its
events. The match uses the stream id and the stream version that the write gives the events, so a filter on either
one works. A filter on the position or on the append id can't match before the event store writes the events, so a
paused subscription with such a filter doesn't stop the write.

A `pauseSubscription(id)` or `stop()` on the synchronous subscription model waits for the writes the model has
already let through, so their handlers still get their events, and `pauseSubscription(id)` returns once the
subscription is paused. A write that starts while the pause waits is refused when the pausing subscription matches
it, and a write that starts while the stop waits is refused. A handler that pauses or stops its own model doesn't
wait, since its own write is one of those, and the pause or stop applies once the last of those writes ends. Until then
`isPaused(id)` answers `false`. On the reactor stack a pause or stop on a non-blocking thread doesn't wait either and
applies the same way. So does one from any thread while the model routes the events of a write, since a reactor
handler runs on whatever thread the event store calls back on. A long write holds a pause or stop back for as long as
it runs.

### What to do

Move a write out of a `@PostConstruct` method, an `InitializingBean` or a `@Bean` method into an `ApplicationRunner`
or an `ApplicationReadyEvent` listener. Before:

```java
@Component
class OpenDefaultAccount {
    private final ApplicationService<DomainEvent> applicationService;

    OpenDefaultAccount(ApplicationService<DomainEvent> applicationService) {
        this.applicationService = applicationService;
    }

    @PostConstruct
    void open() {
        applicationService.execute("account-1", events -> Account.open(events, "account-1"));
    }
}
```

After:

```java
@Component
class OpenDefaultAccount implements ApplicationRunner {
    private final ApplicationService<DomainEvent> applicationService;

    OpenDefaultAccount(ApplicationService<DomainEvent> applicationService) {
        this.applicationService = applicationService;
    }

    @Override
    public void run(ApplicationArguments args) {
        applicationService.execute("account-1", events -> Account.open(events, "account-1"));
    }
}
```

With an `ApplicationReadyEvent` listener, annotate the method with `@EventListener(ApplicationReadyEvent.class)` in
place of `@PostConstruct`. A thread that writes goes the same way, so start it from the `ApplicationRunner`.

In a test with a mocked event store, run the test over `InMemoryEventStore` or a MongoDB event store instead, as
[A catch-up needs the event store itself](#a-catch-up-needs-the-event-store-itself) recommends. To keep the mock, make
`openCatchupReadScope` return a scope. A mocked scope reports no live start, and the subscriptions then start as they
did in 0.34.0:

```java
// Blocking, org.occurrent.eventstore.api.blocking.internal.CatchupReadScope
when(eventStore.openCatchupReadScope(any())).thenReturn(mock(CatchupReadScope.class));
// Reactor, org.occurrent.eventstore.api.reactor.internal.CatchupReadScope
CatchupReadScope scope = mock(CatchupReadScope.class);
when(scope.close()).thenReturn(Mono.empty());
when(eventStore.openCatchupReadScope(any())).thenReturn(Mono.just(scope));
```

A test that stops the synchronous subscription model and keeps writing through the application service now gets
`WriteRefusedException`. Put `@EnableOccurrentTesting` on the test class and register the
`OccurrentSubscriptionsExtension` bean it declares in an `@Autowired @RegisterExtension` field. Building that extension
lets those writes through, and a stopped or paused synchronous handler doesn't get their events. The annotation alone
doesn't, because nothing builds the extension until a test autowires it. Once built, the model lets those writes
through for as long as Spring keeps the test context cached, so every test class that shares the context gets the same
behaviour.

In the application, resume a paused synchronous subscription before writing events it matches, or cancel it.

## A synchronous event dispatcher you implement breaks on `beforeWrite()`

`SynchronousEventDispatcher` in `org.occurrent.application.service.blocking` and `ReactiveSynchronousEventDispatcher`
in `org.occurrent.application.service.reactor` gain an abstract method:

```java
WriteCheck beforeWrite();
```

A class that implements either interface doesn't compile until it implements the method. A class compiled against
0.34.0 and not compiled again throws `AbstractMethodError` when the application service calls the method. The
application service calls it once per write, before it reads anything. `SynchronousSubscriptionModel` answers with the
refusal and the wait that
[A write through the application service is refused while the application starts](#a-write-through-the-application-service-is-refused-while-the-application-starts)
describes.

### What to do

A dispatcher that passes the written events on to a `SynchronousSubscriptionModel`, or to another dispatcher, passes
`beforeWrite()` on as well:

```java
@Override
public WriteCheck beforeWrite() {
    return synchronousSubscriptionModel.beforeWrite();
}
```

`WriteCheck.NONE` is right only for a dispatcher that never wraps a model that can be stopped or paused. There's no
default implementation, because a dispatcher that wraps the model and doesn't pass the call on lets a write through
while the model is stopped or a matching subscription is paused, and that handler never gets its events. For the same
reason the rewrite recipe doesn't add the method, since a generated `return WriteCheck.NONE` would bring that loss
back.

## A synchronous handler the startup scan can't see fails the creation of its bean

The Spring Boot starters register every handler in a scan while the application starts. A bean whose synchronous
handler the scan couldn't see, and that is created after it, now fails its creation.
[ADR 172](../architecture/decisions/0172-how-the-spring-boot-starters-find-and-start-annotated-handlers.md#3-a-write-through-the-application-service-waits-for-the-synchronous-handlers)
has the reasons.

### Who is affected

An application that uses a Spring Boot starter and has a bean with a `@SynchronousSubscription`, or a `@Projection` or
`@Snapshot` with `mode = SYNCHRONOUS`, when both of these hold:

- The bean's declared type hides the annotation. That is a `@Bean` method that returns an interface or `Object`, or a
  `FactoryBean` whose declared product type isn't the annotated class.
- The bean is created after the scan, since it is `@Lazy`, the application sets
  `spring.main.lazy-initialization=true`, or it is a `FactoryBean` product nothing asked for during the scan. That
  includes a bean that an asynchronous handler's startup replay creates, since the asynchronous handlers start once the
  scan is done.

### What changes

In 0.34.0 the handler registered when the bean was created, and never got the writes made before that. Now creating
the bean throws an `IllegalStateException` before its init methods run, so the `getBean` call or the injection that
asked for it fails. When another post-processor replaces the bean with an object that declares such a handler, the
creation fails once the bean is initialized instead. The message names the bean and the handler, for example:

```
Bean 'accountReadModel' declares the synchronous handler com.example.AccountReadModel#on and is created after the startup scan of the Spring Boot starter registered the synchronous handlers. A synchronous handler has no replay and gets only the writes made after it registers, so it would miss the earlier ones. Let the startup scan register it: remove @Lazy from the bean, or declare its @Bean method with the annotated class as the return type rather than an interface or Object. Or make the handler asynchronous, with @Subscription instead of @SynchronousSubscription, or mode = ASYNC on @Projection and @Snapshot.
```

A prototype bean, or the target of a scoped proxy, whose handler registered at startup is created as before.

An object that implements `FactoryBean` and declares a synchronous handler that isn't registered yet now fails its
creation with a `BeanCreationException` wherever the starter handles it as a factory. That is every `FactoryBean`
bean. It is also the product of a `FactoryBean` when that product implements `FactoryBean` too and its factory is
registered with `registerSingleton`, is an inner bean, or asks for the product while it initializes itself. An object
the starter handles as a factory also fails its creation when the starter can't read its handler methods. In 0.34.0
the starter didn't register a synchronous handler of such an object when the object was created. Move the handler to
a class that doesn't implement `FactoryBean`.

An object Spring initializes under a name with no bean definition fails its creation the same way when it declares a
synchronous handler that isn't registered. That is the product of a `FactoryBean` registered with `registerSingleton`,
and an object passed to `initializeBean` of `AutowireCapableBeanFactory`. The startup scan reads only names with a bean
definition, and in 0.34.0 the starter didn't register such a handler when the object was created either. Declare the
bean, or the `FactoryBean` that creates it, with a bean definition, for example with a `@Bean` method. An object
initialized under its class name with `.ORIGINAL` appended isn't checked. That is the name Spring's test support gives
a test instance, so a test class that declares a synchronous handler starts as it did in 0.34.0.

### What to do

Do one of these:

- Declare the annotated class as the `@Bean` method's return type. The startup scan then sees the handler and creates
  the bean to register it, `@Lazy` or not.
- Remove `@Lazy`, so the bean is created with the other singletons, before the startup scan.
- Make the handler asynchronous, with `@Subscription` in place of `@SynchronousSubscription`, or `mode = ASYNC` on
  `@Projection` and `@Snapshot`. With `startAt = BEGINNING` an asynchronous handler reads the events stored before its
  bean was created from the event store.

## Five types the Spring Boot starters share move to an internal package

Both Spring Boot starters read a bean's handlers through `SubscriptionAnnotations`, in the module they share. It moves
from `org.occurrent.springboot.common` to `org.occurrent.springboot.common.internal`, and so do four other types the
starters share, `PushCatchupStatusImpl`, `AsynchronousSubscribables`, `AppliedAppendRecordingRegistry` and
`PolledCatchupSignals`. That package isn't part of the public API, so these types can change in any release.
`PushCatchupStatus`, the interface an application injects to read where each push projection and saga is in its
catch-up, stays in `org.occurrent.springboot.common`. Three methods of `SubscriptionAnnotations` also change so that the
starters find inherited handlers, see [A handler method the bean inherits is registered](#a-handler-method-the-bean-inherits-is-registered).

### Who is affected

Code outside the starters that refers to one of the five types, or to a nested type of `SubscriptionAnnotations`, stops
compiling. Code that calls `invokeDescriptorFactory`, `runsImplementationOf` or `resolveTypeFilter` also gets the
changes below.

### What changes

The four other types move unchanged. Three methods of `SubscriptionAnnotations` change.

- `invokeDescriptorFactory` runs a factory method that the class of the unwrapped bean inherits unchanged, where
  0.34.0 refused every factory method that class didn't declare itself. Behind a CGLIB proxy whose target can't be
  unwrapped, it refuses an inherited factory method the proxy can't override, such as a private or final one. Behind
  a JDK interface proxy whose target can't be unwrapped, it runs the factory through the proxy by the method of the
  proxy's interface that runs it, where 0.34.0 ran only a method the proxy class itself declares.
- `runsImplementationOf` decides whether the bean's class overrides the method by the JVM's rules. Say `Base` in
  package `a` declares a package-private `on(Placed)`, `Mid` in package `b` extends it and declares `on(Placed)` too,
  and `Leaf` in package `a` extends `Mid`. `Mid.on` doesn't override `Base.on`, so a `Leaf` runs `Base.on`, and the
  method now answers `true` for `Base.on` on a `Leaf`, where 0.34.0 answered `false`.
- `resolveTypeFilter` reads the handler's parameter types as the bean's class sees them, so `on(E event)` on `Base<E>`
  subscribes a bean of a class that extends `Base<Placed>` to `Placed`, where 0.34.0 read the bound of `E`. Handed a
  bridge method, it reads the types of the method the bridge calls. `analyzeParameters` still reads the types as the
  method declares them.

### What to do

`UpgradeToOccurrent_0_35` rewrites the imports and fully qualified names to the new package. A later release can change
these types with no recipe and no section in an upgrade guide, so code that keeps calling them can stop compiling or
behave differently on any upgrade. Code that injects `PushCatchupStatusImpl` to read catch-up progress can inject
`PushCatchupStatus` instead. Where you call one of the three methods, check what your code does with the new
answer for an inherited factory method, a package-private method declared again in another package, or a handler
declared with a type variable.

## `ComposedDefaultStartPosition` and its bean are removed

The blocking Spring Boot starter no longer has `ComposedDefaultStartPosition` or the `occurrentComposedDefaultStartPosition`
bean. The bean recorded that a subscription with no start position never replays on the subscription model the starter
builds. The `@Projection(recordAppliedAppends = true)` registrar read it to warn at startup that such a projection's
recordings never reset. Such a subscription now replays when a catch-up stored its checkpoint, see
[A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint),
so the fact the bean reported is no longer true, and nothing in Occurrent reads it.

### Who is affected

Code that injects the bean or names `ComposedDefaultStartPosition`, and code that calls or overrides
`OccurrentMongoAutoConfiguration.occurrentCompetingDurableSubscriptionModel(..)`, which loses its last parameter. That
code stops compiling.

### What to do

Remove the reference. Nothing replaces it. Code that asked `isDefaultKnownLiveOnlyFor(..)` whether a projection never
replays should treat a projection with no `startAt` as one that can replay.

## A blocking cancel no longer promises a rebuild

This applies to the blocking `CatchupThenPushSubscriptionModel` when you give it a `CheckpointStorage` for the catch-up
marker, and to every `@Projection` or `@Saga` with `source = PUSH` in the blocking Spring Boot starter, unless it sets
`catchup = NONE`. The marker keeps its 0.34.0 meaning on the blocking stack. A stored marker means the catch-up
finished, and a start that finds it skips its replay.

### What changes

In 0.34.0 the javadoc said that `cancelSubscription(id)` deletes the marker, so subscribing the same id again replays
the history. The cancel still deletes the marker before anything else, and the code is the same as in 0.34.0, but that
promise is gone. A delete that a Redis failover loses, or that a MongoDB member with the in-memory storage engine rolls
back, leaves the marker stored. So does a marker write the store applies after the delete. The next subscribe of the id
then skips its replay. 0.34.0 had the same gap.

### What to do

* Subscribe under a new subscription id when a projection has to be rebuilt, or when its filter or its projection
  changes. The marker is stored under the id alone, so under the old id a stored marker skips the replay and the
  changed projection never sees the history. Under a new id the subscription starts from the beginning of the history
  and repeats everything its handler does, the records it writes, the commands it dispatches and the messages it
  sends. Stop every instance on the old id before the first one on the new id starts. Stopping them doesn't prevent
  the repeats. With more than one instance, what else a new id needs depends on where each instance keeps what it
  builds and which broker queue or consumer group it consumes, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes.
* Rebuild a push `@Saga` under a new id too. With `catchup = NONE` a new id replays nothing. Otherwise the saga under
  the new id replays the whole history and can repeat everything it did, the state it writes and the commands it
  dispatches. The default MongoDB store keeps the instances of each saga id in a `saga-<id>` collection, so under a
  new id every instance is recreated from its start event and dispatches its commands again. Stop every instance on
  the old id before the first one on the new id starts. Stopping them doesn't prevent the repeats. With more than one
  instance, what else a new id needs depends on where each instance keeps what it builds and which broker queue or
  consumer group it consumes, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes.
* Don't give a durable subscription and a blocking catch-up-then-push model, `CatchupProjectionFeed` or
  `DomainEventFeed` the same subscription id in the same `CheckpointStorage`. Any checkpoint stored under the id counts
  as the marker, so a checkpoint the durable subscription stored makes the blocking catch-up skip its replay.

The code compiles unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.
[ADR 179](../architecture/decisions/0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md)
has the reasoning.

## A blocking cancel doesn't wait for a subscribe whose `StartAt.dynamic` function runs

In 0.34.0 the blocking `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel`, `CatchupSubscriptionModel` and
`DurableSubscriptionModel` asked a `StartAt.dynamic(..)` function while they held the lock for the subscription id, and
`cancelSubscription` for that id waited for the lock. When the function waited for a cancel of its own id on another
thread, that cancel threw `IllegalStateException` after 10 seconds on the catch-up models, and on
`DurableSubscriptionModel` it waited for as long as the function did. These models now ask the function for their own
model with no lock for the id held.

### Who is affected

Code that subscribes through one of these models with a `StartAt.dynamic(..)` function and cancels the same id from
another thread while the function can still be running. Code that subscribes the same id from two threads at once with
such a function is affected too, and so is code that resumes the id on `DurableSubscriptionModel` from another thread
while the function runs.

### What changes

* A `cancelSubscription(id)` made on another thread while the function of a subscribe of `id` runs returns without
  waiting for that subscribe. The subscribe returns once the function has, subscribes nothing, and the `Subscription` it
  returns answers `false` from `waitUntilStarted(..)`. A cancel the function makes on the subscribing thread doesn't
  stop the subscribe, as in 0.34.0, and neither does a cancel that throws.
* Two subscribes of the same id made at once on `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` or
  `CatchupSubscriptionModel` can both ask their function before either has registered. The one that is refused throws
  `DuplicateSubscriptionIdException` once its function has returned. Until then a `pauseSubscription(id)` or
  `resumeSubscription(id)` on another thread waits for it for up to 10 seconds, and then throws
  `IllegalStateException`. A subscribe of an id these models already run throws before it asks the function, as in 0.34.0.
  A `CatchupSubscriptionModel` with both catch-ups still asks the function first to pick the catch-up model, as in
  0.34.0.
* A subscribe on one of these catch-up models waits for the lock for the id before it asks the function, as in 0.34.0,
  and throws `IllegalStateException` when another call for the id holds that lock for longer than 10 seconds. It waits
  for that lock again once the function has returned, and can throw the same way then.
* `DurableSubscriptionModel` asks the function before the model it wraps throws `DuplicateSubscriptionIdException`, as
  in 0.34.0. A `cancelSubscription(id)` made on another thread while that function runs now cancels the subscription
  that runs, and the duplicate then returns a `Subscription` that never starts instead of throwing. A
  `resumeSubscription(id)` of the subscription that runs waits for that function, as the next bullet says.
* A `resumeSubscription(id)` or the new `resumeSubscription(id, startAt)` on `DurableSubscriptionModel` made on another
  thread while the function of a subscribe of `id` runs still waits for that subscribe, but throws
  `IllegalStateException` when the function doesn't return within 10 seconds. In 0.34.0 `resumeSubscription(id)` waited
  for as long as the function did. Its `pauseSubscription(id)` and `stop()` don't wait for the subscribe, as in 0.34.0.

### What to do

* Don't read a returned `cancelSubscription(id)` as a sign that a subscribe of `id` on another thread has returned. Call
  `waitUntilStarted(..)` on the `Subscription` that subscribe returns when you need to know whether it started.
* Keep effects that must happen only for a subscribe that goes ahead out of the function, since a subscribe that then
  throws `DuplicateSubscriptionIdException` or `IllegalStateException` asks it too.
* Don't wait in the function for a `pauseSubscription` or `resumeSubscription` of the same id on a catch-up model, or
  for a `resumeSubscription` on `DurableSubscriptionModel`, on another thread. That call throws `IllegalStateException`
  when the function doesn't return within 10 seconds.
* Wait for the cancel only the first time a subscribe asks the function. A catch-up model asks it again while it holds
  the lock for the id on a resume, on its replay thread during the handover to live delivery, and for the model it
  wraps when that model is another wrapper such as `DurableSubscriptionModel` and the subscribe goes straight to live
  delivery. A cancel made during one of those asks throws `IllegalStateException` after 10 seconds, or during the
  handover waits for as long as the function does.
* A `CatchupSubscriptionModel` with both a stream and a DCB catch-up asks the function first to choose between them for
  a subscribe with no filter, or with a filter that is not a stream, DCB or agnostic one. A cancel made during that ask
  returns, and the subscribe still goes ahead.

The code compiles unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.

## A dynamic `StartAt.now()` over `ReactorDurableSubscriptionModel` fails when the wrapped model answers nothing

`ReactorDurableSubscriptionModel` starts a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()`
from where the feed was when `subscribe(..)` was called, as `globalCheckpointAsOfNow()` of the wrapped model answers it.
When the read answers nothing, the subscription doesn't start. `waitUntilStarted()` of the subscription `subscribe(..)`
returned fails with an `IllegalStateException`, and an `ERROR` is logged. In 0.34.0 the subscription started from
wherever the feed had reached when the wrapped model got it, and skipped what was written before that.

A read that fails is read again, with a `WARN` for each failed read, and the subscription waits without starting until a
read answers or a cancel of the id or a `shutdown()` ends the wait. When the read has failed before the hand-over to
the wrapped model waits for it, the hand-over reads again at once. After each failed read that the hand-over waited
for, it waits before it reads again. The first wait is between 100 and 150 milliseconds, and the waits about double
from there, up to 5 seconds, which the seventh wait is the first that can reach. When `globalCheckpointAsOfNow()`
throws instead of returning a `Mono`, nothing is read, and `waitUntilStarted()` fails with what it threw.

### Who is affected

A `ReactorDurableSubscriptionModel` over a model that manages named subscriptions and whose `globalCheckpointAsOfNow()`
can answer empty, with a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()`. That includes a
subscribe that waits for a checkpoint that a cancel of the id deleted to be written back.

A wrapped model of your own that breaks the contract of `CheckpointAwareSubscriptionModel.globalCheckpointAsOfNow()`
is affected too:

* When it throws a `RuntimeException` instead of returning a `Mono`, `waitUntilStarted()` now fails with what it threw.
* When the `Mono` it returns fails on every read, the subscription never starts, and a `WARN` is logged for each
  failed read.

In 0.34.0 the subscription started in both cases, unless the subscribe waited for such a write back.

`ReactorMongoSubscriptionModel` answers empty when MongoDB's reply to `hello` has no `localTime`, so such a subscription
doesn't start over it. It didn't start in 0.34.0 either. The model opened its change stream at the present with the same
request, and stopped the subscription with an `IllegalStateException` that says the reply has no `localTime`. A
subscribe that waited for such a write back read the position again after each failure instead, and neither started nor
failed. With `startWhenNoStartPositionCanBeRecorded(true)` the subscription is handed to the model as `StartAt.now()`.
It stops with that `IllegalStateException` when the model's own request gets a reply without `localTime` too, and
otherwise opens where the feed is then and skips what was written since `subscribe(..)`, as that setting accepts. When
MongoDB can't be reached, the read fails and is read again until MongoDB answers, and the same request to MongoDB failed
the subscription's opening at the present in 0.34.0 too. The model doesn't throw from `globalCheckpointAsOfNow()`. The
Spring Boot starter's `ReactorDurableSubscriptionModel` asks `ReactorMongoSubscriptionModel`, directly or through a
`ReactorCatchupSubscriptionModel`, so the same holds there.

### What to do

To keep the 0.34.0 behaviour, and accept that the subscription skips what was written between `subscribe(..)` and the
moment the wrapped model gets it, set `startWhenNoStartPositionCanBeRecorded(true)` on the config you pass:

```java
new ReactorDurableSubscriptionModel(wrappedModel, checkpointStorage,
        new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
```

The same setting lets a subscription from the subscription-model default start when no checkpoint is stored and the
read answers nothing.

Otherwise, let the subscribe fail and subscribe the id again once the wrapped model can answer. The id is free once
`waitUntilStarted()` has failed, so you don't need to call `cancelSubscription(..)` first.

The setting doesn't start a subscription over a model that throws, or whose `Mono` fails on every read. Make such a
model return a `Mono` instead of throwing. When no read again can give the position, complete the `Mono` empty. The
contract has that answer for a problem the model can't resolve, and over it the setting starts the subscription as
0.34.0 did. Fail the `Mono` only for a problem that a read again can resolve. For a subscription from a
`StartAt.dynamic(..)` function that answers `StartAt.now()`, `ReactorDurableSubscriptionModel` reads again after each
failure. For one from the subscription-model default, or from a function that answers
`StartAt.subscriptionModelDefault()`, it doesn't, so one failure fails its start when no checkpoint is stored.

### Why there is no recipe

The change is in what happens at runtime, and the code compiles unchanged. A recipe that turned on
`startWhenNoStartPositionCanBeRecorded` would accept the skipped events for you, for the subscription-model default
too, on every subscription.

## A dynamic `StartAt.now()` that the wrapped model refuses fails `waitUntilStarted()` instead of `subscribe(..)`

`ReactorDurableSubscriptionModel` hands a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()` to
a wrapped model that manages named subscriptions once it has read where the feed was at the call, which is after
`subscribe(..)` has returned. So when the wrapped model refuses the subscribe, for a filter it doesn't support for
example, `subscribe(..)` no longer throws. `waitUntilStarted()` of the subscription it returned fails with what the
wrapped model threw, and an `ERROR` is logged. A subscribe of an id that the wrapped model already reports still throws
`DuplicateSubscriptionIdException` at the call.

### Who is affected

Code that catches the wrapped model's refusal around `subscribe(..)` for such a subscription. A subscribe that waits
for a checkpoint that a cancel of the id deleted to be written back was handed over after the call in 0.34.0 already.

### What to do

Handle the refusal where you wait for `waitUntilStarted()`, as 0.34.0 already required for a subscription from the
subscription-model default. Keep handling `DuplicateSubscriptionIdException` at the call too, since it can come from
either place.

## A resumed MongoDB subscription waits for the action that ran when it was paused

`pauseSubscription(id)` and `stop()` on `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` wait at most a
second for a running action. In 0.34.0 a `resumeSubscription(id)` or a `start(true)` after that could call the action
for the next events while the call from before the pause still ran, so two calls for one subscription ran at once and a
later event could be handled before an earlier one. A resumed subscription now opens its change stream and then waits,
with no deadline, until that call returns, before it calls the action again.

### Who is affected

Code that pauses or stops one of these models while an action can run for longer than a second, and then resumes it.
An action that pauses and resumes its own subscription is affected however short it is, since a pause from inside the
action returns at once. The resumed subscription calls the action again only once that call returns, so an action that
then waits until its subscription delivers never returns. In 0.34.0 it got the event. A
`CompetingConsumerSubscriptionModel` over one of them pauses and resumes its subscriptions as its lease moves, so it is
affected too.

### What changes

* While the earlier call runs, the resumed subscription calls the action for no event. An action that never returns
  holds the resumed subscription back for as long as it runs, where 0.34.0 delivered the events after it.
* `start(true)` returns, and `waitUntilStarted(..)` answers `true`, once the change stream has opened, without waiting
  for that call. `resumeSubscription(id)` returns without waiting for either.
* When MongoDB reports as the resumed change stream opens that the history is lost, the subscription waits for that
  call too, before it does anything about the loss. It counts as started once it waits, so `start(true)` and
  `waitUntilStarted(..)` don't wait for that call there either.
* A warning in the log names the subscription when it starts to wait, and another says how long it waited once the call
  returns.
* Under a `CompetingConsumerSubscriptionModel` the node whose subscription waits keeps the lease meanwhile, so no other
  node runs the subscription until the call returns or the subscription is paused on that node.
* The pause still returns after at most a second.

### What to do

Give an action that can block for good a timeout of its own, since a resume no longer gets past it. The code compiles
unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.

## A MongoDB subscription at the present is refused when the server gives it no position

`NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` record a position for a subscription started at the
present before its change stream opens. A replica set and a sharded cluster send MongoDB's operation time with every
reply, and the models record that. On a server whose reply to `ping` has no operation time, the model logs a `WARN`,
reads the change stream once, and records the commit time of the event that read returns, or the resume token of an
empty read. A pause and resume, or a restart, before the first event then opens at that position, where 0.34.0 opened
at a later present and skipped what was written in between.

When that read returns neither an event nor a resume token, the subscription can't start at the present:

* `subscribe(..)` returns without waiting for MongoDB, as in 0.34.0.
* `waitUntilStarted(..)` on the handle it returned throws an `UnsupportedStartAtException`, and so does `start(true)`
  when it starts the subscription. The message says MongoDB gave neither an operation time nor a change stream
  position. The subscription is removed from the model, and the listeners added through
  `StartRefusalReportingSubscriptions` are told, as for a catch-up's checkpoint.
* A `resumeSubscription(id, StartAt.now())` does the read when the resumed change stream opens. When it gets neither,
  the subscription is held paused and stays in the model, `waitUntilStarted(..)` on every handle returned up to the held
  run throws the `UnsupportedStartAtException` until a resume starts it, and the listeners added through
  `StartRefusalReportingSubscriptions` are told through `handleHeldPaused(..)`, not `handleStartRefusal(..)`.
  `resumeSubscription(id)` reads the change stream again. A `StartAt.dynamic(..)` resume that resolves to the present as
  its change stream opens is held the same way.
* With `restartSubscriptionsOnChangeStreamHistoryLost` on, a restart from the present after the history was lost that
  gets neither holds the subscription paused. `isPaused(id)` answers `true`, an error in the log names it, the
  listeners are told through `handleHeldPaused(..)`, and `resumeSubscription(id)` reads the change stream again.
  `waitUntilStarted(..)` on every handle returned up to the held run, the one `subscribe(..)` returned and each one a
  resume returned, then throws the `UnsupportedStartAtException`, also when it answered earlier that the subscription
  started, until a resume starts it.
* Under a `CompetingConsumerSubscriptionModel` the node that holds a subscription paused this way holds it as if the
  user had paused it. It gives the lease back, so another node can get it, and doesn't compete for the lease again
  until `resumeSubscription(id)` is called on that node. A handle that waits for the held start throws once the lease
  is given back, or once the node finds that a resume or a cancel ended the hold first.

In 0.34.0 such a subscription opened at the present.

### Who is affected

Code that subscribes one of these models at `StartAt.now()`, at the model default, or at a `StartAt.dynamic(..)` that
resolves to one of those, on a MongoDB server that sends no operation time. A `StartAt.dynamic(..)` start does the read
at the subscribe whatever it resolves to, and when that read gives neither, it is refused if it resolves to the present
as its change stream opens. A `CompetingConsumerSubscriptionModel` over one of these models, which the blocking Spring
Boot starter always composes, forgets a subscription whose start is refused. Once every node that subscribed it has
been refused, the subscription runs on no node. A replica set and a sharded cluster aren't affected.

### What to do

Subscribe at a checkpoint instead, such as a `MongoOperationTimeCheckpoint` for the time to start from, and handle the
`UnsupportedStartAtException` where you call `waitUntilStarted(..)` or `start(true)`. The code compiles unchanged, and
there is no OpenRewrite recipe, because no signature changes.

The blocking Spring Boot starter waits for a subscription at `NOW` or `DEFAULT` to start by default, so on such a server
the exception can stop the application from starting, with no call of your own in the stack trace. Set
`startupMode = BACKGROUND` on the annotation to start the subscription in the background instead, or pass
`waitUntilStarted = false` to the Kotlin DSL's `subscribe(..)`. The refusal is then logged as an error and the
subscription is removed.

## A MongoDB subscription whose history is lost is refused when restarts are off

`NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` don't restart a subscription whose change stream
history is lost while `restartSubscriptionsOnChangeStreamHistoryLost` is off, which is the default. That hasn't
changed. What changes is what a caller waiting for the subscription to start sees:

* When MongoDB reports the loss as the change stream opens, or through a mongos with the change stream's first
  reply, `waitUntilStarted()` and `start(true)` throw an
  `UnsupportedStartAtException`, and so does `waitUntilStarted(..)` with a timeout, which answered `false` in 0.34.0.
  `startAt()` on the exception is the checkpoint whose history is gone, or the subscription's current start position
  when there is no checkpoint to name, which after a resume at a new position is that position. MongoDB's error, code 286 (`ChangeStreamHistoryLost`), is in its cause chain, and
  its message names the fixes below.
* The listeners added through `StartRefusalReportingSubscriptions` are told through `handleStartRefusal(..)`. A
  `CompetingConsumerSubscriptionModel` forgets the subscription on that node, as it does for any refused start.
* The id is free, so `subscribe(..)` with it works again.
* A loss on a later read, after the handle answered that the subscription started, tells the listeners and frees the
  id the same way. Every handle of the subscription, the one `subscribe(..)` returned and each one a resume returned,
  throws the `UnsupportedStartAtException` from its next `waitUntilStarted(..)` on, and so does a handle a
  `CompetingConsumerSubscriptionModel` returned.

In 0.34.0 the model removed the subscription and released no waiter, so a `waitUntilStarted()` with no timeout never
returned, one with a timeout answered `false`, and `start(true)` returned.

Through a mongos, whatever `restartSubscriptionsOnChangeStreamHistoryLost` is set to, a subscription that opens at a
stored position, or at the position a resumed subscription read to, such as after a pause, `stop()` and `start(true)`,
or a competing consumer's lease takeover, now counts as started only once the change stream's first reply came, where
0.34.0 counted it as started as the change stream opened. On a quiet collection a wait can then take up to the
`maxAwaitTime` configured, or the server's default when none is, so a `waitUntilStarted(..)` with a shorter timeout
answers `false`. So does a subscription at the present that opens at the position `subscribe(..)` read for it, unless
its change stream opens right after that read on the first try. One made while the model was stopped and started later,
one resumed after a pause before its first reply, and one whose first try to open failed wait, since that position can
have left the oplog by then. A start at the present read right before the change stream opens counts as started as it
opens, which covers a `subscribe(..)` at `StartAt.now()` on a started model and a resume at `StartAt.now()`. So does
every start on a replica set, unless the model can't ask MongoDB whether it reaches it through a mongos. It then waits
for the first reply, as through a mongos, and asks again at the next start.

### Who is affected

Code that waits for one of these subscriptions to start with restarts after a loss of history off. The blocking Spring
Boot starter turns restarts on by default, so this reaches it only with
`occurrent.subscription.mongodb.restart-on-change-stream-history-lost=false`. It then waits for a subscription at
`NOW` or `DEFAULT` to start by default, so an application whose stored checkpoint for one is older than the oplog's
history can fail to start with the `UnsupportedStartAtException`. Set
`startupMode = BACKGROUND` on the annotation, or pass `waitUntilStarted = false` to the Kotlin DSL's `subscribe(..)`,
to start it in the background.

Code that waits with a timeout for a subscription that reaches a sharded cluster through a mongos and opens at a
stored position, at the position a resumed subscription read to, such as after a pause, `stop()` and `start(true)`, or
a competing consumer's lease takeover, or at a present that `subscribe(..)` read earlier than right before the change
stream opened, whatever `restartSubscriptionsOnChangeStreamHistoryLost` is set to. Give the wait a timeout with room
for opening the change stream and for its `maxAwaitTime`, or the server's default when none is set.

### What to do

Pick one, as the exception's message says:

* Subscribe through a catch-up subscription, which replays from the event store and then goes live.
* Remove the subscription's checkpoint from the durable storage, so it starts again from the position it is
  subscribed at. With more than one instance, remove it with every instance stopped and start them all again with
  the same start, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes.
* Turn `restartSubscriptionsOnChangeStreamHistoryLost` on, which restarts the subscription at the present and skips
  what was written in between.

## A subscription handle throws once the model gives the subscription up

In 0.34.0 a `Subscription` handle kept answering `true` from `waitUntilStarted(..)` once its subscription had started,
also after a pause, a stop or a lost lock race. That still holds for those three. For a handle that
`NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` returned, two cases now throw instead:

* With `restartSubscriptionsOnChangeStreamHistoryLost` off, a loss of the change stream's history on a later read
  refuses the subscription for good. The next `waitUntilStarted(..)` throws the refusal, an
  `UnsupportedStartAtException`, and so does every call after it.
* With `restartSubscriptionsOnChangeStreamHistoryLost` on, a restart from the present that gets no position holds the
  subscription paused. Each `waitUntilStarted(..)` throws the reason until a resume starts the subscription again.

A cancel or a shutdown ends a hold and frees the id. A handle of a subscription that had started answers `true` again,
and one of a subscription that never started throws an `UnsupportedStartAtException` saying that the subscription was
cancelled while it was held paused and can be subscribed again with the same id.

A refusal reaches every handle of the subscription, the one `subscribe(..)` returned and each one a resume returned. A
hold reaches the handles returned up to the held run, so a handle a later resume returned answers for that later run. A
handle a `CompetingConsumerSubscriptionModel` returned over one of them asks the handle it wraps, so it throws too.
[ADR 181](../architecture/decisions/0181-a-subscription-handle-throws-once-the-model-gives-the-subscription-up.md) has
the reasons.

### Who is affected

Code that keeps a handle and asks it later, such as a readiness check that calls `waitUntilStarted(Duration.ZERO)`.
Where it got `true` in 0.34.0 for a subscription no node ran, it now gets the `UnsupportedStartAtException`.

### What to do

Catch `UnsupportedStartAtException` where you ask a kept handle, and treat it as a subscription that needs you to act,
as the exception's message says. Ask `isRunning(id)` on the model when you want to know whether the subscription runs
now, since a handle answers about its start.

## A blocking catch-up resume at a dynamic start uses the catch-up's answer

`resumeSubscription(id, startAt)` on `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` or the blocking
`CatchupSubscriptionModel` asks a `StartAt.dynamic(..)` function for the catch-up first, with or without checkpoint
storage, and the live model resumes at the start the function answered. In 0.34.0 the catch-up passed the `StartAt`
on unchanged, and the live model asked the function for itself.

A function that answers the same position for every model sees no change. A function that answers by
`SubscriptionModelContext`, with one position for the catch-up and another for the live model, now resumes the live
model at the catch-up's position. A live model that wraps another, such as `DurableSubscriptionModel`, is still asked
whether it starts at all, as at a subscribe. When the function answers `null` for the catch-up, the live model asks it
for itself, as in 0.34.0. To resume the live model at a position of its own, pass that position with
`StartAt.checkpoint(..)` instead of the dynamic start.

## A blocking catch-up refuses a resume at a position of a subscription that runs

`resumeSubscription(id, startAt)` on `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` or the blocking
`CatchupSubscriptionModel` throws `SubscriptionAlreadyRunningException` when the catch-up made the subscription and it
runs. It throws before it stores anything and before it asks a `StartAt.dynamic(..)` function. A subscription runs here
when it isn't paused, the replay its `subscribe(..)` started didn't end before the live model got it, and no replay that
`resumeSubscription(id)` started runs for it or is about to start to take over a live start the live model lost. That
includes one whose `subscribe(..)` replay still runs, and one the live model is still opening after `subscribe(..)`
returned, as the MongoDB models do. In 0.34.0 the catch-up passed such a resume on to the live model. The MongoDB
models threw `SubscriptionAlreadyRunningException` for a subscription they held, which is what the catch-up throws
now. While the `subscribe(..)` replay ran they didn't hold it yet, and threw `UnknownSubscriptionException`.

`RepositionableSubscriptions` already says that `resumeSubscription(id, startAt)` throws
`SubscriptionAlreadyRunningException` for a subscription that runs. A `catch` of that exception, of
`SubscriptionRefusedException` or of `IllegalArgumentException`, which it extends, catches it. A `catch` of
`UnknownSubscriptionException` no longer catches a resume during the `subscribe(..)` replay. A subscription you pause
while its `subscribe(..)` replay runs isn't refused this way. The live model doesn't hold it until that replay hands
over, so a resume at a position before then still throws `UnknownSubscriptionException` from a MongoDB model, as in
0.34.0.

## A catch-up marker on the reactor stack is a position to resume after

This applies to the reactor `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` and `DomainEventFeed` when you
give them a `CheckpointStorage` for the catch-up marker. It also applies to every `@Projection` with `source = PUSH` in
the reactive Spring Boot starter, unless it sets `catchup = NONE`. The starter builds the push model with the
application's `CheckpointStorage` bean, so you never pass one yourself. The blocking ones, and every `@Projection` or
`@Saga` with `source = PUSH` in the blocking starter, keep the 0.34.0 meaning. A stored marker means the catch-up
finished, and a start that finds it skips its replay. Their cancel changes too, see
[A blocking cancel no longer promises a rebuild](#a-blocking-cancel-no-longer-promises-a-rebuild).

In 0.34.0 a stored marker meant that the history had been read in full, and a catch-up that found it went live without
a replay. In 0.35.0 a reactor catch-up start that reads a stored marker replays the events after it. See "What a
reactor catch-up start replays" in
[ADR 179](../architecture/decisions/0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md#what-a-reactor-catch-up-start-replays)
for what a start reads, replays and stores, and the cases that change it. ADR 179 also says why the blocking stack
keeps the 0.34.0 marker.

What you see:

- A restart can replay events the projection handled live before it. The cache that removes duplicates is in memory,
  so the projection applies those events again. Delivery was already at least once, so a projection that applies an
  event twice with the same result as once needs no change.
- `catchUp()` on a feed with a stored marker replays the events after it, where 0.34.0 went straight to live.
- A `@Projection` with `source = PUSH` waits for its catch-up at startup unless it sets `startupMode = BACKGROUND`, and
  the marker is stored only once the replay is done. In 0.34.0 a start that found a stored marker went live at once,
  and now it replays the events after the marker first. When the replay takes longer than a startup probe, such as a
  Kubernetes one, allows, the probe can restart the application before the marker is written, and the next start can
  replay those events again. Set `startupMode = BACKGROUND` so the application starts while the catch-up runs.
- On `CatchupProjectionFeed` and `DomainEventFeed`, where a catch-up can run again after `goLive()`, a catch-up can
  leave the stored marker as it was while an event that the feed's own handler or callbacks fed back waits to be
  handled. A handler that feeds back an event whenever it handles one, under steady live traffic, can then keep the
  marker from moving on, so later starts replay more. `CatchupThenPushSubscriptionModel` runs each catch-up before its
  subscription goes live, so it doesn't meet that cost.
- When a reactor feed or push model runs on more than one replica, an event a handler writes to the event store and
  feeds back needs a durable path of its own. Either your forwarder publishes it to the broker queue the listener
  reads, as it does for any other written event, or the handler feeds it again whenever the event that triggered it is
  delivered again, also when the write is already stored. Replicas share the catch-up marker, so another replica's
  catch-up can store a marker at or above an event this replica fed back and has not handled yet. Without either path,
  a feed or push model that runs on more than one replica can lose such an event, and
  [ADR 179](../architecture/decisions/0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md)
  lists when in "Fed-back events on the reactor stack". 0.34.0 has the same gap.
- `cancelSubscription(id)` still deletes the marker, after it cancels, and the `Mono` it returns completes once the
  delete is done. A later subscribe of the id can still resume after a stored marker, and "What a reactor catch-up
  start replays" in ADR 179 says when. Subscribe under a new subscription id when you need a rebuild.
- The marker is stored under the subscription id alone. When you change the filter or the projection, subscribe under
  a new id. A start under the old id can resume after a position its earlier filter stored, and skip the events at or
  below it that the new filter matches. 0.34.0 skipped the whole replay in that case.
- Under a new id the subscription starts from the beginning of the history and repeats everything its handler does,
  the records it writes, the commands it dispatches and the messages it sends. Stop every instance on the old id
  before the first one on the new id starts. Stopping them doesn't prevent the repeats. With more than one instance,
  what else a new id needs for a rebuild or a changed filter or projection depends on where each instance keeps what
  it builds and which broker queue or consumer group it consumes, as
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes.
- A catch-up that reads a checkpoint stored under the subscription id takes it as its marker. When something other
  than a catch-up stored it, such as a durable subscription that used the id before it moved to `source = PUSH`, and
  it is not a position, that catch-up fails with an `IllegalStateException` that names the id. 0.34.0 took any stored
  checkpoint as a finished catch-up and skipped the replay. Subscribe under a new id, or, when nothing else uses that
  checkpoint, delete it with every instance stopped and start them all again with the same start. Under a new id the
  subscription starts from the beginning of the history and repeats everything its handler does, the records it writes, the commands it
  dispatches and the messages it sends. Stop every instance on the old id before the first one on the new id starts.
  Stopping them doesn't prevent the repeats. With more than one instance, what else a new id needs depends on where
  each instance keeps what it builds and which broker queue or consumer group it consumes.
  [Changing a checkpoint or marker that several instances share](#changing-a-checkpoint-or-marker-that-several-instances-share)
  describes both. A start that finds no marker replays the whole history. A failed catch-up
  does not delete that checkpoint, but the push model's `cancelSubscription(id)` does. When a durable checkpoint is a
  position, a catch-up can resume after it and replace it. So don't share a subscription id between a durable subscription and a catch-up-then-push model,
  `CatchupProjectionFeed` or `DomainEventFeed`, on either stack.
- `ReactiveReplayAware`, and a `ReplayAware` view on a reactor feed, hear `replayStarted()`, `replayCompleted()` and
  `replayAbandoned()` only for a replay from the beginning, as in 0.34.0. A replay after a marker delivers its events
  the way the live feed does, without those calls. A `CatchupListener` on the reactor push model follows the same rule,
  and hears `catchupStarted`, `historyRead` and `alreadyDeliveredByReplay` only for a replay from the beginning. In
  0.34.0 it heard `catchupStarted` on every start, and the applied-append recorder of the reactive Spring Boot starter
  deleted its records then, so a wait for an append applied before a restart could time out. The next start of the id,
  whether it replays from the beginning or resumes after a marker, first sends `historyRead` for a catch-up the listener
  heard start and that a stop, a cancel or a failure cut short, so the listener stops waiting for it. A catch-up a
  shutdown cuts short gets no `historyRead`. A listener of your own that counted on hearing every start needs a look.

0.34.0 wrote the marker only after a replay from the beginning, and a 0.34.0 start that found it skipped its replay and
never wrote it again. The first 0.35.0 start that reads that marker resumes after it, so for an id that has run for
long it can replay most of the history. That replay sends no replay callbacks, so a view that batches its writes during
a replay, such as the one `Projections` builds, reads and writes once per event through it, and the applied-append
recorder writes a record for each append the projection applies. Plan for that replay, and set
`startupMode = BACKGROUND` when a startup probe could cut it short. A 0.34.0 node that reads a marker 0.35.0 stored
skips its replay, as it always did, so a rolling upgrade needs nothing more on either side.

## The reactor `HistoryLossReportingSubscriptions` gains a method

`HistoryLossReportingSubscriptions` in `org.occurrent.subscription.api.reactor` gains an abstract method:

```java
Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt);
```

A class that implements the interface doesn't compile until it implements the method. The reactor catch-up
subscriptions read their live phase through it, so they stop instead of skipping events when the history is lost
after the subscription has read events.

### What to do

Implement it so that the returned `Flux` fails with `HistoryLostException` whenever your model loses the history it
reads from, as it opens or later, and so that your model never restarts the subscription from the current time.
Retrying another error from the position the subscription has read to is fine. Set `lostFrom` to
the position the subscription had read to, or to the position it opened at when it has read nothing. There is no
default implementation, because a model that can't report a later loss would let a catch-up subscription skip events
without saying so. The rewrite recipe doesn't add the method, since it has to be written for your model.

## The MongoDB starter adds two health contributors for competing consumers

With `spring-boot-health` on the classpath, the MongoDB Spring Boot starter adds the health contributors
`competingConsumers` and `competingConsumerLeases`. Spring Boot reports `/actuator/health` as down when any contributor
is down, so the overall status can be down in cases where 0.34.0 reported it up.

### Who is affected

An application on `occurrent-mongodb-spring-boot-starter` with Spring Boot Actuator, when something acts on the overall
status of `/actuator/health`, such as a load balancer or a Kubernetes probe pointed at that path. A probe on
`/actuator/health/liveness` or `/actuator/health/readiness` is not affected, because neither contributor is in those
groups.

### What changes

`competingConsumers` has one component per subscription id, and `competingConsumerLeases` one per
`SpringMongoLeaseCompetingConsumerStrategy` bean. A component is down in these cases:

| Contributor | Down when | Up again when |
| --- | --- | --- |
| `competingConsumers` | The wrapped model refused to start the subscription after `subscribe` returned, so the subscription was given up. | The id is subscribed again or cancelled. |
| `competingConsumers` | The wrapped model refused to start or resume the subscription from its position, so it's held paused. | The subscription is resumed. |
| `competingConsumers` | A call to the lease strategy or the wrapped model failed, and the model keeps trying it. | A try succeeds. |
| `competingConsumerLeases` | The latest lease refresh attempt failed, for example because MongoDB refuses writes. | A refresh attempt refreshes every lease. |

A node on standby, a subscription you paused and a stopped model are up. The details of a component that is down
include the failure and since when it has been failing. When two `CompetingConsumerSubscriptionModel`s use the same
subscription id, its component is down when one of them reports a failure.

### What to do

Nothing, if a node should report down in these cases. To leave one out, set
`management.health.competingconsumers.enabled=false` or `management.health.competingconsumerleases.enabled=false`. To
change what one reports, declare your own `HealthContributor` bean named `competingConsumers` or
`competingConsumerLeases`, which replaces the starter's.

`CompetingConsumerSubscriptionModel#competingConsumerStatus(subscriptionId)`, and `leaseRefreshFailure()` on
`SpringMongoLeaseCompetingConsumerStrategy` and `NativeMongoLeaseCompetingConsumerStrategy`, return the same information
to code of your own. The
code compiles unchanged, there is no OpenRewrite recipe, and nothing stored changes. See
[ADR 183](../architecture/decisions/0183-a-competing-consumer-reports-its-status-and-the-mongodb-starter-turns-it-into-health.md).
