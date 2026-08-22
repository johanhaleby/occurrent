### Changelog next version

#### Highlights

* Declaring a sealed event supertype now matches every concrete type it permits, in projections, DSL subscriptions, queries and snapshot views alike, the same way sagas already did.
* `PushSubscriptionModel` takes an optional `PushObserver` that reports a `RoutingOutcome`, one of six precise values rather than a single match flag, for each event `accept(...)` is asked to deliver, so you can acknowledge a broker message safely instead of guessing.
* The flow saga's deprecated `join` is removed. The `org.occurrent.UpgradeToOccurrent_0_34` recipe rewrites the mechanical call sites to `on(allOf(...))` for you.
* Forwarding stored events to a broker at least once no longer needs a shipped transport, or you can use the new RabbitMQ and Kafka ones. `CloudEventForwarder` holds its checkpoint back until your `CloudEventSink` confirms delivery, so an application with its own publisher wrapper gets that guarantee at the forwarder's default start position, and `occurrent-broker-rabbitmq-blocking` and `occurrent-broker-kafka-blocking` now give you a `CloudEventSink` for each broker that keeps the same guarantee.

#### Changes

* **`RetryExecution` now checks its shutdown predicate while a backoff sleep is running, not only right before it starts.** A caller like `RabbitMqCloudEventSink.close()` passes a shutdown predicate so an in-flight retry loop stops as soon as it fires, but the predicate was only read before each sleep began, so a shutdown requested one millisecond into a long backoff still waited out the whole sleep before the next attempt's own check caught it. The loop now polls the predicate every 50ms during the sleep, so a shutdown request is caught at the next poll instead of only after the whole remaining backoff has elapsed. That 50ms figure is a polling cadence, not a guaranteed deadline, since a single sleep call can itself run a little longer under ordinary scheduling delay. An interrupted sleep still restores the thread's interrupt flag before rethrowing. Resolves [#866](https://github.com/johanhaleby/occurrent/issues/866).
* **`DurableSubscriptionModel` now records a fresh subscription's start position before anything is delivered, and refuses the subscription when it cannot.** A subscription that asks for the model default with no checkpoint stored is recorded from the wrapped model's `globalCheckpoint()` inside `subscribe(..)` itself. When that answer is `null`, which `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` both give when the server refuses the `hostInfo` command, shared MongoDB Atlas clusters for example, `subscribe(..)` throws `IllegalStateException` instead of starting without a recorded position. Up to 0.33.0 such a subscription started anyway with nothing in checkpoint storage, so a crash after a failed first delivery started over from wherever the feed had reached and the failed event was never redelivered. A subscription with a checkpoint already stored, and one subscribing with a `StartAt` of its own, are unaffected. The blocking `ManualStartSubscriptionModel` and the reactor `ReactorDurableSubscriptionModel` already answer an unanswerable position source this way. To start anyway, accepting the loss window, configure `DurableSubscriptionModelConfig.startWhenNoStartPositionCanBeRecorded(true)`, or set `occurrent.subscription.start-when-no-start-position-can-be-recorded=true` in the Spring Boot starter. The same override on `ReactorDurableSubscriptionModelConfig` lets `ReactorDurableSubscriptionModel` start such a registration too, and the property reaches the reactive starter as well. See [section 7 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#7-durablesubscriptionmodel-refuses-a-first-subscription-when-no-start-position-can-be-recorded). Resolves [#852](https://github.com/johanhaleby/occurrent/issues/852).
* **`MigrateSubscriptionModeProperty_0_32`'s YAML half no longer drops an unrelated profile's document from a multi-document `application.yml`.** The guard against re-adding `occurrent.subscription.mode` when a document already has it used `org.openrewrite.yaml.search.FindProperty` as a precondition, which OpenRewrite evaluates over the whole file rather than the one document that sets it. A Spring profile document that never set `occurrent.subscription.mode` could still lose its `occurrent.subscription.enabled` key, and with it the whole document if that key was all it had, because a different profile in the same file happened to set the new key. The YAML half of the guard is now `DropRedundantYamlProperty`, a small imperative recipe (one no-arg subclass per property pair, since this module has neither Lombok nor `-parameters` to bind a declarative options map onto it) that re-checks the replacement key against the specific `Yaml.Document` it would delete from. The `.properties` half is unaffected, since a `.properties` file has no multi-document concept for the bug to exploit. Fixes [#828](https://github.com/johanhaleby/occurrent/issues/828).
* **`MigrateSubscriptionModeProperty_0_32`'s YAML halves no longer rename an unrelated profile's `occurrent.subscription.enabled` to an invalid `occurrent.subscription.mode` value.** `MigrateSubscriptionEnabledFalseInYaml_0_32` and `MigrateSubscriptionEnabledTrueInYaml_0_32` used `org.openrewrite.yaml.search.FindProperty` as a precondition checking `occurrent.subscription.enabled` for one specific boolean, which OpenRewrite evaluates over the whole file rather than the one document that sets it. In a multi-document `application.yml`, a single profile holding the matching boolean satisfied the precondition for the entire file. `org.openrewrite.yaml.ChangePropertyValue` in the recipe list is itself value-scoped and correctly left a profile whose own `enabled` held the other boolean alone, but `org.openrewrite.yaml.ChangePropertyKey` has no value check at all, so it renamed the key in every profile that had it. A profile whose own value never matched the precondition ended up with the raw boolean literal under `occurrent.subscription.mode`, an enum (`disabled`/`manual`/`auto`) that does not bind to it. Fixed the same way as the sibling bug in `DropRedundantSubscriptionEnabledInYaml_0_32` ([#828](https://github.com/johanhaleby/occurrent/issues/828)): `MigrateSubscriptionEnabledInYaml`, a small imperative recipe with one no-arg subclass per boolean, the pattern `DropRedundantYamlProperty` already uses, re-checks the document's own value before rewriting that specific `Yaml.Document`, so a profile is only rewritten when its own value matches. The shipped recipe IDs are unchanged. Fixes [#834](https://github.com/johanhaleby/occurrent/issues/834).
* **A `@Projection(source = PUSH)` fed by a `DomainEventFeed` the starter can identify without creating it no longer fails startup under an id-sensitive `CheckpointStorage`, fixing behaviour shipped in 0.33.0.** The Spring Boot starter's fencing check used to ask about every push projection's id once `catchup` defaulted to `FROM_EVENT_STORE`, even for a `DomainEventFeed`-fed projection, which never resolves `CheckpointStorage` at all. A storage refusing that id's shape, `SpringRedisCheckpointStorage`'s Cluster-safe mode for example, could sink startup for a projection that would never have written a fenced checkpoint. A feed bean the starter cannot type read-only, an ambiguous or otherwise unresolvable one, stays checked, the same conservative behaviour as before this fix. See [section 8 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#8-three-ways-the-spring-boot-starter-now-refuses-to-start). Resolves [#788](https://github.com/johanhaleby/occurrent/issues/788).
* **`SagaFilters` is now public, in `dsl/saga-dsl/common` (`org.occurrent.dsl.saga.internal.SagaFilters`), instead of package-private in `dsl/saga-dsl/blocking`.** It composes the filter a saga subscribes on, mirroring the already-public `ProjectionFilters` in `dsl/projection-dsl/common`, so a future reactive saga runner can reuse it instead of writing the same composition again. Nothing public shipped from the old location, so this is a straight addition. Resolves [#786](https://github.com/johanhaleby/occurrent/issues/786).
* **Reading a stream with a `StreamReadFilter` and a nonzero skip now resumes from the correct stream position, on every event store Occurrent ships.** The native, Spring blocking, Spring reactive, and in-memory stores previously applied skip after the filter had already narrowed the result, so it counted matching events instead of stream positions. `ApplicationService#execute` with both `ExecuteOptions.fromStreamVersion(n)` and a filter set could silently resume from the wrong place in the stream because of this. A negative skip is rejected with an `IllegalArgumentException` on every store, enforced by the shared conformance suite. Fixes [#810](https://github.com/johanhaleby/occurrent/issues/810).
* **The native MongoDB store no longer leaks a `MongoCursor` when a stream read's `Stream` is closed early**, including through Kotlin's `.use { }`. The Spring store already closed correctly here. While tracking this down I also found a dead branch in the same method that looked like it dropped `skip` and `limit` on an unsorted query, but never actually did, because the MongoDB driver's query builder mutates itself in place regardless of which branch runs. Cleaned it up anyway, with no behavior change. Fixes [#810](https://github.com/johanhaleby/occurrent/issues/810).
* **A flow saga instance parked in a step that was since renamed or removed now refuses the delivery with a clear `IllegalStateException`, instead of a bare `NullPointerException`, fixing behaviour shipped in 0.33.0.** The message names the step and offers two ways out. Put the step back, or add a temporary step under the old name that transitions its instances onward, until every parked instance has moved past it. Or delete the stuck instance from the `SagaStateStore`. See [ADR 128](doc/architecture/decisions/0128-a-renamed-or-removed-step-refuses-its-parked-instances.md). Resolves [#748](https://github.com/johanhaleby/occurrent/issues/748).
* **`ReactorDurableSubscriptionModel` no longer drops a live subscription registered under an id whose earlier, failed attempt is still finishing.** Registering again under the same id, the documented recovery from a refused registration, could have the first attempt's failure remove the second registration instead of its own once it finished, leaving the subscription running but invisible to `isRunning`, `isPaused`, `subscriptionIds` and `cancelSubscription`. See [#791](https://github.com/johanhaleby/occurrent/issues/791).
* **A registration-time position read that fails now logs at `WARN` instead of `ERROR` in `ReactorDurableSubscriptionModel`, and the refusal message for a position source answering nothing is sharper there and in `ManualStartSubscriptionModel`.** A checkpoint already stored, or a start position of its own, can still let the subscription start despite the failed read, so `ERROR` overstated it. The refusal message no longer claims there is no position at all when a stored checkpoint is what actually settles the registration, and no longer says "while registering" for a read that can also happen when a stopped subscription starts. See [#804](https://github.com/johanhaleby/occurrent/issues/804) and [#805](https://github.com/johanhaleby/occurrent/issues/805).
* **Two nodes racing to pin a manually-started subscription's first checkpoint on MongoDB no longer risk losing the events between their two positions, and neither does a checkpoint deleted and re-registered while a registration was already under way.** `CheckpointStorage` gains an opt-in `resolveFirstCheckpointRace`, which the native and Spring MongoDB storages implement by comparing the operation time a stored and a candidate checkpoint each carry, atomically with the write that comparison calls for. `ManualStartSubscriptionModel` and `ReactorDurableSubscriptionModel` both reach for it before falling back to the narrower, write-order-based rule 0.33.0 shipped, so on MongoDB the earlier of the two positions now governs regardless of which write reached storage first, and a checkpoint that raced in after an existence check is resolved by comparison instead of accepted on presence alone. A storage without an ordering to compare by, Redis and the in-memory storage included, keeps the narrower rule exactly as it was. See [ADR 130](doc/architecture/decisions/0130-a-subscriptions-first-position-race-resolves-by-order-not-by-write-order.md). Resolves [#771](https://github.com/johanhaleby/occurrent/issues/771).
* **A flow saga's `stepWindow` now counts and evicts only the events its steps declare, fixing behaviour shipped in 0.33.0.** An event of a type no step names, reachable through a `narrowingFilter`/`replacementFilter` wider than the flow's own types or a collapsing `CloudEventTypeMapper`, used to take one of `stepWindow`'s slots and could evict one of the step's own events to make room for itself. It is still retained, since nothing here discards an event that genuinely arrived and correlated to an instance, but it no longer counts against the cap or evicts a declared event. A step fed only such events grows with no cap from `stepWindow` at all, so the store-boundary warning added in 0.33.0 is what surfaces that growth instead. See [ADR 129](doc/architecture/decisions/0129-a-flow-sagas-stepwindow-caps-only-its-own-declared-events.md), which also decided against requiring an explicit retention choice for a flow that can park, and [section 2 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#2-a-flow-sagas-stepwindow-now-caps-only-its-own-declared-events) if you set a wide `narrowingFilter`/`replacementFilter` or a collapsing `CloudEventTypeMapper`. Resolves [#773](https://github.com/johanhaleby/occurrent/issues/773), [#764](https://github.com/johanhaleby/occurrent/issues/764).
* **Four subscription lifecycle-leak fixes in the blocking competing-consumer, durable, and catch-up subscription models.** `CompetingConsumerSubscriptionModel` no longer strands a lease when its delegate refuses to pause, so a node whose catch-up already failed still lets another node take over. `DurableSubscriptionModel` no longer skips checkpoint-based resume for a subscription that opts out once and later resubscribes as managed. In the blocking catch-up models (`StreamCatchupSubscriptionModel` and the DCB catch-up model), cancelling a subscription and resubscribing it right away no longer risks the cancelled replay going live anyway, the new subscription looking cancelled instead, or a stale in-flight event recreating the checkpoint the cancellation just deleted. Resolves [#737](https://github.com/johanhaleby/occurrent/issues/737).
* **A cancellation of a blocking catch-up subscription racing its own handover to a live subscription is no longer silently lost, and a fresh attempt for the same id can no longer have its saved position wiped out by a stale attempt's late cleanup.** Both `StreamCatchupSubscriptionModel` and the DCB catch-up model now hold a per-id lock across the whole handover, not just the ownership decision, closing a gap that predates the fixes above. See [ADR 131](doc/architecture/decisions/0131-a-per-id-lock-closes-the-blocking-catchup-handover-race.md). Fixes [#827](https://github.com/johanhaleby/occurrent/issues/827).
* **A projection, a subscription built through the subscription DSL, a query through `DomainEventQueries`, and a `@Snapshot` view now each expand a declared sealed event type into the concrete types it permits, the same fix 0.33.0 already shipped for a saga and an annotation-based subscription.** Declaring a sealed supertype used to derive a filter naming only that supertype's own CloudEvent type, so a projection, a subscription, a query, or a snapshot keyed on one silently matched fewer events than dispatch would actually accept. All four now call the same `EventTypeExpansion.deriveFilter`, added to `common/filter` alongside `expand`, that the saga DSL and the annotation registrars already used. `DomainEventQueries.query(Collection)` keeps treating a null or empty collection as "match nothing" rather than "match everything", unaffected by the wider expansion. See [ADR 126](doc/architecture/decisions/0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md). Resolves [#750](https://github.com/johanhaleby/occurrent/issues/750) and [#758](https://github.com/johanhaleby/occurrent/issues/758).
* **A `@Projection`, `@Saga` or `@Snapshot` factory method no longer fails Spring Boot startup when its bean is a JDK interface proxy.** The registrar used to invoke the factory method on whatever bean the context resolved, which could be a proxy implementing only the bean's interfaces rather than the concrete class the method is declared on, once that bean was advised under `spring.aop.proxy-target-class=false`. Invoking the method there threw `IllegalArgumentException`. A CGLIB proxy survived by accident, since it subclasses the target and inherits the method either way. `SubscriptionAnnotations.invokeDescriptorFactory` now unwraps the bean to its ultimate AOP target and re-resolves the factory method there before invoking, shared by all five registrar paths that had this call. This covers a proxy with a fixed singleton target, which both `spring.aop.proxy-target-class=false` and `=true` create by default. A proxy backed by a prototype- or pool-scoped target source is left proxied and still fails, now naming the annotation, the factory, and the fix instead of a bare reflection error. See [ADR 127 section 4](doc/architecture/decisions/0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md#4-a-descriptor-annotation-is-read-after-the-singletons-are-instantiated). Fixes [#836](https://github.com/johanhaleby/occurrent/issues/836).
* **`PushSubscriptionModel` and its reactor counterpart take an optional `PushObserver`, told a `RoutingOutcome` for each event `accept(...)` is asked to deliver.** `accept(...)` itself stays silent by design, since it cannot refuse an event that might also have come from the write path (see ADR 104), so a misconfigured queue binding, a missing declared event type, or a type-mapping typo used to look identical to a saga or projection that simply chose not to react. `RoutingOutcome` is `DELIVERED` when a running, unpaused subscription's filter accepted the event, `FILTERED` when that same subscription evaluated it and declined it, and `UNAVAILABLE` when there was no running, unpaused subscription for the event to reach at all. Acknowledge on `DELIVERED` and `FILTERED`, never on anything else. A paused subscription reports `UNAVAILABLE`, not `FILTERED`, because its filter is never consulted, and that distinction is what a plain match flag could not make. A caller that checked `isRunning(...)` after `accept(...)` returned, instead of reading the reported outcome, could see a concurrent resume answer `true` for an event that had already been dropped. A filter that throws while being evaluated reports `NOT_DELIVERABLE`, which is what that value means, since a filter that failed to answer neither accepted nor declined the event, and it always arrives with the filter's own exception. A registered action that refuses an event before attempting any dispatch reports `REFUSED` when it promises the refusal is permanent, which a catch-up-then-live engine does once its replay has failed, and `NOT_DELIVERABLE` when it does not, which is what a full live buffer during a replay gets. So a caller acknowledges on `DELIVERED` and `FILTERED`, offers the event again on `DEFERRED` and `UNAVAILABLE`, applies its own failure policy to `NOT_DELIVERABLE`, and stops on `REFUSED`. Pass the observer through the same constructor that already takes a `DataFieldReader`, and `PushObserver.noop()` is the default, so nothing changes for existing code, including its cost. The match check is skipped entirely when none is configured, and where it does run it shares the same evaluation `route(...)` dispatches from, so the two can never disagree. A `RuntimeException` or `AssertionError` the observer itself throws is always caught and logged rather than turning a delivered event into a broker redelivery. On the blocking stack, the observer is told once the matched registration's action has run, never before. A broker bridge calls `PushSubscriptionModel.acceptRedeliverable(CloudEvent)` rather than `accept(...)` to feed this model, which refuses an event outright rather than buffering it while a `CatchupThenPushSubscriptionModel` in front is still replaying or draining, reported `DEFERRED`, safe to redeliver and never a reason to acknowledge. Fed through plain `accept(...)` instead, that same wrapper still buffers the event and reports `DELIVERED` once it applies it, exactly as it always has. The reactor stack draws the same distinction on the domain-feed side, narrower on the push side: `RegisteringSubscribable.routeReportingMatch` reports from a `RoutingAction` that runs first and returns `Mono<Boolean>`, `DomainEventFeed.acceptCloudEvent` and `CatchupProjectionFeed` report `DEFERRED` rather than `NOT_DELIVERABLE` for an event that arrives before the projection is live, and `ReactiveHandover.acceptIfLive(T)` refuses such an event outright the same way `acceptIfLive` does on the blocking side, without ever buffering it. The reactor `PushSubscriptionModel` itself has no `acceptRedeliverable` counterpart, and its `CatchupThenPushSubscriptionModel` still calls `acceptReportingDelivery`, so a reactor broker bridge has nothing to call yet for the buffering-refusal half of this. Nothing here adds a reactor broker bridge, so the `DeliveryFailurePolicy` bypass half of this has no reactor consumer yet, only the correctness half does. Resolves [#802](https://github.com/johanhaleby/occurrent/issues/802) and [#848](https://github.com/johanhaleby/occurrent/issues/848).
* **`DomainEventFeed` (both stacks) gains `acceptCloudEvent(CloudEvent)`, for a broker bridge that has a `CloudEvent` to hand over rather than an already-decoded domain event.** A new name rather than an `accept(CloudEvent)` overload, since `DomainEventFeed<CloudEvent>` is a legal instantiation and an overload would let the compiler silently choose between two methods with different behavior for the same argument. It matches the event against the same `Filter` `register(...)` was given, decodes it with this feed's `CloudEventConverter` only if it matches, delivers it, and returns the `RoutingOutcome`. A non-matching event is never decoded. The replay and the live path now share that one filter, so they can never disagree about which events are this projection's. `DomainEventFeed` also takes an optional `DataFieldReader` constructor argument, mirroring `PushSubscriptionModel`'s, so a payload condition on that filter can be answered on the live path too. `register(...)` itself is unaffected either way, since it never evaluates the filter in memory. `acceptCloudEvent(...)` is what needs a `DataFieldReader`, and throws the new `UnreadableLiveFilterException` for a payload condition it cannot answer, on its first call rather than on `register(...)`, so a caller that never calls `acceptCloudEvent(...)` keeps registering exactly the filters it always could. That refusal is permanent for the registration, the same exception instance is thrown again on every later call rather than rebuilt, since nothing about a retry changes the answer. It reports `RoutingOutcome.DEFERRED`, not `DELIVERED`, for a matching event that arrives before the registered projection is live, whether that is before any catch-up has run at all, while a replay is still going, or after `stopCatchUp()` interrupted one in flight, since the catch-up-then-live engine refuses such an event outright rather than delivering or buffering it, and a redelivery is always safe until it lands as `DELIVERED`. Feeding it before a projection is registered still throws `IllegalStateException`, as it always has. This feed has no write path to protect, so it keeps refusing outright rather than reporting `UNAVAILABLE` for that case. It also gains `refusesPermanently()`, which answers true once its registered projection's own catch-up has failed and never goes back to false, so a listener can tell that from a replay that is still running and stop consuming for good instead of redelivering into the same refusal. See [ADR 133's amendment](doc/architecture/decisions/0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md#amendment-2026-08-18-a-domain-level-payload-filter-is-refused-on-first-live-match-use-not-at-register-and-the-refusal-is-permanent) for why this refuses here instead of at `register(...)`. Resolves [#848](https://github.com/johanhaleby/occurrent/issues/848).
* **`AppliedAppendStore` records which appends a projection has applied, and answers whether a particular one has been applied yet.** The membership half of [ADR 132](doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md), alongside the write-side `AppendId` stamping shipped earlier in this release. `recordApplied`, `hasApplied`, `clear` and an `inMemory()` factory live in `dsl/projection-dsl/common`. Both Spring Boot starters auto-configure a Mongo-backed store, one document per (projection id, append id) pair. A compound unique index means recording the same append twice writes one document, not two, and a TTL index bounds how much storage the records take. `waitUntilApplied(projectionId, appendId, timeout)` polls until the append shows up or the timeout passes. A store it cannot reach keeps the wait polling rather than answering true early. Configure the collection name, retention, and the wait's poll pacing under `occurrent.projection.applied-append`. `Projections.recordingAppliedAppends(..)` no longer takes a replay check. Build the recording view from a projection id and a store, then call `ReplayAwareSubscriptions.listenForCatchup(projectionId, view)` on the model your subscription runs on, before you subscribe, and schedule the view's `pollForClear()` so a clear that failed while a catch-up ran is retried. An append handled while a clear is still owed waits for that clear, up to a thousand of them, and past that the oldest are dropped with a warning and their waits time out. Set `@Projection(recordAppliedAppends = true)` to wire a projection into this store automatically, on both the blocking and reactor stacks, instead of calling `recordApplied` yourself. It refuses at startup with no `AppliedAppendStore` bean, and with `mode = SYNCHRONOUS`, since a synchronous projection already answers read-your-writes without it. Nothing is recorded while the projection is reading the history its replay set out to read, for a composition whose replay phase this can resolve. A catch-up also delivers the events written while it was reading that history, and those are recorded, because for some of them it is the only delivery they get. A custom or third-party subscription model that does not send those two signals puts its projections on a polled fallback, which records nothing for the whole of a catch-up it sees and misses one that starts and finishes between two polls entirely. Implement `ReplayAwareSubscriptions.listenForCatchup(subscriptionId, listener)` if your own model catches up. The reactor stack logs a warning once when it cannot see a catch-up capability at all. The blocking stack stays silent there by design, since it cannot tell that case apart from a composition that simply never replays (ADR 132 decision 8). An event-store projection left on the default start position on Occurrent's own shipped Mongo composition, an explicit `NOW`, or otherwise wired so its composition never replays, gets its own startup `WARN` naming it, since its recorded memberships then survive a rebuild until the TTL evicts them or an operator clears them (ADR 132 decision 9). A custom composition's own default behavior is its own to declare, so this stays silent there instead of guessing. Replacing `Subscriptions`, `StreamSubscriptions` or `DcbSubscriptions` with a composition of your own keeps that same silence even when the starter still built its own default model behind the scenes, since the warning is now tied to the exact composition it was recorded for rather than to whichever model happens to be in the context. Part of [#740](https://github.com/johanhaleby/occurrent/issues/740), [#865](https://github.com/johanhaleby/occurrent/issues/865), and [#871](https://github.com/johanhaleby/occurrent/issues/871).
* **A reactive `@Projection(source = PUSH)` projection fed by a `DomainEventFeed` and materializing into a `MaterializedView` now updates its read model with each live event's real `EventMetadata`, not `EventMetadata.empty()`.** The registrar routed that one combination through `Projections.reactiveUpdate(MaterializedView)`, which discards metadata by design for a caller with none to give, so a projection keyed on the stream id or reading `appendid` never worked there even though nothing about the combination looks unsupported. It now goes through the metadata-aware `reactiveUpdateWithMetadata` builder instead, the same one every other reactive registration path already used. Found while wiring `@Projection(recordAppliedAppends = true)` into the same code path. Part of [#740](https://github.com/johanhaleby/occurrent/issues/740).
* **A new transport-neutral broker foundation, `occurrent-broker-api-blocking`, plus a RabbitMQ module with both a publish and a consume side built on it, `occurrent-broker-rabbitmq-blocking`, and a Kafka publisher, `occurrent-broker-kafka-blocking`, let an application forward stored events to a broker at least once, with your own wrapper or with either shipped module.** `CloudEventForwarder` runs a durable subscription at its default start position and hands each stored event to a `CloudEventSink` an application implements, advancing its checkpoint only once the sink returns, so a sink that throws leaves the checkpoint where it was and the event publishes again on the next run. That makes publication at least once as long as the sink itself does not return before the broker has actually taken the event, which is the sink's own contract to keep, not something the forwarder can verify for it. The two overloads that take an explicit start position opt out of that resumption instead, as their own javadoc explains. `DomainEventForwarder` does the same for an application whose own converter already produces domain events, decoding each stored `CloudEvent` once and stamping its `EventMetadata` onto the domain event rather than converting twice. `RabbitMqCloudEventSink` and `RabbitMqDomainEventSink` are the shipped RabbitMQ side of that same contract. A publish does not return until RabbitMQ has both confirmed the message and routed it, so a routing key with no matching binding fails the publish instead of confirming it and quietly discarding it. `RabbitMqTopicExchangeDestinationResolver` derives the exchange and routing key from the cloud event type through a `CloudEventTypeMapper`, the same one your `CloudEventConverter` already uses, so a publisher and a consumer agree on where an event goes by reading one mapping instead of matching two hand written strings. The RabbitMQ module now has a consume side too. `RabbitMqCloudEventBridge` reads a queued message back into a `CloudEvent` through the same `RabbitMqCloudEventMapper` the sink writes with, hands it to `PushSubscriptionModel.acceptRedeliverable(CloudEvent)`, and acknowledges only when the reported `RoutingOutcome` was `DELIVERED` or `FILTERED`, never on `UNAVAILABLE` (no running, unpaused subscription reached the event at all), on `NOT_DELIVERABLE` (its filter itself threw while being evaluated), on `DEFERRED` (a `CatchupThenPushSubscriptionModel` in front of the model is still replaying or draining, safe to redeliver), on `REFUSED` (that wrapper's own catch-up has permanently failed, so the bridge stops rather than parking or redelivering into the same refusal), or on a thrown handler exception. `RabbitMqDomainEventBridge` does the same for a `DomainEventFeed`, calling `acceptCloudEvent` instead, since the feed is what evaluates the filter there. `acceptCloudEvent` never returns `UNAVAILABLE`. A matching event that arrives before the feed's catch-up-then-live transition actually reaches live gets `DEFERRED` instead, and an unregistered feed throws `IllegalStateException` rather than reporting an outcome at all. Both bridges declare their queue and bindings from `DestinationResolver.destinationsFor(filter)`, or the catch-all destination when no filter narrows it, unless `declareTopology(false)` is set, so an existing queue and its bindings stay a platform team's to own. An explicit empty `bindings(...)` set now fails `build()` with an `IllegalStateException` instead of consuming nothing, on Kafka always and on RabbitMQ whenever `declareTopology(true)` (the default) applies, since `declareTopology(false)` never reads `bindings` at all. A delivery that is not acknowledged is either redelivered, the default, or published to a parking destination and acknowledged only once that publish is confirmed, chosen through `onDeliveryFailure(DeliveryFailurePolicy)`. A background poll starts and stops the underlying AMQP consumer to match the subscription's running state, or, for the domain bridge, the feed's registration and whether its catch-up-then-live transition has actually started, one second apart by default, a coarse efficiency measure rather than what keeps a message safe. What actually stops a stopped model from ever having an event treated as consumed is the reported `RoutingOutcome` on every single message, `UNAVAILABLE` there too, never acknowledged. `UNAVAILABLE`, the sole subscription paused, the model not running at all, or nothing registered, is held and redelivered paced the same way `DEFERRED` already is, bypassing `DeliveryFailurePolicy` entirely, since nothing about that message is broken either, only not deliverable right now, and `PARK` exists for a genuine failure, not for pacing. An unregistered feed instead throws `IllegalStateException`, which the domain bridge's own generic failure handling routes through the same `DeliveryFailurePolicy`. A `DomainEventFeed`'s own concurrent-duplicate case reports `DEFERRED` instead, the same as the wrapped `CatchupThenPushSubscriptionModel` case above, bypassing `DeliveryFailurePolicy` (including `PARK`) entirely and redelivering, since nothing about that message is broken either. Neither bridge holds a `CatchupThenPushSubscriptionModel`, only the `PushSubscriptionModel` it wraps (ADR 133 decision 1), and `RoutingOutcome.DEFERRED` is what keeps a message safe when that wrapper is still replaying or draining: `acceptRedeliverable(...)` refuses it outright instead of buffering it, and the bridge redelivers, bypassing `DeliveryFailurePolicy` (including `PARK`) entirely, since nothing about the message is broken. For `RabbitMqCloudEventBridge` and `KafkaCloudEventBridge` specifically, that same poll also reads an optional `readinessSource`, a pacing hint rather than a correctness requirement. Wire `readinessSource(catchupThenPush::isReadyForLiveDelivery)`, the wrapper's new `isReadyForLiveDelivery(String)`, and the bridge stops pulling messages at all for as long as that answers false, cutting down on how often the refuse-and-redeliver round trip above happens rather than being what prevents it. `@Projection(source = PUSH)` and `@Saga(source = PUSH)` publish the `CatchupThenPushSubscriptionModel` they build internally as a `"catchupThenPushSubscriptionModel-" + id` bean for exactly this wiring, and the RabbitMQ and Kafka Spring Boot starters wire it automatically with no configuration needed. The default, unset, is unchanged for a `PushSubscriptionModel` fed with no catch-up wrapper in front of it. A `DomainEventFeed` registered with a payload filter it cannot evaluate live throws `UnreadableLiveFilterException`, and `RabbitMqDomainEventBridge` treats that as permanent. It stops itself and never acknowledges that delivery, rather than redelivering into the same failure forever. `example/broker/rabbitmq` runs the RabbitMQ side of this loop end to end against a real RabbitMQ and a real MongoDB, at both levels, both as a runnable bootstrap an operator can point at their own broker and as Testcontainers proofs that a catch-up replay and a broker redelivery of the same events still leave a projection in the right state, that a handler which throws does not lose the message, that restarting the consumer resumes from the broker instead of replaying its whole history again, and that `EventMetadata` survives the round trip through RabbitMQ's headers on the domain path. `KafkaCloudEventSink` and `KafkaDomainEventSink` are the shipped Kafka side of the publish contract. A publish does not return until the broker has acknowledged the send with `acks=all`, which the sink sets on your behalf when your producer config leaves it unset, and refuses to start if you set it any weaker, since an acknowledgement under a weaker setting can arrive for a send the broker never durably stored. `KafkaSharedTopicDestinationResolver` is the shipped default, one topic you name for every event, keyed by the event's stream id, so two events of the same stream stay in order against each other on that topic's shared partition even when they are different event types. Choose that topic's partition count before producing to it, since Kafka hashes a key against the current partition count, so growing it later remaps existing stream ids across partitions and can silently break ordering for whatever streams are still in flight. `KafkaTopicPerTypeDestinationResolver` derives a topic per cloud event type through a `CloudEventTypeMapper` instead and keys each message the same way, the documented alternative for a deployment that wants per-type topics for retention or independent consumer scaling and either has single-type streams or accepts that a stream mixing types is then ordered only within one type, since each type lands on its own topic. `KafkaCloudEventBridge` and `KafkaDomainEventBridge` are the shipped Kafka consume side, feeding a `PushSubscriptionModel` and a `DomainEventFeed` the same way their RabbitMQ counterparts do. A Kafka `Consumer` isn't thread-safe, so each bridge runs one dedicated poll loop that decides the coarse lifecycle gate, feeds the model or the feed, and commits, rather than splitting that work across threads the way the RabbitMQ bridges do. `DELIVERED`, `FILTERED`, and a delivery failure resolved by a confirmed park all stage a record's offset for the next commit. Anything left unresolved, a `REDELIVER` policy or a park that itself failed, seeks the consumer back to that record and stops processing the rest of that partition's poll, leaving other partitions in the same poll unaffected. `enable.auto.commit` has to be `false` or the bridge refuses to start, since Kafka would otherwise advance the offset on a timer regardless of what the bridge decided. `KafkaCloudEventMapper` reads a record back through `cloudevents-kafka`'s own binary reader and corrects two defects verified in that reader. `streamversion` and `position` come back `Long` rather than `String`, and data that is present but empty survives as that instead of collapsing to no data at all. Auto-configuration for both arrives too, `occurrent-broker-rabbitmq-spring-boot-starter` and `occurrent-broker-kafka-spring-boot-starter`, one Spring Boot starter artifact per transport so neither broker client ever lands on the other's classpath. `@EnableOccurrentRabbitMqBroker` and `@EnableOccurrentKafkaBroker` activate them, the same `@Import`-based mechanism `@EnableOccurrent` already uses for the MongoDB starter. Each builds its sink and a bridge factory from `occurrent.broker.rabbitmq.*` or `occurrent.broker.kafka.*`, every default copied from the builder default it configures. The RabbitMQ starter takes your own `Connection` bean rather than building one, since connection setup belongs to `spring-boot-starter-amqp`, not to this starter. A bridge is inherently one per consumer per ADR 90, so instead of one bean trying to be every consumer's bridge, each starter gives you a `RabbitMqCloudEventBridgeFactory` or `KafkaCloudEventBridgeFactory`, and a domain-level twin, pre-seeded with the shared defaults and leaving only the queue or consumer group to name per call. `CatchupThenPushSubscriptionModel` checks that it still owns the subscription id immediately before it starts writing its catch-up marker, so an attempt whose id was cancelled before that check writes nothing, and a marker that is there always describes a history that was read in full. A cancel arriving after the check, while the write is already running, does not call that write off, and does not need to, since what the marker claims was already true when it began. Every later subscription of that id trusts such a marker, whether it starts in the same process or after a restart, so cancelling a subscription and subscribing the same id again goes straight to live delivery rather than reading the history a second time. Delete that id's checkpoint when you want the history read again. The blocking model writes that marker under a lock for the subscription id rather than under the model's own monitor, so a checkpoint store that is slow to answer no longer holds up `stop`, `start`, `pauseSubscription` and `resumeSubscription` on that model while a write is in flight, and no longer holds the platform thread under the replay's virtual thread. One exception, since it is a slow store either way. A `cancelSubscription` for that same id still waits for the write, deliberately, and it holds the model monitor while it waits, so another lifecycle call arriving behind such a cancel waits too. `cancelSubscription` and `subscribe` still wait for a write already running, since those are the calls that move an id out from under one. Both stacks now refuse to write that marker while the model is stopped, so a stop reaching the marker step leaves nothing marked where before it wrote one. A stop cannot call off a write that has already begun, since that would mean waiting for a checkpoint store inside `stop()`, and `stop()` says so on both stacks rather than promising more. See [ADR 133](doc/architecture/decisions/0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md). Resolves [#413](https://github.com/johanhaleby/occurrent/issues/413), [#414](https://github.com/johanhaleby/occurrent/issues/414), [#415](https://github.com/johanhaleby/occurrent/issues/415), [#416](https://github.com/johanhaleby/occurrent/issues/416), [#417](https://github.com/johanhaleby/occurrent/issues/417), [#419](https://github.com/johanhaleby/occurrent/issues/419) and [#846](https://github.com/johanhaleby/occurrent/issues/846).
* **`@Transactional`, or any other class- or method-level Spring aspect, on a `@Subscription`, `@StreamSubscription` or `@DcbSubscription` handler method now actually runs when the handler is invoked.** The registrar invoked each of those three handler methods on the raw bean instance handed to the `BeanPostProcessor`, before Spring wraps it in its AOP proxy, so any such advice was silently skipped, on both the blocking and the reactor stack. The handler is now looked up from the `ApplicationContext` by bean name at delivery time instead, the same pattern `@SynchronousSubscription` already used, so the proxy runs and the advice applies. A handler method that already declared `@Transactional` starts running inside a transaction it never actually had, which can change its rollback and failure behavior. Resolves [#837](https://github.com/johanhaleby/occurrent/issues/837).

#### Breaking changes

* **A `DurableSubscriptionModel` subscription on a shared MongoDB Atlas cluster (or any wrapped model whose `globalCheckpoint()` answers `null`) now refuses to start unless it already has a checkpoint stored, see Changes above.** Up to 0.33.0 it started anyway with no recorded position. Set `occurrent.subscription.start-when-no-start-position-can-be-recorded=true` (or `DurableSubscriptionModelConfig.startWhenNoStartPositionCanBeRecorded(true)`) to keep the previous behavior, or subscribe with a `StartAt` of your own. See [section 7 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#7-durablesubscriptionmodel-refuses-a-first-subscription-when-no-start-position-can-be-recorded).
* **The flow saga's deprecated `join`, Kotlin's `expect<T>`, and `Expectation` are removed.** `join` was already deprecated in 0.33.0 in favor of `on(StepCondition, ...)` with `allOf(...)`, and that replacement is what every caller now needs. `UpgradeToOccurrent_0_34` rewrites every `join` call whose expectation list is a literal `List.of(...)`/`Arrays.asList(...)` of literal `Expectation.of(...)` calls, collapsing a duplicate-typed pair to the higher of their counts the same way `join` itself did, but only when both counts are integer literals. A list built from a variable or a method call, a duplicate-typed pair whose count is not a literal, and every Kotlin call site (`expect<T>` and `join` alike) are left alone and stop compiling, so the compiler finds them for you. [ADR 125](doc/architecture/decisions/0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md) had rejected removing `join` outright, since no recipe covered it and removal would have broken every caller with no automated fix. That recipe is what this release adds, closing the gap ADR 125 identified rather than relitigating it. See [section 1 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#1-a-flow-sagas-join-kotlins-expectt-and-expectation-are-removed), [#707](https://github.com/johanhaleby/occurrent/issues/707) and [#806](https://github.com/johanhaleby/occurrent/issues/806).
* **A projection, a subscription built through the subscription DSL, a query through `DomainEventQueries`, or a `@Snapshot` view that declares an event type whose concrete subtypes cannot be found is now refused, the same refusal 0.33.0 already shipped for a saga and an annotation-based subscription. And a concrete class that is neither final nor sealed, which every one of those six exempted in 0.33.0, is refused too.** You are affected if one of these declares an interface or an abstract class that is not sealed, an array, a primitive class literal, a sealed hierarchy reopened below the declared level, or a concrete class that is neither final nor sealed. That last shape is the one that also changes a saga and an annotation-based subscription. Up to 0.33.0 it derived a filter naming the declared class alone, so under every `CloudEventTypeMapper` Occurrent ships a caller declaring `class OrderPlaced` and publishing a `class SpecialOrderPlaced extends OrderPlaced` never received the subclass and got no warning, even though dispatch would have accepted it. One caller was working rather than losing events, the one whose own `CloudEventTypeMapper` maps a whole hierarchy onto a single CloudEvent type string, since the subclass was then stored under the declared class's type string and did arrive. That caller is refused too and wants an explicit filter, the same remedy the four shapes above already point it at. `Projection` and `SnapshotView` throw when a runner or a query derives the filter, `DomainEventQueries` and the subscription DSL throw at the first query or subscription registration that needs one, a saga throws at `build()`, and `SnapshotAnnotationRegistrar` and the subscription annotations throw at Spring Boot startup. Three remedies for the last shape, mark the declared class `final` when nothing extends it, seal the hierarchy, or declare the concrete event types, and two for the rest, seal the hierarchy or declare the concrete event types. `Projection.filter(...)`, `SnapshotView.filter(...)` and a saga's `replacementFilter(...)` are a further way out for the three that have an override. There is no recipe entry, for the same reason 0.33.0's equivalent change had none, and for the last shape also because a recipe cannot tell whether a class should become `final` or the declaration should list concrete types. See [section 3 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#3-declaring-an-event-type-whose-concrete-subtypes-cannot-be-found-is-refused) and [ADR 126](doc/architecture/decisions/0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md). Resolves [#753](https://github.com/johanhaleby/occurrent/issues/753).
* **`occurrent.event-store.collection`, `occurrent.event-store.time-representation`, `occurrent.subscription.collection` and `occurrent.subscription.restart-on-change-stream-history-lost` are renamed to `occurrent.event-store.mongodb.collection`, `occurrent.event-store.mongodb.time-representation`, `occurrent.subscription.mongodb.collection` and `occurrent.subscription.mongodb.restart-on-change-stream-history-lost`.** Each old key still works and is deprecated, removed in the release after next, and the new `UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites both `.properties` and `.yaml` for you. See [section 4 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#4-four-mongodb-only-keys-move-under-mongodb). Resolves [#439](https://github.com/johanhaleby/occurrent/issues/439).
* **A `@Projection`, `@Saga` or `@Snapshot` bean's class-level advice, `@Transactional` or a custom aspect for example, no longer runs once at startup as a side effect of its descriptor factory being invoked.** That was never a documented behavior. It came from CGLIB, the default under `spring.aop.proxy-target-class=true`, subclassing the bean, so the factory happened to run through the proxy and any matching advice ran with it. The factory now always runs directly on the bean's own class. See [section 5 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#5-a-descriptor-factorys-class-level-advice-no-longer-runs-at-startup).
* **A reactor `@Projection`, `@Saga` or `@Snapshot` factory method that returns `null` now fails startup with `IllegalStateException`, where it previously threw `IllegalArgumentException`.** This only affects a factory that is itself broken, returning `null` instead of the descriptor it declares, not a normal application, but a catch block scoped to `IllegalArgumentException` around that failure no longer catches it. The blocking stack already threw `IllegalStateException` for the same mistake before this release. See [section 5 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#a-null-returning-factory-now-fails-differently-on-reactor).
* **`WriteResult` and `DcbAppendResult` each gain a fourth component, `Optional<AppendId>`.** Every store now stamps the same `appendid` CloudEvent extension on every event a single write or DCB append call persists, and the result returns that identifier. An empty write reports no append id. This is the write-side half of [ADR 132](doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md), the identifier a later release records per projection so a wait can ask whether a projection has applied a specific append instead of asking about a position. Two things break for an external caller here already. A test that compares a whole `WriteResult` or `DcbAppendResult` with `isEqualTo(...)` starts failing on any result that persisted something, since a fresh id is minted per call and no recipe can predict the value an old assertion should expect. Compare `streamId`, `oldStreamVersion` and `newStreamVersion` (or the DCB equivalents) instead. A record pattern that still names the original three components, `case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion) -> ...` for example, stops compiling, because a record pattern has to name every component of the canonical constructor. `UpgradeToOccurrent_0_34` adds the fourth binding, `var appendId`, to both shapes for you. See [section 6 of the upgrade guide](doc/migration/upgrading-to-0.34.0.md#6-writeresult-and-dcbappendresult-gain-a-fourth-component-the-append-id). Part of [#740](https://github.com/johanhaleby/occurrent/issues/740).

### Changelog 0.33.0 (2026-08-16)

#### Highlights

* Matcher-based step conditions on the flow saga DSL. `on(allOf(...))`/`on(anyOf(...))` express an alternative or a combined count a `join` step could not, and `join` is deprecated in their favor.

#### Changes

* **A saga that declares a sealed event type now receives the concrete events stored under it, which fixes behaviour the saga DSL shipped in 0.32.0.** `startsOn(OrderEvent.class)`, `evolve` and `react` on a sealed supertype, `step.on(OrderEvent.class, ...)` and `event(OrderEvent.class, 2)` all subscribed on the CloudEvent type of the supertype itself, so the saga silently missed the concrete events it was waiting for. A sealed interface or abstract supertype has no stored event of its own, so that meant receiving nothing at all when the supertype was the only type it declared. A concrete sealed supertype is different under the class-keyed mapper Occurrent ships, which gives it a CloudEvent type distinct from every permitted subtype's. It received its own direct instances and missed every permitted subtype instead. A saga now asks for every concrete type its sealed types permit as well. Nothing changes for a saga that already declared concrete types. See [ADR 124](doc/architecture/decisions/0124-a-saga-expands-a-declared-sealed-event-type.md) and [#743](https://github.com/johanhaleby/occurrent/issues/743).
* **A subscription that declares a sealed event type now also asks for that type's own CloudEvent type, which changes behaviour `@Subscription`, `@StreamSubscription`, `@SynchronousSubscription` and `@DcbSubscription` shipped.** The filter derived from a sealed type used to name only the concrete types it permits, so an event stored under the declared type's own CloudEvent type never matched, even while every concrete type the filter did name kept arriving. That matters whenever the declared sealed type is itself concrete, an event is stored as an instance of it directly, and the mapper gives that instance a CloudEvent type none of the permitted concrete types share, true automatically under the class-keyed mapper Occurrent ships. An extra type in a filter can only widen what matches, so a subscription that only hit this gap keeps working. A subscription whose sealed hierarchy is reopened below the declared type is a different story. The same expansion now refuses it instead of silently naming only the reopened level, a breaking change for a subscription that started under 0.32.0's looser check. See [section 10 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused) and [ADR 124](doc/architecture/decisions/0124-a-saga-expands-a-declared-sealed-event-type.md).
* **A saga can now select on more than event type, with `Saga.Builder.narrowingFilter(Filter)` and `Saga.Builder.replacementFilter(Filter)` (`FlowSaga.Builder` has both, both Kotlin `saga { }` blocks expose them, and `Saga.create(...)` takes a replacement as a trailing argument).** A `narrowingFilter` is combined with the filter derived from the saga's event types, so the saga keeps asking for its own types and also requires your condition on subject, source, data or time. A `replacementFilter` is used instead of deriving one, which is how you run a saga over an event hierarchy whose concrete types cannot all be found, the way out for a `CloudEventTypeMapper` of your own that maps a whole hierarchy onto a single CloudEvent type string. Keep either wide enough for the saga's start events and for the events that move an instance on. A replacement asks two more things of you, and a narrowing on a saga that declares no event types and no replacement asks one of them, since it is the whole selector there. On a flow saga a narrowing also changes what a guard reading `received.none(...)` sees. Both are covered in [section 10 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused) and [ADR 124](doc/architecture/decisions/0124-a-saga-expands-a-declared-sealed-event-type.md). Resolves [#751](https://github.com/johanhaleby/occurrent/issues/751).
* A flow saga reaction can ask whether an event type has arrived, with `received.none(Rejected.class)` or `received.any(Rejected.class)`, instead of fetching the whole list and testing it for emptiness. Kotlin gains a reified `none<T>()` beside the `any<T>()` it already had.
* **A flow saga step can now wait on a matcher tree instead of only a single-branch choice or a `join`.** `on(StepCondition, then)` expresses an alternative ("wait for either two approvals or one rejection") or a mixed count-and-alternative that `join` could not, built from `event(type[, count][, predicate])` leaves combined with `allOf(...)`/`anyOf(...)`. A tree is a plain value, reusable across `on(...)` calls, and mixes freely with classic `on(Class, ...)` branches in one step. Kotlin gets matching `event<T>`, `allOf`/`anyOf`, and `on(condition, then)` sugar. `join` and Kotlin's `expect<T>` are deprecated in its favor, with no forced migration. An `on(StepCondition, ...)` reaction reads the events received since its step was entered, the same ones the condition counted, so a count it takes matches the count that fired it (`received.initiating(...)` still reaches the start event, while a guard and a `timeout`'s `onExpiry` still read the whole retained history). A deprecated `join`'s reaction reads that same narrowed window too, see Breaking changes below. Two `allOf` children that match the same event are refused when the saga is built, so `allOf(A.class, A.class)` asks you for `event(A.class, 2)` instead. A leaf's predicate must be a deterministic function of the event it is given, and it is run again over the step's window whenever a count cannot be read from the instance's state, which is the case for a leaf whose predicate has no name, for a declaration that changed, and for every count during a replay. See [ADR 120](doc/architecture/decisions/0120-a-step-condition-is-a-monotone-matcher-tree.md), [ADR 123](doc/architecture/decisions/0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md) and [#707](https://github.com/johanhaleby/occurrent/issues/707).
* **A flow saga can now cap the events of the step it is parked in, with `FlowSaga.Builder.stepWindow(int)` (`stepWindow(...)` in the Kotlin `saga { }` block).** `historyWindow` only ever limited the carry-over behind the current step's entry, so an instance parked in one step while a large number of correlated events arrived kept every one of them, and the 0.31.0 entry that introduced `historyWindow` was wrong to say the retained state does not grow without bound. `stepWindow` is the missing limit, unbounded by default, and set together with `historyWindow` an instance holds at most `historyWindow + 2 * stepWindow + 1` events, since a transition keeps the events of the step it left for that step's reaction while the step it enters fills its own cap. A step condition still completes on the same event, because its counts are kept in the instance's state rather than counted from the events. What reads less is a guard, a `timeout`'s `onExpiry` and a window-condition reaction, which see only the events still kept, while `received.initiating()` and the event that fired a branch stay reachable at any cap. Keeping a count means matching it back to its leaf after a redeploy, so a leaf in a capped step names its predicate, `event(Payment.class, 2, "isBig", p -> p.isBig())`, and `build()` refuses a capped step whose predicate has no name or whose two leaves share a name while holding different predicates. Change the name when the predicate's meaning changes, since a kept name with a changed test is the one thing this cannot detect. Changing what a capped step waits on while instances are parked in it makes those instances refuse their next delivery with a message naming the step. The Spring MongoDB store also warns once, the first time a parked instance's retained events cross 1,000, then re-arms after a later save drops back below that count, after the saga is deleted, or after the tracking latch evicts the id under churn, naming `stepWindow` as the way to trim them, since that count is what pushes the document toward MongoDB's 16 MB limit. See [ADR 123](doc/architecture/decisions/0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md), [Upgrading to 0.33.0](doc/migration/upgrading-to-0.33.0.md#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in) and [#741](https://github.com/johanhaleby/occurrent/issues/741).
* `CheckpointWriteVersionSource` lets `DurableSubscriptionModel`, the blocking catch-up subscription models, and `CatchupThenPushSubscriptionModel` stamp a checkpoint write `notOlderThan` the version a configured source answers, or `any()` when it answers empty or none is configured. Part of [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and [#665](https://github.com/johanhaleby/occurrent/issues/665).
* A checkpoint write refused because its lease moved to another node is no longer retried by `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel`, the subscription stays known and pausable, and a later resume redelivers the event. Part of [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **`ReactorCheckpointStorage` now retries a transient MongoDB error while reading, saving or deleting a checkpoint, instead of letting it reach the subscription.** It retries with exponential backoff by default, 100 ms up to 2 seconds, the same interval `SpringMongoCheckpointStorage` already uses on the blocking stack, but bounded to 5 attempts before rethrowing the original failure rather than retrying without limit. Pass your own `reactor.util.retry.Retry` to the new three-argument constructor to change it. Resolves [#656](https://github.com/johanhaleby/occurrent/issues/656).
* **`CompetingConsumerSubscriptionModel.pauseSubscription(id)` now works on a node that has not won the lock, not only the one delivering events.** The pause is remembered and honoured once the lock arrives, and `isPaused`/`isRunning` answer truthfully for it in the meantime. Cluster-wide pause is calling `pauseSubscription` on every node ([ADR 112](doc/architecture/decisions/0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md)). This changes behaviour shipped in 0.31.0. Resolves [#565](https://github.com/johanhaleby/occurrent/issues/565).
* **A MongoDB lease competing consumer strategy no longer lets its scheduled lease refresh and an application thread change the same consumer at the same time.** A consumer unregistered while the refresh was acquiring the lease for it could be registered again by that refresh, after `CompetingConsumerSubscriptionModel` had already forgotten it, which left the lease held and refreshed by a node with no consumer for it and no other node able to take that subscription over until the process restarted. The same window could report one change of status to a `CompetingConsumerListener` twice, or not report a real one at all. This affects `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy`, and changes behaviour shipped before 0.32.0. Both classes also drop the `synchronized` on `registerCompetingConsumer` and `unregisterCompetingConsumer`, so registering or unregistering a consumer no longer waits behind every other consumer's call on the same strategy instance. Rationale in [ADR 113](doc/architecture/decisions/0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md). Resolves [#651](https://github.com/johanhaleby/occurrent/issues/651).
* **A MongoDB lease's expiry is now judged against the database's own clock, not the asking node's.** `expiresAt` used to be written from the holder node's clock and compared against whichever node's clock was asking, so a node whose clock ran ahead of the holder's could take a healthy lease, and a node whose clock ran behind waited out the skew on top of the lease time before taking over a dead one. Both the write and the read now use MongoDB's `$$NOW`, so clock skew between nodes can no longer shorten or extend a lease. This affects `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy`, and changes behaviour shipped before 0.32.0. Their builders' `clock(Clock)` method no longer affects lease timing and is kept only for source compatibility. Rationale in [ADR 114](doc/architecture/decisions/0114-a-lease-expires-on-the-database-clock-not-the-asking-nodes.md). Resolves [#659](https://github.com/johanhaleby/occurrent/issues/659).
* **A released MongoDB competing-consumer lock now stays in the collection instead of being deleted, so its version keeps climbing instead of resetting to 0 the next time the lock is acquired.** `MongoListenerLockService.remove` unsets `subscriberId` and `expiresAt` in place rather than deleting the document. A missing `subscriberId` was already read as a free lock and a missing `expiresAt` as an expired one, so acquiring after a release behaves exactly as before. This lays the groundwork for the fencing token in [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md). Nothing reads the version as a fence yet. The lock collection must not be dropped independently of the checkpoint store, since that would reset the version underneath a checkpoint that remembers a higher one. `CheckpointStorage.delete(subscriptionId)` already clears both together, so recovery needs no new API. Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **`NativeMongoCheckpointStorage`, `SpringMongoCheckpointStorage` and `ReactorCheckpointStorage` now evaluate `notOlderThan` and `ifAbsent` for real instead of refusing them with `UnsupportedOperationException`, and `writeVersion` answers the stored version on all three.** Part of [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and [#665](https://github.com/johanhaleby/occurrent/issues/665).
* The Spring Boot starter now wires the checkpoint fence into every model it builds, so an application with a competing-consumer strategy bean gets it automatically. It refuses to start when that would pair the fence with a `CheckpointStorage` that only writes unconditionally, because the first checkpoint write a node makes while holding its lease would throw. Set `occurrent.subscription.competing-consumer.fence-checkpoints=false` to keep such a storage and write every checkpoint unconditionally. Part of [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **A competing consumer that regains its lease now resumes from the stored checkpoint, not the position it had read before losing the lease.** It no longer redelivers everything the other node already handled while it was gone, and no longer writes the checkpoint backward. At-least-once delivery is unchanged. With `EveryN(n)` checkpointing the resume can still go back up to `n - 1` events, zero with the default `everyEvent()`. See [ADR 117](doc/architecture/decisions/0117-a-resumed-competing-consumer-continues-from-the-checkpoint.md). Resolves [#668](https://github.com/johanhaleby/occurrent/issues/668).
* **A MongoDB lease competing-consumer strategy's scheduled refresh now gives up after 5 attempts per MongoDB call instead of retrying a down database forever.** The refresh runs on a single-thread scheduler that starts the next round only once the current one returns, so a round stuck retrying a store that never answered blocked every later round behind it, and a rival that should have taken the lease over by then never got the chance. Registering and unregistering a consumer keep retrying exactly as configured. This affects `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy`, and changes behaviour shipped before 0.32.0. Resolves [#691](https://github.com/johanhaleby/occurrent/issues/691).
* `ManualStartSubscriptionModel` now records a subscription's first start position with a single conditional write, so two nodes starting the same manually-started subscription at once can no longer both write, resolving [#669](https://github.com/johanhaleby/occurrent/issues/669). The write only happens for a registration that resolves to the subscription model default, which is the one a wrapped model reads a stored checkpoint for.
* **`ManualStartSubscriptionModel` now pins a subscription's first-run start position when it registers rather than when it starts, which changes behaviour shipped in 0.32.0.** The position was always captured at registration, but the write itself waited for start, so two nodes registering minutes apart during a rolling deploy could have whichever one started first win the pin regardless of which one registered first, silently skipping the events between the two registrations. The first write to reach storage now wins, so a subscription starts from where it was registered instead of from wherever it happened to be started. This decides where a subscription registered with the default start position begins, which is the one a stored checkpoint is read for. A registration that names a position of its own writes nothing, so a replay you asked for stays a replay instead of turning into a resume from the moment of registration. A dynamic start position is resolved layer by layer down the wrapped models, following what those models do to it when the subscription starts, so one that answers with nothing for the outermost layer is still recorded from the layer below. A walk that ends with nothing to record asks the layers it reached again under the classes they inherit from, so a function branching on an exact model type still recognises a subclassed model, a proxy built by subclassing one included. A layer whose own answer decides something other than where the subscription starts, as `CompetingConsumerSubscriptionModel`'s decides whether to compete, says so by answering `false` from the new `SubscriptionModelWrapper.decidesWhereTheSubscriptionStarts()` and is passed over, leaving the answer to the model that does read the checkpoint. That covers every start position Occurrent builds. A wrapper of your own whose answer does not decide where the subscription starts, whether it resolves the position for another decision or passes it down untouched, and that does not answer `false`, can still be read differently here than the model that starts the subscription reads it, and [ADR 86](doc/architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md) has what that costs. Two nodes registering the same subscription for the first time are the exception, because only one of the two positions can be stored and neither node can tell which of them is earlier. The node that cannot show the stored checkpoint holds the position it read has its registration refused with the new `StartPositionAlreadyPinnedException`, rather than starting the subscription from a position it never read and saying so only in a log. Registering again is what clears that one, since the node then finds the other position stored and starts from it, so under `occurrent.subscription.mode=manual` this is an application that fails to start when its subscription is brand new and two of its nodes read different positions at the same moment. A single node reaches the same refusal without a second one in sight when its checkpoint storage answers from a replica that has not caught up, so the exception is not proof that anybody else is registering, and that one needs a reader that has seen the write rather than another start. [Section 8 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#8-three-ways-the-spring-boot-starter-now-refuses-to-start) has what to do about it. A node that finds a checkpoint already stored before it reads its own position accepts it without a word, as before, so a node starting behind a leader election long after another has been running the subscription is unaffected. A position source that answers `null` refuses the registration too, with an `IllegalStateException`, since `null` is how a source reports a problem it cannot resolve and there is then no position to record. Such a registration used to be let through in silence and start from wherever the feed had reached once it was started. A subscription that already has a checkpoint stored is not refused, since that checkpoint is where it starts and nothing would have been recorded over it anyway, so a database that never answers stops a brand new subscription rather than every subscription the application has. `stoppedByDefault(SubscriptionModel)`, or a `StartAt` of your own, registers without recording a position at all, and neither carries the guarantee this entry describes. See [ADR 86](doc/architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md)'s amendment and [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md)'s, and [#771](https://github.com/johanhaleby/occurrent/issues/771).
* **`ReactorDurableSubscriptionModel` now records a subscription's first start position with a conditional write, and refuses a registration that loses it, which changes behaviour shipped in 0.32.0.** It read the stored position, found none, read the current position and wrote that with no condition attached, so two nodes registering a subscription for the very first time at the same moment both wrote and the second one won without anybody being told, and the events between the two positions reached neither. The write is conditional on nothing being stored now, and a node that cannot show the stored position is the one it read has its registration refused with `StartPositionAlreadyPinnedException` rather than starting from a position it never read. A position that was already stored when this model read for it is still taken without a word, so a node joining a subscription another has been running is unaffected. Unlike the blocking twin this is not limited to `occurrent.subscription.mode=manual`, because the reactive stack has no manual-start wrapper and this is its only durable model, so any reactive subscription whose start position resolves to the subscription model default can be refused on its very first run. A registration that names a position of its own records nothing and is never refused, so a replay you asked for stays a replay. Starting the node again takes whatever position storage holds by then, except when the refusal came from a reader that has not seen the write, which answers the same way every time and needs a reader that has. Where the refusal comes out depends on the wrapped model. It is thrown from `subscribe(..)` when that model manages named subscriptions of its own, and signalled on `Subscription.waitUntilStarted()` with an `ERROR` logged when this model drives the cold subscription primitive itself, where it cannot throw. A read of that position that fails is a different problem from another node having stored one first, and it too refuses the registration, in the same two places, reporting the failure it threw. A read that answers nothing refuses it as well, with an `IllegalStateException` naming the subscription, where it used to fall back to `StartAt.now()`. That fallback looked harmless for a subscription that starts at the moment it registers, and is not, because the wrapped model applies a start position when it opens its feed rather than when it receives one, so a wrapped model that is itself stopped starts from wherever the feed has reached by the time it is started. A `CheckpointStorage` that answers nothing when asked to record the position is refused for the same reason, since `save(..)` hands the checkpoint back and a storage answering nothing has shown neither that the position was recorded nor that it was not. A `CheckpointStorage` of your own that answers `false` from `evaluatesWriteConditionsFor(String)` keeps the unconditional write and gets a `WARN` saying so, since nothing here can make such a storage write conditionally. Both reactive storages Occurrent ships answer `true`. See [ADR 89](doc/architecture/decisions/0089-manual-subscription-mode-on-the-reactive-stack.md)'s last amendment and [#738](https://github.com/johanhaleby/occurrent/issues/738).
* **A framework-built materialized view now batches its store calls during a catch-up replay, reached through `CatchupProjectionFeed` and its reactor twin.** A history of N events over K projection keys used to cost one read and one write per event on that path. It now costs about one of each per key, and about two calls per batch when the repository does real bulk operations (the shipped Mongo ones below do). Batching only applies with the default `RetryStrategy.none()`. A flush built with any other retry strategy reads and writes one key at a time instead. `Projections.materializedView` (blocking) and `Projections.reactiveUpdateWithMetadata` (reactor) build such views with batching on by default, sized by `batchSize` on the new `MaterializedViewOptions`, and a batch size of `1` restores the old write-through behaviour. A view of your own opts in by implementing the new `ReplayAware` capability (a reactive twin, `ReactiveReplayAware`, lives in the reactor projection DSL). `ViewStateRepository` gains defaulted `findAllById` and `saveAll`, so an existing repository keeps working unchanged. The subscription-fed projection runners, `ProjectionRunner` and its reactor twin, stay on the old per-event cost for now, per [ADR 110](doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md). What a partly failed batch write leaves behind, and why that stays within the at-least-once replay contract, is also defined there. Resolves [#638](https://github.com/johanhaleby/occurrent/issues/638).
* **The Mongo-backed `ViewStateRepository` implementations the library ships now read and write in bulk.** The repositories built by `SpringMongoViewExtensions` and `MongoProjectionStoreProvider` override `findAllById` and `saveAll`, so a batched replay reads a whole batch with one `_id in (..)` query and writes it with as few bulk writes as `@Version`-aware optimistic locking allows. `OptimisticLockingFailureException` and `DuplicateKeyException` still reach you the same way a single `save` throws them. Resolves [#643](https://github.com/johanhaleby/occurrent/issues/643).
* **`MongoProjectionStoreProvider`'s `save` and `saveAll` now reject a `@Projection` state whose `@Id` doesn't match the projection key.** Before this, a mismatch silently wrote to the wrong document and the read model never accumulated. Both methods now throw `IllegalStateException` instead, the same guard `SpringMongoViewExtensions`'s Mongo-backed repositories already apply. This changes behaviour shipped in 0.31.0 (`saveAll` in 0.32.0). Resolves [#629](https://github.com/johanhaleby/occurrent/issues/629).
* **`@EnableOccurrentTesting` now clears more state on its own.** The extension bean it hands back applies `clearingCheckpoints(..)` automatically once the context has exactly one `CheckpointStorage` bean, and a new `clearState = true` attribute applies `clearingStateWith(..)` automatically once a store integration is available to flush with, `occurrent-testing-mongodb` plus a `MongoTemplate` bean today. Resolves [#636](https://github.com/johanhaleby/occurrent/issues/636).
* **A composed `AND` filter with several `data` payload paths no longer reparses a byte-backed event's payload once per path.** The filter is still evaluated strictly left to right, stopping at the first mismatch exactly as before, so a metadata condition ahead of a payload one still decides the result without touching the payload, and a store with no payload reader still gets a `false` rather than an exception for a filter that never reaches its `data` leaf. Once left-to-right evaluation lands on a run of two or more consecutive payload paths, that run is read in one pass instead of once per path, so a byte-backed event with several payload paths in the same run stops paying for the same parse repeatedly. A Map-backed event, the production MongoDB path, already skipped that reparse, though it now picks up a small allocation for a batched run once one needs two or more paths from it. Resolves [#623](https://github.com/johanhaleby/occurrent/issues/623).
* **A dotted path through an array of objects on a byte-backed event no longer picks up a same-named field nested inside a later sibling.** A filter on such a path could match the wrong events. Pre-existing, found and fixed as part of the filter work above.
* **`CompetingConsumerStrategy` gains a `fencingToken(subscriptionId)` default method, and `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy` now answer it.** The token increases on a genuine change of lease owner and stays unchanged when the same holder refreshes, giving a `CheckpointStorage` something to compare a stored value against and refuse a write from a lease that has moved on ([ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md)). It answers empty unless exactly one consumer is registered for the subscription and that consumer holds the lock, whatever that consumer's status otherwise is. The default method answers empty too, so an existing `CompetingConsumerStrategy` keeps compiling and simply has no fence. Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* `UpgradeToOccurrent_0_33` stubs the two new `CheckpointStorage` members for you on a class it finds missing them, marking each with a review comment, so the signature half of the upgrade compiles without a manual pass. The generated `save` delegates `any()` to the class's own two-argument write when it has one, and refuses every write instead when it does not, since delegating there would call back into `CheckpointStorage`'s own default and recurse. Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **A `CompetingConsumerStrategy` bean of your own now replaces the Spring Boot starter's default one whatever type it is, and several of them with no `@Primary` fail startup.** A strategy of any other type than `SpringMongoLeaseCompetingConsumerStrategy` used to leave the starter's default in place beside it, so the subscription model kept delivering under the starter's own lease, and the ambiguity that created was read as no strategy at all, which wrote checkpoints unconditionally and ran a `@Saga`'s timer poller on every instance. `AmbiguousCompetingConsumerStrategyException` now names the beans found and the remedy, which is to mark one `@Primary` or leave only that one in the application context. See the amendment to [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) and [#684](https://github.com/johanhaleby/occurrent/issues/684).
* **`SpringRedisCheckpointStorage` now evaluates `notOlderThan` and `ifAbsent` for real, instead of refusing them, on Redis Cluster too.** The write version lives in a second, prefixed key, separate from the checkpoint's own, so a node still on the previous release keeps reading the checkpoint with a plain `GET` through a rolling deploy. A Lua script compares the stored version and writes both keys atomically, and a refused write throws `CheckpointWriteConditionNotFulfilledException` immediately rather than being retried, since a refusal can never succeed on a later attempt. The version key carries a hash tag matching whatever the checkpoint key itself hashes on, so Cluster places both keys in the same slot instead of refusing the script for crossing them, and it also carries a SHA-256 digest of the subscription id after that tag, so two ids sharing a hash tag still get their own version key rather than sharing a fencing version. Built with either of the original two constructors, `save` refuses a `notOlderThan` or `ifAbsent` condition outright for a subscription id Cluster reduces to hashing whole while it is empty or still carries a closing brace, whether or not the deployment behind it is actually Cluster, which is what keeps `evaluatesWriteConditions()` true without exception in that mode. `any()` never refuses one, and `delete` falls back to two single-key deletes instead, which removes a version key left behind by a standalone-to-Cluster migration and is a no-op otherwise, still safe either way. `read`, `save`, `delete`, and `exists` also refuse a subscription id that starts with the version key's own reserved prefix, since a caller-chosen id equal to another subscription's version key would otherwise let a write against it corrupt that other subscription's stored version, see the breaking change below for a subscription that already has such an id. Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **`CheckpointStorage` gains `evaluatesWriteConditionsFor(subscriptionId)`, a per-id refinement of `evaluatesWriteConditions()` that defaults to it, and `SpringRedisCheckpointStorage.forStandalone(RedisOperations)` (with a `RetryStrategy` overload) builds a new mode for it.** A storage whose answer to a conditional write depends on the subscription id, rather than being the same for every one, overrides the new method instead of leaving it at the default. `SpringRedisCheckpointStorage`'s original two constructors keep today's Cluster-safe behaviour, refusing the one subscription id shape above for `notOlderThan` and `ifAbsent`. `forStandalone` is the other mode, for a deployment that is standalone or replicated rather than Cluster, where slot alignment is not a concept a server has, so a conditional write there accepts that shape too. Do not build a Cluster deployment's storage with `forStandalone`, since a conditional write for an id the standalone mode accepts but Cluster cannot align a slot for then fails with Redis's own `CROSSSLOT` error. The Spring Boot starter's fencing check now also asks `evaluatesWriteConditionsFor` for the subscription ids the new `CheckpointStorageCannotFenceSubscriptionException`'s javadoc names precisely, and throws that exception, naming the storage and every refused id, when a storage answers `true` to `evaluatesWriteConditions()` overall but `false` for one of them. See [section 8 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#8-three-ways-the-spring-boot-starter-now-refuses-to-start). Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **A catch-up subscription whose replay failed no longer reports itself as still running or catching up.** `isRunning(id)` and `isCatchingUp(id)` kept answering `true` for a subscription whose catch-up had already failed, and pausing it recorded a pause that would never be applied instead of reaching the delegate. This affects `StreamCatchupSubscriptionModel` and the DCB catch-up model, and changes behaviour shipped before 0.33.0.

#### Breaking changes

* **`ManualStartSubscriptionModel.stoppedByDefault(SubscriptionModel, GlobalCheckpointSource, CheckpointStorage)` now refuses a `CheckpointStorage` that answers `false` to `evaluatesWriteConditions()`, which changes behaviour that factory shipped in 0.32.0.** It records a subscription's first start position with `ifAbsent()`, so it needs a storage that evaluates that condition, and it throws `IllegalArgumentException` naming the storage class rather than accepting one that says it does not. `evaluatesWriteConditions()` is new in this release and defaults to `false`, so a storage of your own written against 0.32.0 answers `false` even when it evaluates `ifAbsent` correctly. A Spring Boot application running `occurrent.subscription.mode=manual` with a `CheckpointStorage` bean of its own fails to start until that is fixed, whatever `occurrent.subscription.competing-consumer.fence-checkpoints` is set to. Two remedies. Override `evaluatesWriteConditions()` to return `true` on a storage that evaluates both `notOlderThan` and `ifAbsent`, or use the one-argument `stoppedByDefault(SubscriptionModel)`, which records no position at all and lets a first run start from the moment it is started. Every storage Occurrent ships answers `true`. See [ADR 86](doc/architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md)'s amendment, [section 8 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#8-three-ways-the-spring-boot-starter-now-refuses-to-start) and [#669](https://github.com/johanhaleby/occurrent/issues/669).
* **A saga or an annotation-based subscription that declares an event type whose concrete subtypes cannot be found is refused, which changes behaviour the saga DSL shipped in 0.32.0, and for one shape, behaviour `@Subscription`, `@StreamSubscription`, `@SynchronousSubscription` and `@DcbSubscription` shipped too.** You are affected if a saga or a subscription registers an event type whose concrete subtypes Occurrent cannot enumerate, which means an interface or an abstract class that is not sealed, an array (this expansion does not support one as a declared type at all, not because of enumeration), or a sealed hierarchy, whether or not its root can be instantiated, with a level below the root that is neither sealed nor final (`non-sealed` in Java, `open` or `abstract` in Kotlin). `build()` throws `IllegalArgumentException` naming that type for a saga, where 0.32.0 built the saga and started it, and a subscription's registrar throws the same for a subscription, at startup for a Spring Boot application. A saga or a subscription that declares concrete types, or a sealed hierarchy that is sealed or final all the way down, is unaffected. Under the type mappers Occurrent ships, a saga in that position was receiving nothing, or missing part of the hierarchy, so the exception is reporting a saga that never worked rather than breaking one that did. A subscription is only partly the same story. A non-sealed interface, a non-sealed abstract class or an array already refused a subscription with an `IllegalArgumentException` in 0.32.0, under a message this release also rewords, so the only shape that is newly breaking for a subscription is a sealed hierarchy reopened below the declared type, which 0.32.0 accepted and matched only the reopened level. The one case that genuinely worked before is a `CloudEventTypeMapper` of your own that maps a whole hierarchy onto one type string, and there declaring the concrete types keeps working because they all map to the same string. Two remedies either way, seal the hierarchy or declare the concrete event types. A saga has a third, a `replacementFilter(...)`, which is the direct way out under such a mapper. A subscription has a third of its own, its `eventTypes` attribute. There is no recipe entry, because telling a refused declaration from a sealed one that now works needs the sealed modifier and the type model behind a class literal does not carry it, so the upgrade guide is the migration path. See [section 10 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused), [ADR 124](doc/architecture/decisions/0124-a-saga-expands-a-declared-sealed-event-type.md), [#743](https://github.com/johanhaleby/occurrent/issues/743) and [#755](https://github.com/johanhaleby/occurrent/issues/755).
* **A checkpoint write now states its condition, so this changes behaviour that every `CheckpointStorage` implementation ships.** The existing two-argument `save` is unchanged, still an unconditional write. `save(subscriptionId, checkpoint, CheckpointWriteCondition)` also accepts `notOlderThan(long)`, which throws `IllegalArgumentException` for a negative version and is otherwise refused once a newer version is already stored, and `ifAbsent()`, accepted only when nothing is stored yet. `writeVersion(subscriptionId)` reads back the version a condition is judged against, and `evaluatesWriteConditions()` answers whether this storage evaluates them at all, defaulted to `false` so a storage that only writes unconditionally needs to say nothing, and overridden by one that does so a caller can ask before it wires anything up. A refused write throws `CheckpointWriteConditionNotFulfilledException` on the blocking stack and signals it as a `Mono.error` on the reactive one. Every storage Occurrent ships, both Mongo storages, the reactor Mongo storage, the Redis storage, and both in-memory storages, evaluates all three conditions for real, on Redis Cluster too for the subscription ids the Redis bullet above doesn't already refuse outright. This is the foundation for the fencing token in [ADR 116](doc/architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md), and `DurableSubscriptionModel`, the blocking catch-up subscription models, and `CatchupThenPushSubscriptionModel` already write a conditional checkpoint as part of this release. If you implement `CheckpointStorage` yourself, see [Upgrading to 0.33.0](doc/migration/upgrading-to-0.33.0.md). Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **`SpringRedisCheckpointStorage`'s `read`, `save`, `delete`, and `exists` now refuse a subscription id that starts with `occurrent:checkpoint-version:`, which changes behaviour a subscription id of that exact shape had under 0.32.0.** That prefix is reserved for the version key the conditional-write support above introduced, and a subscription id starting with it could otherwise be, or alias, another subscription's version key, so a write against it could corrupt that other subscription's stored version. 0.32.0 stored every checkpoint at a single unprefixed key with no version concept at all, so an id of that shape worked there like any other. You are affected only if an existing subscription id already starts with that exact string, which nothing this library or a realistic caller produces by accident. [Section 4 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#4-redis-cluster) has the migration path, which has to run before upgrading, since `read` refuses the id afterward too and cannot hand back the checkpoint to move. Part of [#665](https://github.com/johanhaleby/occurrent/issues/665).
* **`ReplayAwareSubscriptionModel` and `IntrospectableSubscriptionModel` are renamed to `ReplayAwareSubscriptions` and `IntrospectableSubscriptions` on both stacks, and `DelegatingSubscriptionModel` is renamed to `SubscriptionModelWrapper`, so this changes behaviour that shipped.** None of the three ever extended `SubscriptionModel`. `ReplayAwareSubscriptions.findIn` is called with a plain `Subscribable` in `SagaAnnotationRegistrar`, and `IntrospectableSubscriptions.findIn` is called with a `SubscriptionModelLifeCycle` in `OccurrentSubscriptionsExtension`, so the old "SubscriptionModel" suffix claimed a relationship neither interface has. `SubscriptionModelWrapper`'s two methods are renamed with it, `getDelegatedSubscriptionModel()` to `getWrappedSubscriptionModel()` and `getDelegatedSubscriptionModelRecursively()` to `getWrappedSubscriptionModelRecursively()`, so the type and its methods share one vocabulary. The published TCK base class `IntrospectableSubscriptionModelConformance` moves with the interface it is named after, to `IntrospectableSubscriptionsConformance`. `UpgradeToOccurrent_0_33` renames all five interfaces, both methods and the TCK base class for you, in Java and Kotlin alike, and [section 5 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#5-five-subscription-capability-interfaces-are-renamed) covers doing it by hand.
* **`RepositionableSubscriptions.of`, `ReplayAwareSubscriptions.of` and `IntrospectableSubscriptions.of` are renamed to `findIn` and narrowed from `Object` to the new `SubscriptionModelCapability`, and this changes behaviour that `ReplayAwareSubscriptions.of` and `IntrospectableSubscriptions.of` shipped under their old type names in 0.32.0.** `of` is the convention Java uses for constructing a value, and `Optional.of` in particular never returns empty, but this method searches a wrapper chain and can come back empty, so `findIn` says what it actually does. Every `Subscribable`, `SubscriptionModelLifeCycle`, `SubscriptionModel` and `SubscriptionModelWrapper`, on both stacks, now extends the new marker interface `SubscriptionModelCapability` and satisfies the narrowed parameter without change, so every call this library makes keeps compiling under the new name. `UpgradeToOccurrent_0_33` renames `ReplayAwareSubscriptions.of` and `IntrospectableSubscriptions.of` to `findIn` for you, and marks a call whose argument is itself typed `Object` with a review comment, since the narrowed parameter no longer accepts that and the recipe cannot narrow the argument's declared type for you. `RepositionableSubscriptions.findIn` never shipped under the old name, so it needs no recipe entry. `SubscriptionModelCapability` also gains two default methods, `capability(Class<T>)` and `hasCapability(Class<?>)`, so a caller holding a capability's `Class` can ask for it directly instead of switching on the class to reach the matching static `findIn`. The three static `findIn` methods now delegate to `capability(..)` rather than each walking the wrapper chain itself. `dsl/subscription-dsl` (both stacks) adds reified `capability()` and `hasCapability()` extensions returning a nullable type instead of `Optional`. See [ADR 118](doc/architecture/decisions/0118-a-subscription-model-capability-marker-replaces-object-in-the-of-lookups.md) and [ADR 119](doc/architecture/decisions/0119-subscription-model-capability-gains-instance-side-lookups.md).
* **A saga timer's name is a `TimerName` rather than a `String`, so this changes behaviour the saga DSL shipped in 0.32.0.** `TimerName.parse("payment")` reads a plain name out of a string and `TimerName.of("step", "awaiting-players")` builds one inside a namespace, which encodes back to `step:awaiting-players`. `SagaTimeout` becomes `record SagaTimeout(String sagaId, TimerName timerName)` with no two-string constructor, and `SagaEffect`'s `StartTimeout`, `StartTimeoutAt` and `CancelTimeout` carry a `TimerName` too. `startTimeout`, `startTimeoutAt`, `cancelTimeout`, `evolveOnTimeout` and `reactOnTimeout` all keep their string forms, which read the string through `TimerName.parse`, so a saga that only calls those compiles unchanged and its timers keep firing under the same names. `SagaInput.timeout(sagaId, timerName)` fires a timer without building a `SagaTimeout` first. `FlowSaga.stepTimer("awaiting-players")` gives you the name of the timer a flow step arms, so a test fires that timer and asserts on its effects without writing the namespace itself. Kotlin gets every overload the Java API gained, plus a top-level `stepTimer` next to `saga { }`. Nothing on disk changes, since a timer is still stored under the string it always was, and an instance with a pending timer keeps working across the upgrade. If you read `timerName()` into a `String`, build a `SagaTimeout` from two strings, construct `StartTimeout`, `StartTimeoutAt` or `CancelTimeout` directly with a string name, or match a timer effect against a `String` component, `UpgradeToOccurrent_0_33` wraps a string handed to one of those constructors in `TimerName.parse` and appends `encode()` to a read it can type, and marks the rest for you, see [section 7 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#7-a-saga-timers-name-is-a-timername). See [ADR 121](doc/architecture/decisions/0121-a-saga-timers-name-carries-its-namespace.md) and [#716](https://github.com/johanhaleby/occurrent/issues/716).
* **A lowered `join`'s reaction now reads the window that fulfilled it instead of the whole retained history, so this changes behaviour that shipped in 0.31.0.** [ADR 120](doc/architecture/decisions/0120-a-step-condition-is-a-monotone-matcher-tree.md)'s 2026-08-11 amendment had exempted a lowered `join` from the narrowing `on(StepCondition, ...)` got, so the two callbacks read different things by design. That exemption is reversed, in the first step as much as any later one. A `join` past the first step no longer sees an earlier step's events, matching type or not, so even a repeat of one of its own expectation types drops out. A `join` in the first step keeps whatever a `stepWindow` cap has left of its own events, but loses the initiating event from `count`, `all`, `first`, `any`, `none` and `asList`, since a reaction's window always starts after it. A `stepWindow` cap narrows the reaction's own window the same way it already did for `on(StepCondition, ...)`, while the condition still completes on the same event either way. `received.initiating(...)` still reaches the start event regardless. `join`'s own deprecation javadoc said none of this could happen, that lowering it to `on(allOf(...))` changed nothing about what the callback sees, which was false and is corrected too. See [section 11 of the upgrade guide](doc/migration/upgrading-to-0.33.0.md#11-a-lowered-joins-reaction-now-reads-its-own-window-not-the-whole-retained-history), [ADR 125](doc/architecture/decisions/0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md) and [#707](https://github.com/johanhaleby/occurrent/issues/707).

### 0.32.0 (2026-08-08)

#### Highlights

* A published TCK. An implementation of Occurrent's event store or subscription contracts can now run the same conformance suites Occurrent runs against itself.
* Subscriptions can filter on the event payload. `Filter.data("amount", eq(42))` works on every subscription model Occurrent ships, and in-memory as well as on MongoDB.
* Push-fed sagas and projections. `@Saga` and `@Projection` can be fed from RabbitMQ, Kafka, or any broker through a push subscription model or a domain event feed, with catch-up and background replay.
* Manual subscription mode. `occurrent.subscription.mode=manual` registers everything and starts nothing, for leader election and staged startup.
* Published testing artifacts. `occurrent-testing-junit-jupiter-blocking`, its reactor twin, and `occurrent-testing-mongodb` keep subscriptions, checkpoints, and MongoDB state clean between tests.

Each has a full entry below, and the breaking changes at the end all link their migration paths.

#### Changes

* **Blocking `CatchupThenPushSubscriptionModel` now implements `IntrospectableSubscriptionModel`, matching its reactor twin.** `subscriptionIds()` used to answer empty for it, so `IntrospectableSubscriptionModel.of(..)` never found it and the JUnit extension's `startAll()` and its diagnostics missed it. It now delegates to the live feed, the same as the reactor side, and gained the same introspection conformance test. Resolves [#590](https://github.com/johanhaleby/occurrent/issues/590).
* **`SpringMongoSubscriptionModel` and `NativeMongoSubscriptionModel` `stop()`/`start()` now snapshot the subscriptions they move instead of walking the live map**, closing the same undefined self-mutation the reactor models were cured of in [#509](https://github.com/johanhaleby/occurrent/issues/509). Resolves [#590](https://github.com/johanhaleby/occurrent/issues/590).
* **`CommandDispatcher` gains a `ForwardingCommandDispatcher` base class for decorators.** A decorator that overrides `dispatch` and delegates silently turns a delegate's atomic `dispatchAll` back into one call per command, reintroducing the partial-progress hazard [ADR 76](doc/architecture/decisions/0076-batch-command-dispatch-seam.md) removed. Extending `ForwardingCommandDispatcher` forwards both methods, so overriding one leaves the other correctly delegated. Resolves [#590](https://github.com/johanhaleby/occurrent/issues/590).
* **`ReactiveHandover`'s live buffer no longer pre-allocates its full capacity at construction.** The 100k default used to cost roughly 800 KB per subscription up front, held for the handover's whole lifetime. Memory now tracks actual use, with the same cap and the same rejection past it. Resolves [#590](https://github.com/johanhaleby/occurrent/issues/590).
* Added a published TCK. An implementation of Occurrent's event store or subscription contracts can now run the same conformance suites Occurrent runs against itself, so verifying a new store is a few lines of test code instead of a few thousand. Five artifacts ship, `occurrent-tck-common`, `occurrent-tck-eventstore-blocking`, `occurrent-tck-eventstore-reactor`, `occurrent-tck-subscription-blocking` and `occurrent-tck-subscription-reactor`. You supply a fixture (`EventStoreFixture`, `SubscriptionModelFixture`, `CheckpointStorageFixture` or `CompetingConsumerStrategyFixture`) that hands back the thing under test and declares the capabilities it was built with, then extend one suite per capability. Where implementations genuinely differ, the fixture declares which way yours goes and the suite asserts the documented outcome for that answer, rather than the assertion being loosened until everyone passes. The suites never skip a test, so declining a capability means not extending its suite, visible in the code. Occurrent's own four event stores, five blocking subscription models, four checkpoint storages and two competing consumer strategies all run them (the reactor models through test-only blocking bridges), which is the first time the build proves they agree, and running them found real bugs, fixed and listed separately below. Two things to know before depending on the TCK. The suites live under `src/main`, so JUnit and AssertJ arrive at compile scope. And **upgrading it to a new minor release can turn a green build red**, because a minor may add suites and tighten what the existing ones assert, so fix the implementation or stay on the Occurrent version you were on. Your fixture keeps compiling either way, since a new fixture member always arrives with a `default`, and `SubscriptionModelFixture.deliveryTimeout()` and its reactive twin widen the delivery wait for a model that has to reach a broker before it can deliver. The blocking artifact's `package-info` covers which suite to extend for which capability, every declaration a fixture makes with what each answer obliges, and what a version of the TCK promises. As a side effect `SynchronousSubscriptionModel` and `PushSubscriptionModel` are now declared `SubscriptionModel`s, so they can be passed anywhere one is expected, and `NativeMongoSubscriptionModel` refuses a subscription filter it cannot apply when you subscribe, instead of failing later on a background thread. Part of [#393](https://github.com/johanhaleby/occurrent/issues/393), and it resolves the older [#75](https://github.com/johanhaleby/occurrent/issues/75). Rationale in [ADR 77](doc/architecture/decisions/0077-a-published-tck-for-occurrent-contracts.md), in [ADR 88](doc/architecture/decisions/0088-what-a-dcb-append-condition-guarantees.md) for the DCB half, in [ADR 93](doc/architecture/decisions/0093-a-missing-capability-is-refused-and-a-reactive-publisher-is-cold.md) for the capability guards and the reactive contract, in [ADR 94](doc/architecture/decisions/0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md) for the subscription side, and in [ADR 107](doc/architecture/decisions/0107-what-a-tck-version-promises.md) for what a version of it promises.
* A subscription can now filter on a field inside an event's `data` payload, on every subscription model Occurrent ships. `Filter.data("amount", eq(42))` used to throw on the first delivered event. A synchronous, push, or in-memory model reads the payload through a `DataFieldReader` you supply, so on Spring Boot add `occurrent-common-inmemory-filter-matching-jackson` and the starter contributes one, or define your own bean. A model built without one refuses a payload filter at subscribe time with an `UnsupportedOperationException` naming the artifact to add. A catch-up subscription model needs nothing, since it trusts the store for payload conditions and keeps checking attributes and extensions itself. One behaviour change worth knowing, for a subscription model you wrote yourself, is that a catch-up wrapper over a model that ignores a payload filter now over-delivers instead of throwing. Resolves [#499](https://github.com/johanhaleby/occurrent/issues/499) and [#582](https://github.com/johanhaleby/occurrent/issues/582). Rationale in [ADR 92](doc/architecture/decisions/0092-a-subscription-can-filter-on-a-payload-field.md).
* `@Saga` can now be fed by a push subscription model instead of the event store, with `source = PUSH`, so a saga can react to events arriving from RabbitMQ, Kafka or an HTTP listener without wiring a `SagaRunner` by hand. It mirrors `@Projection(source = PUSH)`, with two deliberate differences. Only a `PushSubscriptionModel` is accepted, not a `DomainEventFeed`, because a domain event feed carries none of the metadata a saga recognises a redelivery by. And `catchup = NONE` opts out of the replay entirely, for a saga fed by another application's broker whose events are not in this application's store. `startAt`, `startAtGlobalPosition` and `resumeBehavior` are refused on a push saga rather than silently ignored, and `startupMode` works under the default catch-up. The new `ReplayAwareSubscriptionModel` capability came out of this and is useful on its own. `isCatchingUp(id)` reports whether a subscription is still replaying history, which `isRunning(id)` cannot answer because it is true throughout the replay, and every catch-up model on both stacks implements it. A saga's timers wait for that handover, so a timeout cannot decide against half-replayed state. Resolves [#349](https://github.com/johanhaleby/occurrent/issues/349). Rationale in [ADR 96](doc/architecture/decisions/0096-a-push-fed-saga-may-have-no-history-to-replay.md).
* `occurrent.subscription.mode` decides whether Occurrent starts your subscriptions for you. `auto` is the default and behaves as before. `manual` creates and registers everything and starts nothing until you call `resumeSubscription("someId")` or `start()`, for leader election, staged startup, or a test that picks which subscriptions run. `disabled` creates no subscription beans at all. A waiting subscription registered with the default start position starts from where it was registered rather than from where it was started, so it does not skip what was written while it waited, unless two nodes register the same subscription for the very first time, where only one of their two positions can be stored. Push-fed projections and sagas are withheld through `ManualStartPushSources`, one registry per stack, so bringing everything up behind a leader election is one `startAll()`, and `ManualStartSubscriptionModel` gives you the same thing without Spring. The reactive stack has all of it too, returning `Mono`s you subscribe to. The deprecated `occurrent.subscription.enabled` still works, fails at startup if it contradicts `mode`, and the `org.occurrent.UpgradeToOccurrent_0_32` OpenRewrite recipe rewrites it for you, in `.properties` and `.yaml` alike, with the [upgrade guide](doc/migration/upgrading-to-0.32.0.md) listing what it cannot safely touch. Combining `@EnableOccurrent` and `@EnableOccurrentReactive` on one classpath, which [ADR 44](doc/architecture/decisions/0044-reactive-spring-boot-starter.md) says should work, now actually does. Resolves [#481](https://github.com/johanhaleby/occurrent/issues/481). [ADR 86](doc/architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md) explains why registering and starting became separate events, and [ADR 89](doc/architecture/decisions/0089-manual-subscription-mode-on-the-reactive-stack.md) the reactive side.
* New `occurrent-testing-junit-jupiter-blocking` and `occurrent-testing-junit-jupiter-reactor` artifacts give you a JUnit 5 extension that keeps every subscription stopped while a test runs, so a test only runs the subscriptions it names and adding a subscription to your application can no longer change what an existing test asserts. `OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel)` stops them before and after each test, `start("someId")` resumes one and waits until it has actually started, `alwaysStart("someId")` covers a whole test class, and `startAll()` starts everything. Naming an id that does not exist fails with the ids that do, and the start wait defaults to 30 seconds on both stacks, widened with `withStartTimeout(Duration)`, failing with the id that never started rather than hanging the run. Each leaf depends on nothing but JUnit and its own subscription API, so it works without Spring and without a container, and `@EnableOccurrentTesting` wires whichever leaf, or leaves, you added. [ADR 82](doc/architecture/decisions/0082-a-published-testing-module-for-application-authors.md) explains why this is published while `test-support` stays internal, and [ADR 99](doc/architecture/decisions/0099-a-reactive-testing-twin-that-stops-every-model.md) the reactive twin. Resolves [#530](https://github.com/johanhaleby/occurrent/issues/530).
* A new `occurrent-testing-mongodb` artifact empties a MongoDB database between tests without breaking your subscriptions. `OccurrentMongoFlush.everyCollectionIn(database)` deletes the documents and leaves the collections and their indexes alone. That matters because dropping a collection invalidates every live change stream, and because a drop also removes the unique indexes an Occurrent store only creates in its constructor, which a cached Spring test context never re-runs, so your optimistic-concurrency tests keep passing with no index behind them. It names no collections on purpose, since Occurrent writes to more of them than you are likely to remember. `collectionsIn(database, ..)` narrows it, `except(..)` keeps one, a flush that fails throws instead of leaving the previous test's data in place, and `droppingTheDatabaseIn(database)` exists for the one case deleting cannot serve, a test asserting that a collection or an index does not exist.
* A `@Projection(source = PUSH)` can now keep its catch-up replay off the startup path with `startupMode = BACKGROUND`, on both stacks, so an application with a large history starts while the read model is still filling. The default is unchanged. Because nobody waits for a background replay, both starters contribute a `PushCatchupStatus` bean answering per id with one of `CatchingUp`, `Live`, `NotStarted`, `Failed` carrying the cause, or `Unknown`, so a readiness probe can tell a read model that is still filling from one that is ready to serve. A saga fed by `@Saga(source = PUSH)` is covered too, a failure is still logged at `ERROR` either way, and registering the same id twice is refused rather than silently replacing the first. [ADR 91](doc/architecture/decisions/0091-a-push-catch-up-replays-off-the-startup-path.md) explains the design.
* **`SpringMongoEventStore.readInPositionOrder` no longer loads the whole matched history into memory before delivering the first event.** It now reads through a live server cursor one batch at a time, matching the native, reactor and in-memory stores. The returned `Stream` must now be closed, which `PositionOrderedReader.readInPositionOrder` and its `DomainEventQueries` DSL wrapper document, and both push catch-up callers already do. Resolves [#586](https://github.com/johanhaleby/occurrent/issues/586).
* **A conformance-suite failure now names the test class that ran it.** Two implementations in one module used to report the same nested test name, so one failing and one passing looked like a single flaky, retried test. An assertion failure out of a suite now starts with `Run by <YourConformanceTest>.` and keeps its description and values. Resolves [#575](https://github.com/johanhaleby/occurrent/issues/575).
* **`subscriptionIds()` on the blocking MongoDB subscription models can no longer omit a subscription that is mid-pause or mid-shutdown.** It now takes the same lock as the mutators, matching the reactor models, and `RegisteringSubscribable` on both stacks got the same treatment for its milder version of the gap. Resolves [#536](https://github.com/johanhaleby/occurrent/issues/536).
* **Declaring your own asynchronous subscription model on either MongoDB starter no longer fails the application context.** It used to leave two `Subscribable` beans in the context with neither marked `@Primary`, so the context failed to start with `expected single matching bean but found 2` unless you also marked your own model `@Primary`. Both starters now resolve your model without that, for the subscription DSLs, a non-push `@Saga`, and the default (non-push, asynchronous) `@Projection` alike. Resolves [#541](https://github.com/johanhaleby/occurrent/issues/541) and [#563](https://github.com/johanhaleby/occurrent/issues/563).
* **A paused MongoDB subscription no longer loses what was written while it was paused.** `SpringMongoSubscriptionModel` used to rebuild its change stream from the `StartAt` the subscription was created with, which for the default resolves to the present, so anything written during a pause never arrived. It now resumes from the position it had read to, like the other two MongoDB models, and a subscription restarting after a change-stream error uses that position too. One whose change-stream history is gone still restarts at the present, and still only if you asked for that with `restartSubscriptionsOnChangeStreamHistoryLost`. **The consequence to plan for is redelivery.** Resuming this way is at least once, so handlers have to cope with the same event arriving twice, and `stop()` followed by `start()` is the same case. Found by the subscription TCK. Resolves [#522](https://github.com/johanhaleby/occurrent/issues/522). Reasoning in [ADR 94](doc/architecture/decisions/0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md).
* **`ReactorMongoSubscriptionModel.stop()` and `start()` snapshot the subscriptions they move instead of walking the live map**, which could silently leave a subscription running after `stop()` or paused after `start()`. The subscription TCK gained a test holding every wired model to it. Resolves [#509](https://github.com/johanhaleby/occurrent/issues/509).
* **`ReactorMongoSubscriptionModel` refuses an unsupported `SubscriptionFilter` when `subscribe(..)` is called**, instead of accepting it and failing later inside the deferred change-stream pipeline where nobody was listening. The plain `FluxSubscriptionModel` primitive still delivers its failure through the `Flux`, as a cold publisher should.
* **A reactor MongoDB subscription now retries a failing action instead of dying of it.** One bad delivery used to end a named subscription for good, since the retry only guarded the change stream underneath. The action is now retried with the model's configured backoff, the reactor counterpart of the blocking models' `RetryStrategy`, so a handler that fails deterministically keeps its subscription alive and retrying with a `WARN` per attempt, where it previously died silently. Found by running the subscription TCK over the reactor stack.
* **A durable reactor subscription hands the subscription to the model it wraps, when that model manages named subscriptions itself**, so filter validation and the action retry are inherited from the model doing the retrying instead of re-implemented in the wrapper. The three reactor catch-up models now manage named subscriptions themselves, so the starter's `Durable(Catchup(Mongo))` composition inherits both the same way. One composition changes loudly rather than silently, a durable model over a catch-up model whose own wrapped model offers only the cold primitive now refuses with an `IllegalStateException` naming the remediation, and the migration guide covers it. Resolves [#547](https://github.com/johanhaleby/occurrent/issues/547) and [#550](https://github.com/johanhaleby/occurrent/issues/550).
* **A subscription started through `ReactorCatchupSubscriptionModel` now reports that model's own type to a `StartAt.dynamic(..)`**, instead of whichever mode-specific catch-up model it routed the subscription to, so `context.hasSubscriptionModelType(ReactorCatchupSubscriptionModel.class)` matches the way it always has on the blocking twin. Resolves [#557](https://github.com/johanhaleby/occurrent/issues/557). Reasoning in [ADR 103](doc/architecture/decisions/0103-a-catch-up-dispatcher-answers-as-the-model-the-caller-holds.md).
* `@Projection(source = PUSH)` can now set `catchup = NONE`, the same opt-out `@Saga(source = PUSH)` already has. It takes live events only and touches no event store, which is what a projection fed by another application's broker needs, since the local store holds none of those events. A missing `PositionOrderedReader` or `CheckpointStorage` bean under the default now names `catchup = NONE` as the fix instead of a bare missing-bean error. Resolves [#528](https://github.com/johanhaleby/occurrent/issues/528). Rationale in [ADR 100](doc/architecture/decisions/0100-a-push-fed-projection-may-also-have-no-history-to-replay.md).
* `CatchupProjectionFeed.goLive()` and `DomainEventFeed.goLive(id)` skip the one-time catch-up and start delivering live events directly, on both stacks. Use this for a feed whose events are not in the local event store, so there is nothing there to replay. The existing `catchUp()`/`catchUpAll()` would either find nothing or, worse, replay unrelated history that happens to live in the same store. No completion marker is written, so a later real catch-up still replays the full history.
* **The reactive Spring Boot starter keeps its durable subscription model when you declare a synchronous or push model of your own.** Declaring a `SynchronousSubscriptionModel` or a `PushSubscriptionModel` bean used to remove the `@Primary ReactorDurableSubscriptionModel` without saying anything, and every asynchronous subscription in the application stopped existing with it. Neither model reads an event store, so neither replaces the asynchronous one, and the starter no longer steps aside for them. Resolves [#535](https://github.com/johanhaleby/occurrent/issues/535).
* **A released competing consumer no longer reports that it still holds the lock.** `hasLock` used to answer yes for up to half the lease time after `releaseCompetingConsumer`, and `SagaRunner` asks on every timer tick, which is where this would have reached you. `unregisterCompetingConsumer` and `releaseCompetingConsumer` also carried word-for-word identical javadoc and now describe what separates them. Unregistering keeps the consumer out until you register it again, which is what you want for a subscription a user paused. Releasing keeps it registered so it may take the lock back on its own, which is what you want for one the system paused. Found by writing `CompetingConsumerStrategyConformance`. Resolves [#516](https://github.com/johanhaleby/occurrent/issues/516).
* **A MongoDB subscription now refuses a start position it cannot apply, instead of accepting it and never delivering.** A `Checkpoint` whose stored string cannot be parsed used to fail on a background thread and be retried forever, with `waitUntilStarted(..)` never answering. `NativeMongoSubscriptionModel` and `ReactorMongoSubscriptionModel` now refuse it from `subscribe(..)` itself, as `SpringMongoSubscriptionModel` always did. A dynamic start position is still resolved when the model asks for it, because resolving one means calling your own function. Found by the subscription TCK.
* **A reactive subscription model can now list the subscriptions it knows about.** `IntrospectableSubscriptionModel.subscriptionIds()` in `occurrent-subscription-api-reactor` answers with every subscription id a model holds, running or paused, so a test or an admin endpoint can name the ids that exist rather than repeating the one it was given. `ReactorMongoSubscriptionModel`, `ReactorDurableSubscriptionModel`, `CatchupThenPushSubscriptionModel` and the reactive push and synchronous models implement it, and the blocking stack has had the same interface for a while.
* **An in-memory checkpoint storage for the reactive stack.** `org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage` in `occurrent-subscription-inmemory` is the reactive twin of the blocking `InMemoryCheckpointStorage` published earlier in this release. Checkpoints live in a `ConcurrentHashMap` with nothing to connect to, a good fit for tests and for small applications whose checkpoints need not survive a restart. Its publishers are cold, so nothing is read, saved or deleted until you subscribe, and arguments are validated when you call. The artifact declares the reactive subscription API as an optional dependency, so a blocking-only application carries nothing new.
* `OccurrentSubscriptionsExtension` gained pieces that compose with the above, all working with any store rather than only MongoDB. `clearingCheckpoints(checkpointStorage)` deletes every known subscription's checkpoint before each test, so a subscription cannot resume from where an earlier test left it. `clearingCheckpointsFor(checkpointStorage, ids...)` names checkpoints to clear for subscriptions no model reports yet, and `clearingStateWith(runnable)` runs whatever you give it, a database flush say, after every subscription is stopped and before any is resumed. Together a test class needs one `@RegisterExtension` rather than two ordered with `@Order`. Resolves [#483](https://github.com/johanhaleby/occurrent/issues/483). Rationale in [ADR 95](doc/architecture/decisions/0095-a-published-testing-leaf-for-mongodb.md).
* `@Saga` now honours `startupMode`, which it previously accepted and ignored. A saga that replays history from the beginning starts in the background by default, the same as `@Subscription` and `@Projection`. **This changes behaviour even if you never set `startupMode`.** A `@Saga(startAt = BEGINNING)` used to block startup until its replay finished and now does not, and `startupMode = WAIT_UNTIL_STARTED` gets the old behaviour back. A saga starting in the background does not fire timers until its replay is done, so it cannot issue commands against state it has not finished rebuilding, and `SagaRunner.run(...)` gained a `waitUntilStarted` parameter for a caller driving the runner directly.
* `CatchupThenPushSubscriptionModel` can be stopped, started, paused and cancelled like any other subscription model on both stacks, and stopping it now stops a catch-up replay that is already running rather than only the live feed behind it. [ADR 85](doc/architecture/decisions/0085-every-subscription-model-can-be-stopped.md) gave the four register-only models a life cycle but left the two catch-up wrappers out, and this closes it for both push ones. `DomainEventFeed` and `CatchupProjectionFeed` gained `stopCatchUp()` for the same reason. Stopping is reversible, so starting the model again replays the history from the beginning for any catch-up the stop interrupted, and `start(false)` leaves those replays for `resumeSubscription(..)` to pick up one at a time. Closing a Spring context now stops these replays and waits for them to unwind, on both stacks, which it previously had no way to do at all on the reactive one.
* A reactive subscription registered while its subscription model is stopped, and asking for the subscription model default start position, no longer misses what was written before you start it. `ReactorDurableSubscriptionModel` reads the current position when the subscription is registered and stores it when the subscription starts, so stopping the model, registering a subscription and starting it later withholds events rather than losing them. Before this, the position was only decided when the subscription started, so a subscription running for the first time began wherever the feed had reached by then. One that has run before was never affected, since it resumes from its own stored position, and nothing is written until a subscription actually starts, so one you register and never start still leaves nothing behind. A read that fails at registration, and one that answers nothing, both refuse the subscription instead of being read again when it starts, since a read taken then answers with wherever the feed has reached by then and starting from that is the loss this entry is about. Where the refusal comes out depends on the wrapped model, as it does for the conditional write above. It is thrown from `subscribe(..)` when that model manages named subscriptions of its own, which is your own call and needs no log to reach you. When this model drives the cold subscription primitive itself it cannot throw there, so it is logged at `ERROR` and signalled on `Subscription.waitUntilStarted()`, on the handle `resumeSubscription(..)` hands back, and on the handle you got when you registered as well once that registration asked for the subscription model default and storage has confirmed it holds nothing. A storage that cannot be read leaves that handle waiting rather than reporting a refusal the start may not make. A dynamic start position is read for too, since it may answer the model default when the subscription starts, but its registration handle keeps waiting rather than reporting a refusal that its own answer may avoid. Starting it is what drops it from the model, so getting it back means registering it again rather than resuming, and one you never started keeps its id until `cancelSubscription(..)` releases it. `start(true)` starts the other subscriptions all the same. A registration naming its own `StartAt` is not read for at all, `StartAt.now()` included, since this model records no position for it and you have said where to begin, and one that already has a checkpoint stored begins from that checkpoint and starts even when the read could not answer. The blocking stack never had this problem, because its models register with the feed even while stopped rather than skipping registration entirely. [ADR 86](doc/architecture/decisions/0086-a-manual-subscription-is-registered-not-started.md) explains the guarantee this brings the reactive stack in line with.
* An event that has been through CloudEvents JSON can be read again. The CloudEvents SDK writes any extension number that is not an `Integer` as a JSON string, so an event forwarded to a broker and rebuilt on the listener side comes back with `streamversion` as a string even though Occurrent wrote a `long`. `OccurrentExtensionGetter.getStreamVersion` required exactly a `Long` and threw otherwise, and `EventMetadata.getStreamVersion()` did a plain cast, so a subscription fed that way failed on every event. Both now accept a `Number` or a `String`, the way the two position accessors already did. Nothing that worked before stops working, since this only relaxes a check.
* A saga refuses an event that carries neither a stream id with a stream version nor a position, rather than reacting to it. Without one of those it cannot tell a redelivered event from a new one, so every redelivery would run the reaction again and issue its commands again. The refusal reaches the feed, so the event is not acknowledged and the listener that dropped the metadata is what you find rather than a duplicate command. Set `redeliveryDetection = BEST_EFFORT` on the `@Saga`, or `SagaRunnerConfig.withRedeliveryDetection(..)` if you drive `SagaRunner` yourself, to take those events anyway with one warning, which is what a feed carrying another application's events needs when every command the saga issues is safe to receive more than once. Occurrent's own stored events always carry the metadata, so an event-store saga and the catch-up in front of a push feed never reach this. Rationale in [ADR 109](doc/architecture/decisions/0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md).
* The in-memory event store and the in-memory subscription model can now filter on a field inside an event's `data` payload, which previously threw and worked only on MongoDB. That mattered most in tests, where a `Filter.data(..)` that worked in production failed locally. Add `occurrent-common-inmemory-filter-matching-jackson` and build the store with `new InMemoryEventStore().withDataFieldReader(new JacksonDataFieldReader())`, or pass the same reader to `InMemorySubscriptionModel`. A store built without one refuses a data filter with an `UnsupportedOperationException`, rather than quietly matching nothing. The path rules are MongoDB's, measured rather than assumed, and the conformance suite now holds every store to them. A dotted path reaches into nested objects and into each element of an array of objects, a plain array field matches when any element does, and a number stays a number, so `eq("42")` does not match a stored `42`. Anything beyond dot notation is unsupported on purpose, since MongoDB cannot answer it either. `DataFieldReader` also gained a `readAll(CloudEvent, Collection<String>)` default method, resolving every path in one traversal, and the default just loops over `read(..)` so no existing implementation breaks. Resolves [#58](https://github.com/johanhaleby/occurrent/issues/58), [#582](https://github.com/johanhaleby/occurrent/issues/582) and [#587](https://github.com/johanhaleby/occurrent/issues/587). Rationale in [ADR 87](doc/architecture/decisions/0087-a-seam-for-reading-a-payload-in-memory.md).
* A range filter on a numeric CloudEvent extension no longer fails against the in-memory store. Comparing a value to an operand of a different numeric type, as in `Filter.filter("streamversion", gt(5))` where the stored value is a `Long`, threw `ClassCastException` instead of comparing the two. Numbers are now compared by value regardless of their Java type for every operator `Filter` exposes, not only the range ones, so `eq`, `ne` and `in` agree with `gt`/`lt`/`gte`/`lte` on whether a stored `Long` and an operand built from an `int` literal are the same number, on an attribute, an extension, or a `data` payload field alike. A comparison between genuinely different types, such as a number and a string, matches nothing rather than failing, which is what MongoDB does. Resolves [#582](https://github.com/johanhaleby/occurrent/issues/582).
* `SpringMongoSubscriptionModelConfig.autoStartup(false)` gives you a subscription model that is created stopped, so a subscription registered on it is paused from the outset and no change stream opens until you call `start()` or `resumeSubscription(id)`. Use it to bring subscriptions up under your own control, behind a leader election or a health check, or in a test that picks which ones run. It also decides what `isAutoStartup()` reports, so a model you register as a Spring bean is not started for you either, which it previously was regardless of how you configured it. The default is unchanged.
* The synchronous and push subscription models can now be stopped, started and paused like every other subscription model, on both the blocking and reactive stacks. Before this they had no life cycle at all, so a test that stopped every subscription still had its synchronous projections running, and `startAll()` skipped them. Note what stopping means for these models. An event handed to a stopped model, or to a paused subscription, is dropped rather than held, and resuming does not replay it. That matters most for a synchronous projection, since the write still succeeds while the projection does not run. `shutdown()` is the one that is not reversible. It drops every registration and releases the ids, so a shut-down model delivers nothing even after `start()`. [ADR 85](doc/architecture/decisions/0085-every-subscription-model-can-be-stopped.md) explains why this is the right trade and why the previous decision not to have a life cycle did not hold.
* `SubscriptionModelLifeCycle.stop()` now documents that it leaves every running subscription paused, so you can resume one on its own with `resumeSubscription(id)` afterwards without starting the rest, and that doing so reopens `isRunning()` for the model as a whole even though every other subscription `stop()` paused stays paused, individually, until it too is resumed or `start()` is called. Every subscription model that owns a single running/stopped flag already behaved this way on both stacks, but nothing said so, which made it unsafe to build on. `ManualStartSubscriptionModel` answers the same way, even though it withholds registrations on top of the model it wraps. Resuming one subscription after `stop()` makes `isRunning()` report `true` again, and a subscription registered after that is started rather than withheld. A model you have never started is unaffected, since resuming one subscription there starts only that one. `SubscriptionModelConformance` now checks the `isRunning()` half the same way it already checked the paused-and-individually-resumable half. Resolves [#523](https://github.com/johanhaleby/occurrent/issues/523).
* A subscription model can now tell you which subscriptions it has. `IntrospectableSubscriptionModel.subscriptionIds()` returns every id it knows, running or paused, and `IntrospectableSubscriptionModel.of(subscriptionModel)` finds it behind any number of wrapping models so you do not have to unwrap by hand. The in-memory, Spring MongoDB and native MongoDB models implement it, and the competing consumer model reports its own consumers too, including one still waiting for the lock that its delegate has not been told about yet. Before this, `isRunning(id)` and `isPaused(id)` could answer for an id you already had, and nothing could list them. [ADR 83](doc/architecture/decisions/0083-a-subscription-model-reports-the-subscriptions-it-knows.md) explains why this is a separate capability rather than a method on `SubscriptionModelLifeCycle`.
* `CatchupSubscriptionModel.stop()` now stops a catch-up replay that is already in flight. Before this it only stopped the live subscription model behind it, so a replay kept reading history and delivering events after `stop()` returned. `isRunning()`, `isRunning(String)` and `isPaused(String)` now also account for a replay in progress, which the live model cannot report because the subscription is not registered with it until the replay hands over.
* The Kotlin `queries.project(projection)` fold on the reactor stack now works with a projection whose state can be `null`, completing the returned `Mono` empty when a fold produces one. It also applies each event as the query emits it, instead of reading every selected event into a list first, which matters because the on-demand fold is the path that reads history rather than a live tail. Both were already true of `Projections.project`, the Java entry point for the same fold on either stack, and the Kotlin extensions now call it instead of repeating it. Resolves [#453](https://github.com/johanhaleby/occurrent/issues/453).
* `InMemorySubscriptionModel.waitUntilAllEventsProcessed()` waits until the model has handled every event given to it, so a test can write events and then check the read model with an ordinary assertion. Before this a test had to keep retrying the assertion until it passed or a timeout ran out, because events are handled on a background thread. It throws `IllegalStateException`, naming the subscriptions still busy, if the timeout expires or the wait is interrupted, so the usual "wait, then assert" test cannot silently fall through a dropped timeout into the following assertion. It takes a `Duration` if 10 seconds is the wrong budget. Only the in-memory model has this. A MongoDB change stream never reaches a point where everything written has arrived, so a test against a real database still needs to poll. [ADR 78](doc/architecture/decisions/0078-deterministic-in-memory-subscription-drain.md) explains the choice, including why waiting for a global position would have been wrong. Resolves [#451](https://github.com/johanhaleby/occurrent/issues/451).
* A saga can now issue the domain function itself instead of a command object, so a domain model built from plain functions needs no command types and no `handle(events, command)` switch. `Invocation<E>` in `occurrent-command-dispatch` pairs a stream id with the function to run against that stream, and `CommandDispatchers.invocation(applicationService)` dispatches it. In Kotlin every core and flow reaction gets `issue(streamId) { events -> ship(events) }`. `DcbInvocation` and `DcbCommandDispatchers.invocation(...)` are the DCB versions, taking a `DcbCriteria` read boundary instead of a stream id. One thing to know before choosing this is that a lambda has no value equality, so you cannot compare issued commands with `containsExactly`. Check what the command does instead, with `step.issuedCommands().single().decision().apply(events)`, or run the saga and check the events it wrote. [ADR 81](doc/architecture/decisions/0081-function-shaped-saga-commands.md) explains why this is a command type rather than a new kind of saga effect.
* `CommandDispatchers.decider(...)` and `DcbCommandDispatchers.decider(...)` now write a saga reaction's commands as one append per target instead of one per command, so each command decides against what the ones before it decided and a failure partway leaves nothing written, instead of partial progress. Only consecutive commands are combined. Commands for order A, then order B, then order A again stay three appends, because the two order A commands are not next to each other. There is nothing to change in your code. `DcbCommandDispatchers.invocation(...)` is unaffected by design, since invocations sharing a boundary may carry different tag generators, and `DcbDeciderApplicationService` gained an `execute(DcbCriteria, List, decider)` overload for a caller that has already resolved the boundary. Resolves [#480](https://github.com/johanhaleby/occurrent/issues/480) and [#491](https://github.com/johanhaleby/occurrent/issues/491).
* `Saga.Step.timerEffects()` tells you which timers a reaction started or cancelled, the way `issuedCommands()` tells you which commands it issued. This helps when a reaction does both, because you can check the timers on their own instead of checking them mixed in with the commands. `SagaEffect` now has a `TimerEffect<C>` sealed subtype permitting `StartTimeout`, `StartTimeoutAt` and `CancelTimeout`, so the two accessors partition the sealed hierarchy at the type level: `timerEffects()` returns `List<SagaEffect.TimerEffect<C>>` and cannot statically hold a command any more than `issuedCommands()`'s `List<C>` can hold a timer.
* `Saga.Step` gained `issuedCommands()`, the commands a reaction issued. `effects()` mixes those with timer effects, so asserting on commands used to mean filtering the sealed `SagaEffect` hierarchy by hand. `effects()` itself is unchanged. Kotlin callers need the parentheses, `step.issuedCommands()`, since this is a derived accessor rather than a record component. Resolves [#448](https://github.com/johanhaleby/occurrent/issues/448).
* `occurrent-subscription-inmemory` now ships an `InMemoryCheckpointStorage`, so a `DurableSubscriptionModel` in a test or a small application no longer needs MongoDB or Redis just to remember where a subscription got to. It keeps checkpoints in a `ConcurrentHashMap`, which means they are gone when the process is, and the in-memory stack now has a checkpoint storage to go with the event store and subscription model it already had. Four test classes in this repository had each written their own copy of it, which is what made the case for publishing one. Part of [#395](https://github.com/johanhaleby/occurrent/issues/395).
* The three MongoDB event stores now report the same thing when they start against a collection whose events were written before positions existed. Each store worded it differently before, so the instruction an operator got depended on which store they ran. The message names `doc/runbooks/position-backfill.md` either way. The two paths deliberately say different things. With `requireBackfilledPosition(true)` the store refuses to start and tells you how to satisfy that setting, and without it the store starts and tells you what it silently loses, which is that position-ordered reads and position-based catch-up skip every event that has no position. Resolves [#486](https://github.com/johanhaleby/occurrent/issues/486).
* `InMemoryEventStore.exists(streamId)` no longer reports that a stream exists after `delete(Filter)` has removed every event in it. The store kept one entry per stream and answered existence by asking whether that entry was present, but the filter delete path could only replace an entry's contents, never remove the entry, so an emptied stream stayed behind as an empty one. `count()` and reading the stream were already correct, so existence was the only thing that disagreed, and it disagreed with all three MongoDB stores and with `deleteEventStream` on the in-memory store itself. Existence now means the stream holds events, and the filter delete path removes an entry it empties. Resolves [#464](https://github.com/johanhaleby/occurrent/issues/464).
* `updateEvent` reports the same message on every event store when the update function returns `null`. All four threw `IllegalArgumentException` already, but the in-memory store worded it differently from the three MongoDB stores, so a caller could not match on the message and an implementation outside this repository had no wording to copy. The message now lives in one place next to the other event-store API internals and every store uses it. The MongoDB wording is the one that survived, so only the in-memory message changed. Resolves [#465](https://github.com/johanhaleby/occurrent/issues/465).
* `ReactorMongoEventStore.updateEvent` now emits the event when the update function returns something equal to what is already stored, instead of completing empty. An empty result means one thing in that interface, that no cloud event matched the id and source, so completing empty for an event that was found and left alone made "found but unchanged" indistinguishable from "does not exist". The other three stores already returned the event. The reactive interface's javadoc also stopped describing its result as an empty `Optional`, which was copied from the blocking twin. The in-memory store additionally skips rewriting a stream when an update changes nothing, which the MongoDB stores already did. Resolves [#466](https://github.com/johanhaleby/occurrent/issues/466).
* `ReactorMongoEventStore.write` no longer fails with `WriteConditionNotFulfilledException` when several threads write to the same stream at once under `anyStreamVersion()`. That write condition promises the write cannot fail on the stream's version, and both blocking MongoDB stores have retried the race since 2024, but the fix at the time named only those two and left the reactive store out. It now retries the same way, and only when it owns the transaction ([ADR 74](doc/architecture/decisions/0074-retry-only-where-the-transaction-is-owned.md)), so a write joined to a caller's transaction still runs once. Found by running the new TCK concurrency suite against all four stores. Resolves [#474](https://github.com/johanhaleby/occurrent/issues/474).
* `WriteConditionNotFulfilledException` and `DcbAppendConditionNotFulfilledException` each got a constructor that composes the standard message, so a caller passes only the data (the write condition, the stream id and the version the stream was at) and gets the message every Occurrent event store produces. The exact wording is part of the contract rather than a cosmetic detail, because `WriteConditionNotFulfilledException.equals` compares `getMessage()` and the event store conformance suite asserts it, but it was written out by hand at nine throw sites across the four stores and `MongoExceptionTranslator`. Anyone implementing an event store outside this repository therefore had to copy a format string out of a store's source to pass the suite. The message-taking constructors stay for a store that genuinely needs different wording, the produced strings are unchanged, and no existing caller needs migrating. Resolves [#456](https://github.com/johanhaleby/occurrent/issues/456).
* Stopping and then starting a `CompetingConsumerSubscriptionModel` no longer leaves a subscription with no consumer at all. A stopped model kept competing for the lock and could take one while stopped, so the restart found nothing to do and events written afterwards were never delivered. This needed a lease time short enough for a refresh to land while the model was stopped, so the default 20 seconds rarely hit it.
* Contributor-facing only, with nothing changing in a published artifact. A MongoDB test container now owns the database it hands out, so two Maven runs on one machine no longer delete each other's data, and nothing binds host port 27017 any more, so a locally installed MongoDB no longer blocks the suite. Removing the fixed port also surfaced that the test configuration still used the deprecated `spring.data.mongodb.uri`, silently reaching a local MongoDB all along, and the test side now uses `spring.mongodb.uri`. This repository's own tests also moved off `test-support`'s `FlushMongoDBExtension` and onto the published `OccurrentMongoFlush`, and the old extension is deleted. Resolves [#505](https://github.com/johanhaleby/occurrent/issues/505). Rationale in [ADR 97](doc/architecture/decisions/0097-a-test-container-owns-its-database.md).
* **`Projections.materializedView` and `Projections.domainEventFeed` gain overloads taking a `RetryStrategy`, so the default push-projection read, fold and save can recover a concurrent update instead of silently losing one.** This became reachable once a live push handler stopped running behind one lock ([ADR 108](doc/architecture/decisions/0108-a-live-push-handler-runs-outside-the-handover-lock.md)): two threads handling two events for the same projection key can now both read the same state, and the second save overwrites the first. The two existing overloads are unchanged, still `RetryStrategy.none()`, so nothing that compiles today changes behaviour. The new overloads only help when your store detects the conflict and throws, an optimistic-locking failure or a unique-key violation, since a retry that never sees an exception is a no-op. `CatchupProjectionFeed`, `DomainEventFeed`, `ProjectionRunner`, `DcbProjectionRunner` and `@Projection(source = PUSH)` all take the resulting `MaterializedView` through their existing overloads, so no further API grew to reach them. [Section 14 of the upgrade guide](doc/migration/upgrading-to-0.32.0.md#14-a-live-push-handler-can-now-be-called-concurrently) covers the fix and what it does not cover. Resolves [#616](https://github.com/johanhaleby/occurrent/issues/616).

#### Breaking changes

* **A saga now refuses an event it cannot recognise a redelivery of, so this changes behaviour that shipped.** `SagaRunner` takes any `Subscribable`, so a saga wired over a `PushSubscriptionModel` by hand has always been able to receive events carrying neither a stream id with a stream version nor a position. It reacted to every one of them, issuing the same commands again on every redelivery, and said so in a single warning. It now throws `SagaRedeliveryDetectionException` before the reaction runs, and the event goes unacknowledged, so the listener that dropped the metadata is what you find. Forward the Occurrent CloudEvent extensions from that listener, or keep the old behaviour with `SagaRunnerConfig.withRedeliveryDetection(BEST_EFFORT)` and `@Saga(redeliveryDetection = BEST_EFFORT)`, which is right when the feed genuinely carries none of them and every command the saga issues is safe to receive more than once. [Section 15 of the upgrade guide](doc/migration/upgrading-to-0.32.0.md#15-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of) covers both. Resolves [#583](https://github.com/johanhaleby/occurrent/issues/583). Rationale in [ADR 109](doc/architecture/decisions/0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md).
* **A live push handler no longer runs behind one global lock, so this changes behaviour that shipped.** `CatchupThenPushSubscriptionModel` and `CatchupProjectionFeed` on the blocking stack used to hold one lock around every live handler call, permanently, once the catch-up finished, capping throughput at roughly one payload per handler duration no matter how much listener concurrency you configured. A JMH benchmark confirmed the ceiling, and moving the handler call outside the lock reached 3.4x higher throughput at 8 threads. The dedup-key reservation that protects the replay-to-live overlap still happens under the lock. **A handler must now tolerate concurrent invocation once the handover is live, wherever you configure more than one delivering thread.** A single-threaded caller, the common case, sees no change. Resolves [#588](https://github.com/johanhaleby/occurrent/issues/588). Reasoning, including the full benchmark, in [ADR 108](doc/architecture/decisions/0108-a-live-push-handler-runs-outside-the-handover-lock.md), and see [Upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md).
* **The reactor `SubscriptionModel` now means what the blocking `SubscriptionModel` means.** It is `Subscribable` plus `SubscriptionModelLifeCycle` and declares nothing of its own, so a named, lifecycle-managed subscription model has one type and one name on both stacks. `ReactorMongoSubscriptionModel`, `ReactorDurableSubscriptionModel`, `CatchupThenPushSubscriptionModel` and the reactive push and synchronous models implement it. The interface that used to have this name, the one whose `subscribe` returns a `Flux<CloudEvent>` you subscribe to yourself, is now `FluxSubscriptionModel`. `UpgradeToOccurrent_0_32` renames it in your code, and [section 5 of the upgrade guide](doc/migration/upgrading-to-0.32.0.md#5-the-reactor-subscriptionmodel-is-now-fluxsubscriptionmodel) covers what to do if you would rather do it by hand. The rename left the blocking `CheckpointAwareSubscriptionModel` extending the wide `SubscriptionModel` while its reactor twin extends `FluxSubscriptionModel`, which over-constrained `ManualStartSubscriptionModel.stoppedByDefault`: its position-source parameter took the whole blocking model just to call `globalCheckpoint()`. `GlobalCheckpointSource<T>` now carries just that one method, both `CheckpointAwareSubscriptionModel` interfaces extend it, and `stoppedByDefault`'s parameter widens from `CheckpointAwareSubscriptionModel` to `GlobalCheckpointSource<@Nullable Checkpoint>`. Reasoning in [ADR 98](doc/architecture/decisions/0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md).
* **`NativeMongoLeaseCompetingConsumerStrategy` moves from `org.occurrent.subscription.mongodb.spring.blocking` to `org.occurrent.subscription.mongodb.nativedriver.blocking`.** It is built on the native Java driver and never touched Spring, but shipped under the package the three Spring competing-consumer artifacts own for real, so an application depending on all four carried four identical copies of the same `package-info.class`, and the import line of a Spring-free application read as though it depended on Spring. Every other native-driver subscription type already lives under `nativedriver.blocking`, and this was the one outlier. `UpgradeToOccurrent_0_32` renames it for you, including the qualified `Builder` construction and the `withDefaults(..)` factory call, and [section 9 of the upgrade guide](doc/migration/upgrading-to-0.32.0.md#9-nativemongoleasecompetingconsumerstrategy-moves-to-the-native-driver-package) covers doing it by hand. Resolves [#534](https://github.com/johanhaleby/occurrent/issues/534).
* The MongoDB event stores now persist the CloudEvent `time` attribute in one canonical shape when `TimeRepresentation.RFC_3339_STRING` is configured, always with seconds and always with nine fractional digits, so `2026-07-28T12:00:00.000000000Z` rather than `2026-07-28T12:00Z`. That fixes two bugs. `Filter.time(instant)` did not match an event written at exactly that instant when the timestamp was truncated to a whole minute, on the event stores and on MongoDB subscriptions alike, and range queries on `time` were unsound because a variable-length string does not sort chronologically. Nanosecond precision is unchanged. Range ordering now holds for a collection whose events all carry the same UTC offset, so one mixing offsets still needs `TimeRepresentation.DATE` or a custom attribute. Events written by an earlier version keep their old shape. An `eq`, `in` or `ne` filter on `time` still matches them, since it now compares against both the canonical and the legacy shape a value could have been written with, but a range boundary landing on one of them can still miss it. Nothing has to be migrated for queries over events written from this version onward to be correct. If you want exactness across the upgrade, [upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md) has an optional one-off rewrite. Resolves [#463](https://github.com/johanhaleby/occurrent/issues/463) and [#468](https://github.com/johanhaleby/occurrent/issues/468). Rationale in [ADR 79](doc/architecture/decisions/0079-canonical-fixed-width-time-for-rfc3339-storage.md).
* **Each way a subscription model can refuse a call now has its own exception type, so this changes behaviour that shipped.** `subscribe(..)` throws `DuplicateSubscriptionIdException` for an id this model instance already has, `UnsupportedSubscriptionFilterException` for a filter shape it cannot apply, and `UnsupportedStartAtException` for a start position it does not accept. `pauseSubscription(..)` throws `SubscriptionNotRunningException` and `resumeSubscription(..)` throws `SubscriptionAlreadyRunningException`. Each one carries what the call named, so you can read `subscriptionId()`, `filterType()` or `startAt()` off it instead of parsing a message that was never part of the contract. They all extend `IllegalArgumentException`, which is what every one of them threw before, so an existing `catch` still catches them and no call site changes. **The sixth is a new answer rather than a new name for an old one.** `UnknownSubscriptionException` says this model has no subscription with that id, where pausing or resuming an id no model had ever seen used to claim it was not running or not paused. If you hold several subscription models and want the one that owns an id, that is now the difference between "try the next model" and "this is the owner and the answer is no", which is what `OccurrentSubscriptionsExtension` needed and had to approximate before. The six types are sealed under `SubscriptionRefusedException`, so a `switch` over them is exhaustive, and each builds one standard message, replacing the six different spellings Occurrent's own models used between them. A model built without a `DataFieldReader` and asked to filter on a payload field now throws `UnsupportedOperationException` rather than an argument exception, matching what a store already does for a capability it was not built with, because supplying a different filter is not what fixes it. Resolves [#580](https://github.com/johanhaleby/occurrent/issues/580). Reasoning in [ADR 106](doc/architecture/decisions/0106-a-refused-subscription-call-says-which-condition-it-hit.md), and see [Upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md).
* **`start()` on a subscription model that is already started is now accepted rather than refused, so this changes behaviour that shipped.** `CompetingConsumerSubscriptionModel` threw `IllegalStateException`, every other subscription model on both stacks accepted the call, and nothing said which was right, so the answer depended on which model you happened to hold. It matters because the competing consumer model is the one the Spring Boot starter wires by default, and because `ManualStartSubscriptionModel` hid the refusal, which made `occurrent.subscription.mode=auto` and `mode=manual` answer differently for the same caller. Starting a model is now a goal rather than a transition. It brings up whatever is not running yet, resumes a subscription that was paused on its own, and leaves the rest alone, so a leader election or a health check can call `start()` without asking `isRunning()` first, which is what manual mode is for. `SubscriptionModelConformance` checks it now, so every model has to agree. Resolves [#579](https://github.com/johanhaleby/occurrent/issues/579). Reasoning in [ADR 105](doc/architecture/decisions/0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md), and see [Upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md).
* **A subscription that has not started now says so from `waitUntilStarted`, and three released cases change with it.** The method promised only that it returns once the subscription has started, and seven different answers had grown underneath that. It now answers for the one start the handle was created for, and reports started once nothing further is required of you before the subscription can deliver. A subscription registered while a `PushSubscriptionModel` or a `SynchronousSubscriptionModel` was stopped answers `false` instead of `true`, because a stopped model of that kind drops what it is handed rather than holding it. A catch-up replay that failed reaches you as an exception from the blocking stream and DCB catch-up models, instead of being logged and reported as `false`, so a read model that was never filled cannot pass for a working one, and that handle also returns its delegate's answer rather than a fixed `true`. A catch-up cancelled before it went live answers `false`, and fails rather than completing on the reactive stack. A subscription that has started keeps answering `true` afterwards, even once it is paused or is waiting for another node to release a competing consumer lock, so ask `isRunning(id)` and `isPaused(id)` about the present. Resolves [#579](https://github.com/johanhaleby/occurrent/issues/579). Reasoning in [ADR 105](doc/architecture/decisions/0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md), and see [Upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md).
* **A synchronous subscription now still handles an event when another one throws**, on both the blocking and reactive stacks. Before this the first exception ended the dispatch, so the subscriptions registered after it never received that event and never would, since a synchronous subscription has no replay. It applies when the write is not inside a transaction, which is the default for a `GenericApplicationService` you build yourself but not for the Spring starter, which wires a transaction for you. The first failure still reaches you unchanged and any others arrive in its `getSuppressed()`. Inside a transaction dispatch still stops at the first failure, because the write rolls back, so no subscription is left having acted on an event that no longer exists. **One source break, only for a dispatcher you implement yourself.** `SynchronousEventDispatcher.dispatch(List)` and `ReactiveSynchronousEventDispatcher.dispatch(List)` are replaced by `dispatch(List, boolean transactional)`. Your implementation owns the handler loop, so a flag you could ignore by accident would keep stranding handlers with nothing to tell you. `SynchronousSubscriptionModel` keeps a one-argument `dispatch` of its own, so driving the model directly is unaffected. `TransactionExecutor` and `ReactiveTransactionExecutor` also gained `isTransactional()`, answered for the moment of the call so an executor whose transaction depends on its configuration can tell the truth. The amendment at the end of [ADR 57](doc/architecture/decisions/0057-synchronous-subscriptions.md) has the reasoning, and [upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md) has the migration.
* **A push sink now feeds exactly one projection or saga, and a second registration is refused at startup.** `PushSubscriptionModel` and `DomainEventFeed` used to fan one received message out to every consumer registered on them. A broker message carries one acknowledgement decision, so those consumers shared it. One that kept failing held up every consumer behind it on every redelivery, and they lost the message entirely once the broker gave up on it. Declare one sink bean per projection or saga, each fed by its own queue, and point each `@Projection(source = PUSH)` at its own bean with `subscriptionModelName`. If you shared one, your application now fails to start with a message naming both consumers. Nothing else changes. An event-store subscription model still serves any number of subscriptions, because each has its own cursor and checkpoint, and the synchronous models still fan out, because a handler failure there fails the write rather than stranding a sibling. [ADR 90](doc/architecture/decisions/0090-a-push-sink-feeds-one-consumer.md) has the reasoning, and [upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md) has the migration.
* **`DomainEventFeed.accept(..)` now refuses an event when no projection is registered.** It used to return normally, so a listener that acknowledges once `accept` returns acknowledged an event nothing received, and the broker then discarded it. Ordinary configuration reached this. Under `occurrent.subscription.mode=manual` the registration is deferred until you start the push sources, so anything your listener received before that point was lost. Register the projection before the listener starts consuming, or call the new `hasProjection()` to check first. `catchUpAll()` refuses on an empty feed for the same reason, where it used to do nothing and report success. `PushSubscriptionModel.accept(..)` still returns normally in that situation, because it is also fed from the write path (an `InMemoryEventStore` listener, say), where the event is already in the event store and refusing would fail the write instead of protecting anything. Ask its `hasSubscriptions()` when you feed it from a broker. [ADR 104](doc/architecture/decisions/0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) has the reasoning, and [upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md) has the migration.
* **Behaviour change for direct users of `CatchupThenPushSubscriptionModel`.** On the blocking stack, a catch-up replay failure now reaches you from `waitUntilStarted()` on the returned subscription instead of being thrown out of `subscribe(...)`, because the replay no longer runs on the calling thread. If you call `subscribe` inside a `try`/`catch`, move it to the `waitUntilStarted()` call or you will start with a read model that is silently empty. On the reactive stack the failure already arrived that way, but the replay itself now runs on `boundedElastic` rather than inline, so code that read the projected state straight after `subscribe(...)` without joining has to join. Nothing changes if you use `@Projection(source = PUSH)` or the projection DSL, both of which wait for you. A subscription whose catch-up failed also keeps its registration now, and refuses every event fed to the live feed afterwards, so your source redelivers them instead of losing them. It used to release the registration, which freed the subscription id but meant every later event was acknowledged and discarded. Fix the cause, then call `cancelSubscription(..)` and subscribe again. [Section 8 of the upgrade guide](doc/migration/upgrading-to-0.32.0.md#8-a-push-catch-up-replays-on-its-own-thread) shows the migration.
* **`CompetingConsumerSubscriptionModel` now refuses a subscription id it already has, and refuses a pause of a subscription it does not have. Both used to be accepted, so this changes behaviour that shipped.** `subscribe(..)` throws `DuplicateSubscriptionIdException` for an id this model instance is already subscribed to, the same as `InMemorySubscriptionModel`, `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` have always done, and `pauseSubscription(..)` throws `UnknownSubscriptionException` for an id it never had, which is what `SubscriptionModelLifeCycle` documented all along and the wrapped model already did before the wrapper swallowed it. **The refusal is scoped to one model instance and nothing about the competing consumer pattern changes.** Several instances, on several nodes, are still expected to subscribe to the very same subscription id, and that is what makes them compete. What is refused is a second subscription for one id *inside* one instance, which never worked in the first place, since the model resolves a consumer by subscription id alone and the second one was unreachable through `cancelSubscription`, `pauseSubscription` and `resumeSubscription` alike. Cancelling a subscription that opted out of competing consumption also frees its subscription id now, where the model used to remember it forever. Found by the subscription TCK, whose general suite runs against this model for the first time. Resolves [#553](https://github.com/johanhaleby/occurrent/issues/553). Reasoning in [ADR 102](doc/architecture/decisions/0102-a-subscription-id-is-unique-per-subscription-model-instance.md), and see [Upgrading to 0.32.0](doc/migration/upgrading-to-0.32.0.md).
* **`occurrent-command-composition` no longer exports the in-memory event store.** Its POM declared `occurrent-eventstore-inmemory` among the test dependencies but without `<scope>test</scope>`, so every consumer of `occurrent-command-composition` received the in-memory event store at compile scope. The dependency is now test-scoped. If your application used `InMemoryEventStore` through this transitive dependency, it will stop compiling. Declare `occurrent-eventstore-inmemory` explicitly. Resolves [#500](https://github.com/johanhaleby/occurrent/issues/500).

### 0.31.0 (2026-07-28)

#### Changes

* A DCB append no longer retries a write conflict when it is running inside a transaction it did not open. Only the code that began a transaction can begin a fresh one, so retrying inside a caller's transaction could never commit: MongoDB aborts the transaction on the first conflict and every later attempt fails on its first read with `NoSuchTransaction`, spending all 15 attempts and about 5 seconds before failing anyway. The two Spring MongoDB stores now check for an active transaction and run the append once when they find one, which is what the native driver store has always done with an ambient `ClientSession`. The retry moves to the layer that owns the transaction instead: `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry a conflict around the transaction they open, so a setup with synchronous subscriptions (where Occurrent itself opens the transaction so the write and the handlers commit together) keeps retrying as before. If you wrap a command in your own `@Transactional`, retry at that boundary, because a participating transaction is marked rollback-only once the append throws and carrying on inside it fails at commit with `UnexpectedRollbackException`. Both stacks change together so their retry behavior stays identical ([ADR 53](doc/architecture/decisions/0053-dcb-api-freeze-consistency.md)). The same rule now covers every retry on the write path, so a stream `write` inside a transaction it did not open no longer retries its any-version write condition or the global position counter's cold start either. That matters most for the write condition: joined to a caller's transaction, a failed condition marks it rollback-only, so a retry could previously report a success the caller could not keep. Rationale in [ADR 74](doc/architecture/decisions/0074-retry-only-where-the-transaction-is-owned.md).
* Added a reusable command-dispatch capability, extracted from the Saga DSL so a command producer other than a saga can use it. `CommandDispatcher<C>`, the interface a command producer calls to issue a command (previously in the saga module), now lives in the new `occurrent-command-dispatch` module together with `StreamIdResolver<C>`, a named command-to-stream-id function that is the stream-side counterpart of the DCB `TagGenerator`. The new `@TargetStreamId` annotation, which marks the command member whose value is the target stream id, joins the other Occurrent markers in `occurrent-annotations`. `occurrent-command-dispatch-annotation` adds `AnnotationStreamIdResolver`, a reflection-based `StreamIdResolver` that reads `@TargetStreamId` (or a custom marker annotation) off a command, mirroring the DCB `AnnotationTagGenerator`/`@DcbTag`. The blocking Spring Boot starter contributes a default `AnnotationStreamIdResolver` bean, overridable with your own `StreamIdResolver`. This is stream-side only, since DCB routes a command to its append boundary through tags (`@DcbTag`/`TagGenerator`, `DcbDecider.criteriaFor`) rather than a stream id. The convenience factory `CommandDispatchers`, the adapter turning a `Decider`-backed application service into a `CommandDispatcher`, lives in `occurrent-command-dispatch` too (package `org.occurrent.command`), taking a `StreamIdResolver` in place of a bare `Function`, so it is usable outside sagas without depending on the saga module. Its DCB counterpart `DcbCommandDispatchers.decider(...)` lives in a separate `occurrent-command-dispatch-dcb` module, kept apart so the heavier DCB dependency stack never reaches stream-only users, and needs no `StreamIdResolver` because the `DcbDecider` it wraps already carries its own `DcbCriteria` boundary and tags.
* A subscription handler can now receive the stream id or stream version directly through the new `@StreamId` and `@StreamVersion` parameter annotations, so `void on(MyEvent event, @StreamId String streamId, @StreamVersion long streamVersion)` works without declaring an `EventMetadata` parameter and calling `getStreamId()`/`getStreamVersion()`. The annotations may appear in any order alongside the event and an optional `EventMetadata` parameter, on `@Subscription`, `@StreamSubscription`, and `@SynchronousSubscription`, for both the blocking and reactive stacks. `@StreamId` binds a `String`, `@StreamVersion` a `long` or `Long`. They are rejected at startup on `@DcbSubscription`, whose stream id/version are internal partition values. On the capability-agnostic `@Subscription` a DCB-appended event exposes the internal partition id and per-partition counter, the same values `EventMetadata` already gives there. The existing `EventMetadata` parameter form is unchanged.
* The MongoDB change stream backing a subscription can now be tuned for throughput and latency. `NativeMongoSubscriptionModelConfig` gains `batchSize(int)` and `maxAwaitTime(Duration)`, and `SpringMongoSubscriptionModelConfig` gains `maxAwaitTime(Duration)`. A larger `batchSize` reduces server round-trips (throughput), and `maxAwaitTime` bounds how long the server holds an idle `getMore` before returning a (possibly empty) batch (latency versus resource usage). Both options are opt-in and default to unset, so the driver/server defaults still apply and no existing subscription changes behavior on upgrade. The options differ per model because Spring Data's abstractions do not expose the full set: the native driver's `ChangeStreamIterable` supports both, while the Spring blocking model reads the change stream through Spring's `MessageListenerContainer`, whose `ChangeStreamRequest`/`ChangeStreamRequestOptions` API carries a `maxAwaitTime` but no batch size (`ChangeStreamTask` never applies one), so `batchSize` is only available on the native model. `ReactorMongoSubscriptionModel` exposes neither yet, because Spring Data's `ReactiveMongoTemplate.changeStream` and its `ChangeStreamOptions` carry neither, and adding them means driving the raw reactive driver directly (deferred as a follow-up). Resolves [#173](https://github.com/johanhaleby/occurrent/issues/173). Rationale in [ADR 67](doc/architecture/decisions/0067-configurable-change-stream-batchsize-and-maxawaittime.md).
* Building DCB query criteria from event classes reads better in Kotlin. `queries.criteria()` (and `subscriptions.criteria()`) now lets you write `type<OrderPlaced>()` and `types<OrderPlaced, OrderCancelled>()` with the base event type inferred, or the `KClass` forms `type(OrderPlaced::class)` and `types(OrderPlaced::class, OrderCancelled::class)`, instead of `::class.java`. You can also reuse a shared boundary and give it query-specific types with `criteria(boundary).types<...>()`, where `boundary` is a `DcbCriterion` (a single alternative, so it stays within the OR-of-items model, [ADR 32](doc/architecture/decisions/0032-fluent-dcb-query-construction.md)). This is a Kotlin-only convenience: `DcbCriteriaBuilder` became a Kotlin class so the reified helpers can infer the base type as members, and its Java surface is unchanged. The old `typeOf<T, E>()` is deprecated in favor of `type<T>()` (the base type is now inferred), with a `@Deprecated` replacement hint. Rationale in [ADR 66](doc/architecture/decisions/0066-kotlin-first-dcb-criteria-construction.md).
* Added a Saga DSL, a first-class way to describe an event-driven process manager: react to events, and to their absence (timeouts), by issuing commands while holding per-instance state. Where a decider turns commands into events, a `Saga<E, S, C>` turns events into commands. It is pure data and pure functions with no I/O, so it unit-tests the same way a decider or view does. A saga reacts to two kinds of input, a domain event or one of its own timers firing, and both arrive as a `SagaInput`. It applies that input to its state with `evolve`, then `react` returns the effects to run against the state `evolve` produced (a `SagaEffect` issues a command, or starts or cancels a timer). Replay is therefore effect-free, because rehydrating an instance runs only `evolve` and never calls `react`. A reaction can read the delivering event's metadata (its stream id and version, global position, and any CloudEvent extension) through optional metadata-carrying `evolve`, `react`, and `onStart` overloads, so a saga can correlate or decide on position without carrying it in the event payload, and the flow layer's `on` step and its `startsOn` expose the triggering event's metadata the same way. In Kotlin a reaction returns the effects receiver rather than nothing, which is what makes a produced but discarded command a compile error instead of a saga that silently does nothing. Correct reactions are unaffected, since `issue` and the timer calls return that receiver, so only a reaction whose last statement is neither (typically one ending on an `if` without an `else`) needs the new `nothing` to close it. A branch, join, timeout or start that issues nothing takes no reaction lambda at all, in Kotlin by omitting it and in Java through new no-commands overloads on `StepBuilder`, matching the `startsOn(Class)` convenience that already existed. Rationale in [ADR 75](doc/architecture/decisions/0075-make-a-dropped-saga-command-a-compile-error.md). Timers carry a `Duration` or an `Instant`, never a clock-reading `Deadline`, so a reaction stays deterministic and effect-equal in tests. Correlation is per-event-type with a `correlateAll` fallback, validated at build time so a handled event can never lack a correlation at runtime. Two DSLs describe the same saga and run on the same executor. The core DSL (`Saga.builder(...)` in Java, `saga(initialState) { }` in Kotlin) registers a fold and a reaction per event type against your own state. The flow DSL (`FlowSaga.builder()` in Java, `saga { }` in Kotlin) describes the process as named steps with branches, joins and per-step timeouts, keeps the state for you, and builds a `Saga<E, FlowState<E>, C>` underneath, so the executor only ever runs one kind of saga. A flow saga's received-event log is bounded to a configurable window (`FlowSaga.Builder.historyWindow(int)` in Java, `historyWindow(...)` in the Kotlin `saga { }` block, default 100), so a long-running instance does not grow without bound and each save re-serializes only that window. A blocking executor runs a saga against a subscription (`SagaRunner.agnostic(...)`/`stream(...)`): it applies and persists per-instance state through a `SagaStateStore` (an in-memory implementation, and a Spring Mongo `SpringMongoSagaStateStore`), dispatches the commands each reaction issues through a `CommandDispatcher` (usually a plain lambda over an `ApplicationService`, with `CommandDispatchers.decider(...)` as an adapter into the decider machinery), and fires timeouts. Timers live in the saga's own state envelope rather than an external scheduler, so timer bookkeeping is exactly-once (saved atomically with the state) and an executor-side poller reads due timers from the store. The saga DSL therefore takes no dependency on the deadline module. In a multi-instance deployment the poller is lease-gated (via a `CompetingConsumerStrategy`, wired by the `@Saga` starter by default, opt out with `occurrent.saga.competing-consumer.enabled=false`) so only one instance queries the store, and it polls every 15 seconds by default (`occurrent.saga.timer-poll-interval`, matching JobRunr) to suit the minutes-to-days timescale saga timeouts run on ([ADR 64](doc/architecture/decisions/0064-lease-gate-the-saga-timer-poller.md)). Command dispatch is at-least-once (dispatched before the compare-and-set state save, so a command is never lost), which is safe because an `ApplicationService`-backed receiver replays the authoritative stream and rejects a stale or duplicate command. One reaction's commands reach the dispatcher as a unit, through `CommandDispatcher.dispatchAll(List)`, whose default simply dispatches them one at a time. A dispatcher whose commands all target one stream or one decider can override it and write them in a single transaction, which removes the window where a failure partway through the list leaves the earlier commands dispatched and the state unsaved. That is a seam a dispatcher may use rather than a guarantee Occurrent makes, so dispatch stays at-least-once either way ([ADR 76](doc/architecture/decisions/0076-batch-command-dispatch-seam.md)). `@Saga` declares one on the blocking Spring stack (a factory method returning a `Saga`), resolving the state store by convention (an explicit or unique `SagaStateStore`, otherwise a `saga-<id>` MongoDB collection) and the command dispatcher from a bean, and registering it through the same catch-up and durable infrastructure `@Projection` uses. Instance lifecycles are observable through `SagaInstances`, a read-only lookup obtained from `SagaSubscription.instances()` or, on the Spring stack, either by injecting the `SagaInstancesRegistry` (keyed by saga id, and able to enumerate the registered ids so a dashboard need not hardcode them) or from the per-saga `sagaInstances-<id>` bean the `@Saga` registrar publishes. Both Spring paths only hold sagas once the `@Saga` scan has run, which is after the context refreshes, because a saga factory cannot be invoked before the beans it collaborates with are wired. It hands back `SagaInstance`, a narrow view carrying the instance id, its `SagaStatus`, the created/updated/completed timestamps, when its earliest pending timer is due, and the step a flow saga is waiting in, while the executor's delivery bookkeeping and the saga's own state stay off it. Enumeration is an optional store capability: a store that also implements `SagaStateStoreQueries` supports `findByStatus(status, updatedBefore, limit)`, which returns instances least-recently-updated first, so a progress view lists everything in a status and a stuck-instance check asks for the active instances that have been quiet for longer than a threshold, neither of which requires reading the store's documents directly. It is kept off the core `SagaStateStore` because the executor never needs it and because ordering is a real demand on a store, so an implementation can run sagas without supporting observation. `SagaInstances.find(sagaId)` works against any store and only enumeration requires the capability. A store answers this without reading saga state (the Mongo store denormalizes the flow step into its own field, the way it already does the next timer), so enumerating costs the same whether an instance carries one event of history or a hundred. A saga is not a substitute for a Dynamic Consistency Boundary: where two rules must hold in one append, DCB is the right tool. A saga is for genuinely cross-boundary, time-involving, eventually-consistent processes. `adapt` ships now. `compose` is deferred, because composing two sagas does not combine their correlation rules, their timer names, or their notions of being finished in any obvious way. A new `order-fulfillment` example module demonstrates both surfaces, in-memory and Docker-free. Resolves [#124](https://github.com/johanhaleby/occurrent/issues/124) and [#377](https://github.com/johanhaleby/occurrent/issues/377). Rationale in [ADR 63](doc/architecture/decisions/0063-saga-dsl.md), with the instance observation surface in [ADR 70](doc/architecture/decisions/0070-saga-instance-observation.md).
* Added a generic push subscription model so the Projection DSL can be fed from an external source instead of a MongoDB change stream. `PushSubscriptionModel` (blocking and reactor, in the new `occurrent-subscription-push-blocking` and `occurrent-subscription-push-reactor` modules) is a register-only `Subscribable` driven by an `accept(CloudEvent)` call rather than a change stream, so a RabbitMQ, Kafka, Spring-event, or HTTP listener can drive projections in production. It is the same shape as the synchronous subscription model, sharing register-and-route machinery with it through `RegisteringSubscribable` in the blocking and reactor subscription api modules. Occurrent adds no broker dependency of its own, and the pushed events must carry the Occurrent cloud-event extensions the handlers rely on (forward the stored CloudEvent as CloudEvents JSON and reconstruct it on the listener side). Because the projection DSL already accepts any `Subscribable`, `ProjectionRunner.agnostic(pushModel, converter).project(...)` and the Kotlin `project(...)` extensions work over a push model unchanged. The plain push model carries only the live tail, so a `CatchupThenPushSubscriptionModel` (blocking and reactor) adds a one-time catch-up in front of it: on first subscribe it replays a projection's history from the event store, then hands over to the live feed (buffering the feed during the replay and de-duplicating the overlap by event id), and records a one-shot catch-up-complete marker so a restart skips the replay. Because it always replays from the beginning and then hands over, it rejects a caller-supplied `startAt` rather than quietly ignoring one. Live-resume is the broker's job (acknowledge after processing), so no live position watermark is persisted and delivery is at-least-once over idempotent folds. This is the "broker owns live-resume" contract, chosen because Occurrent reserves positions outside the write transaction, making a feed-derived position watermark unworkable (permanent gaps are indistinguishable from late-committing holes in the delivery stream). Declaratively, `@Projection` gains a `source` attribute: `source = Source.PUSH` (with `subscriptionModel`/`subscriptionModelName` to pick the push model bean) binds a `@Projection` to a `PushSubscriptionModel`, wrapping it in the catch-up automatically, on both the blocking and reactor stacks. The default `Source.EVENT_STORE` is unchanged. The `accept(CloudEvent)` capability is exposed as a `Pushable` interface (blocking and reactor, in the subscription api modules) that `PushSubscriptionModel` implements, so a listener can depend on the capability rather than the concrete model. Cancelling one subscription by id is now its own capability, `CancellableSubscriptions` (blocking and reactor, in the subscription api modules), which the released `SubscriptionModelLifeCycle` extends and which the register-only push and synchronous models implement. Those models have nothing to start, stop, or pause, since their events arrive from the caller rather than from a feed they drive, but cancelling one still means something, and the catch-up model uses it to release a subscription whose replay failed instead of leaving a handler that rejects every later event and blocks the subscriptions registered behind it. Existing implementations and callers of `SubscriptionModelLifeCycle` are unaffected.
* Added domain-event feeds for projections, so a source that already hands you domain events (a RabbitMQ or Kafka listener with its own message converter) can drive a projection with no double encode/decode on the live path. `Projections.domainEventFeed(projection, repository)` returns the sink that applies a domain event straight into the read model with no CloudEvent conversion, a `MaterializedView<E>` (blocking) or a `BiFunction<EventMetadata, E, Mono<Void>>` (reactor). Metadata is carried on the live path as well as the replay, so a projection keyed on the stream id, version or position works either way: a listener whose broker message carries those values passes them alongside the event using the two-argument form of whichever sink it feeds (`update(metadata, event)` on the blocking `MaterializedView`, `apply(metadata, event)` on the reactor `BiFunction`, or `accept(metadata, event)` on a `CatchupProjectionFeed` or `DomainEventFeed`), and the one-argument form applies with none. Occurrent cannot derive them for a live domain event, because there is no CloudEvent behind it, so the source supplies them or nothing does. A projection keyed on metadata that is fed without any fails loud instead of silently skipping every event, which is what a key resolving to null would otherwise mean. `CatchupProjectionFeed` adds a one-time catch-up to that domain feed: the live path applies domain events directly, and only the catch-up reads the event store and decodes each replayed event once, de-duplicating the replay-to-live overlap by an id extracted from the domain event (so it does not depend on the CloudEvent id). `DomainEventFeed<E>` is an application-owned fan-out sink (the domain twin of `PushSubscriptionModel`) that feeds several projections from one source, and `@Projection(source = Source.PUSH, subscriptionModelName = "...")` binds a declared `@Projection` to it on both stacks, backed by a `ViewStateRepository` or a `MaterializedView` store. The de-dup cache size and the live-buffer cap are configured with `CatchupThenLiveOptions`, which `CatchupThenPushSubscriptionModel` takes too, or in Spring Boot with `occurrent.subscription.catchup-then-live.dedup-cache-size` and `.max-buffered-events` for a projection fed by a `PushSubscriptionModel`. Those properties do not reach a `DomainEventFeed`, since your application declares that bean, so pass the options to its constructor instead. Rationale in [ADR 62](doc/architecture/decisions/0062-pluggable-projection-event-source.md).
* Added first-class snapshot support, an opt-in optimization that lets a command's replay apply only the events written after a saved state instead of the whole history. A snapshot is a discardable, schema-versioned cache of the state at a known version, so a changed state shape or a missing snapshot falls back safely to a full replay and a snapshot is never a source of truth (a snapshot found ahead of the stream's head, after a stream reset or truncation, is discarded and rebuilt rather than trusted). It works from a `Decider` (the decision state) and from a plain `View` (the deciders-free read side), across stream and DCB and across the blocking and reactor stacks. The pieces: a small `SnapshotStore` capability that binds to no particular store (an in-memory implementation, and a Spring Mongo `SpringMongoSnapshotStore`), a higher-order `SnapshotPolicy` that unifies technical and domain triggers (`everyNEvents`, `onEvent`, `whenState`, `always`, `never`, `or`, and `whenTerminal` which is the "closing the books" trigger built on `Decider.isTerminal`), and the executors `SnapshotDeciderApplicationService` and `SnapshotDcbDeciderApplicationService` (with reactive twins) plus a `SnapshotViews` on-demand facade whose `readState` is a pure read and whose `refresh` explicitly persists a snapshot (no policy on the read path). Each of these is a global facade, constructed once around the shared infrastructure (the application service, or the event store plus converter) and reused for every aggregate. What is specific to one aggregate lives in a per-aggregate spec created with `from(...)`: `SnapshotDecider.from(decider, store, options)`, `SnapshotDcbDecider.from(dcbDecider, store, options)` (defaulting the criteria-to-key function, with an overload to override it), and `SnapshotViewSource.from(view, store)` (reactive twins `ReactiveSnapshotDecider`, `ReactiveSnapshotDcbDecider`, `ReactiveSnapshotViewSource`), each bundling the decider or view with its `SnapshotStore` and options, so one facade never binds one aggregate's store or policy and the same policy cannot vary per command. A spec holds an I/O collaborator (its store), so it is not a pure value like `Decider`, but it stays inert and the facade performs all I/O. The `@Snapshot` annotation declares and maintains a snapshot through the same catch-up and durable infrastructure `@Projection` uses, on both the blocking and reactor stacks and for stream and DCB (a stream snapshot is kept per stream, a DCB snapshot per boundary keyed by a canonical form of its criteria). A snapshot fold can read the delivering event's metadata (its stream id and version, global position, and any CloudEvent extension) by building the view with the metadata-carrying `View.create(initialState, fold)` form, and it sees the real metadata on every path a snapshot is maintained, whether the update applies a single new event, applies a range of several events at once (which is what an `everyNEvents` policy does on each save), or applies a DCB boundary. The Kotlin `snapshotView { }` and `dcbSnapshotView { }` blocks take a metadata-aware `on` too, alongside the event-only form. Snapshot persistence through the DSL executors is best-effort: the snapshot is saved after the write commits, a save failure is logged rather than failing the already-committed command, and a lost snapshot only costs a fuller replay. When a snapshot must stay consistent on the write path, maintain it with `@Snapshot(mode = SYNCHRONOUS)` or a synchronous subscription, which applies it inside the write transaction (ADR 57). Closing the books is a policy plus a documented pattern rather than a separate module: model the closing balance as a real domain event that becomes the next period's opening balance, and archive prior events with the existing `deleteEventStream(String)` or `delete(Filter)` when they are no longer needed. Enabling a snapshot adds one snapshot load and one tail read per snapshotted execute, paid only where snapshots are used. Supporting core additions: `ExecuteOptions.fromStreamVersion(...)` and `DcbExecuteOptions.fromPosition(...)` (a state-agnostic read offset so the one execute path is reused, not forked) and `Decider.evolve(state, events)` (the apply-onto-a-base-state primitive, mirroring `View.evolve`). A new `closing-the-books` example module demonstrates both a technical every-N snapshot and a domain period close. Rationale in [ADR 61](doc/architecture/decisions/0061-first-class-snapshot-support.md) and, for the global-facade-plus-per-aggregate-spec API shape, [ADR 69](doc/architecture/decisions/0069-programmatic-snapshot-facade-and-per-aggregate-spec.md).
* Added the `@Projection` annotation, a persistent, declarative read model built on the Projection DSL from ADR 58. A factory method annotated with `@Projection` returning a `Projection` or `DcbProjection` is registered as a managed subscription on both the blocking and reactor stacks, subscribing through the same catch-up and durable-checkpoint infrastructure `@DcbSubscription`/`@StreamSubscription` already use, so catch-up and durable resume come from the subscription model, not from the DSL. Store the result through a `ViewStateRepository`, a `MaterializedView`, or a Spring Data `CrudRepository`, selected by the store bean's type with `store = SomeStore.class` or by bean name with `storeName` (and `storeName` disambiguates when several beans share the type), with the Mongo default on the blocking stack when both are left unset. `mode` selects `ASYNC` (catch-up then live) or `SYNCHRONOUS` (read-your-writes, reusing the synchronous subscriptions from ADR 57), and is mutually exclusive with the catch-up start knobs `startAt`, `startAtGlobalPosition`, and `resumeBehavior`. The same catch-up-then-resume behavior is reachable programmatically, without the Spring starter, through the new `ResumeStartPositions.replayThenResume(...)`/`replayThenResumeDcb(...)` helpers (blocking and reactor) passed to `ProjectionRunner`/`DcbProjectionRunner`. Rationale in [ADR 59](doc/architecture/decisions/0059-projection-annotation.md).
* Added a Projection DSL, a higher-level way to build read models. A `Projection` couples a `View` (the pure fold) with the function deriving which view instance an event updates and the event types it handles, and a `DcbProjection` adds a DCB read boundary such as a tag filter. A single-instance read model omits the per-event id with `singletonProjection`/`dcbSingletonProjection` in Kotlin or `singletonBuilder(...)` in Java, and the runtime keys its one slot by the projection's own identity. You describe the fold with a type-safe, per-event-type handler builder, in Java with `Projection.builder(initialState).on(OrderPlaced.class, (state, event) -> ...)` or in Kotlin with `projection(initialState) { on<OrderPlaced> { state, event -> ... } }`, and the registered handlers also determine the subscription filter, so the event types are declared once. A handler and the id function can also read the event's metadata (stream id and version, global position, CloudEvent extensions) through optional metadata-carrying `on(...)` and `id(...)` overloads, so a projection can key a view instance by stream id or the global position, leaving the event-only forms unchanged. The reactive runner's caller-supplied update function can take the metadata as well, on both the plain and the DCB stack, matching the materializing overloads beside it, and the Spring MongoDB `project(...)` helpers resolve a metadata-keyed id through the same path every other runner uses. Runners then create the subscription or query and keep the read model up to date. The same shape works across stream and DCB and across the blocking and reactive stacks, in three delivery modes. An asynchronous subscription updates a stored read model eventually, a synchronous subscription updates it on the write path for read-your-writes (see synchronous subscriptions above), and an on-demand variant runs a query when you ask. It builds on the existing view DSL and is the read-side counterpart to the `DcbDecider` write-side model. A new `projection-dsl` example module demonstrates it in Java and Kotlin for both stream and DCB, including the tag-scoped `isUsernameClaimed` projection. Resolves [#194](https://github.com/johanhaleby/occurrent/issues/194). Rationale in [ADR 58](doc/architecture/decisions/0058-higher-level-read-model-projection-dsl.md).
* The view DSL can now handle event metadata. `View` gained a metadata-carrying `evolve(state, metadata, event)` and a `View.create(initialState, fold)` taking a `(state, EventMetadata, event)` fold, and `MaterializedView` gained an `update(metadata, event)` and a `create(...)` overload that derives the view-instance id from metadata (for example keying by stream id), and the Spring MongoDB `materialized(...)` helper gained the same metadata-aware id function so a Mongo-backed view can be keyed that way too. That helper now also fails with a clear message when the document's `@Id` differs from the id derived for the event, which previously produced a view that read one document, wrote another, and so never accumulated. The event-only methods are unchanged and delegate with empty metadata, and the on-demand query and replay path, which has no originating CloudEvent, applies events with empty metadata (its typed stream accessors then have nothing to read). This is the primitive the metadata-aware projection and saga DSLs build on, reusing the existing `EventMetadata` so the fold reads the same stream id, version, position, and CloudEvent extensions a subscriber already sees. Rationale in [ADR 68](doc/architecture/decisions/0068-first-grade-event-metadata-in-the-dsls.md).
* Added synchronous subscriptions. A subscription can now be declared synchronous with the new `@SynchronousSubscription` annotation (or by registering a handler on a `SynchronousSubscriptionModel` via the `Subscriptions` DSL, no Spring required), and the application service invokes it synchronously, on the writer thread, before `execute` returns, so a projection can be updated in the write path. This is a distinct, decoupled mechanism from the existing asynchronous subscriptions, which are unchanged. The handler receives the just-written events enriched with stream version and global position. Transactions are opt-in and best-effort by default: configure a `TransactionExecutor` on the application service (`builder(...).transactionExecutor(...)`) to make the write and the handlers commit atomically, with a Spring-backed executor (`TransactionTemplate`/`TransactionalOperator`) or a native MongoDB `ClientSession`-backed executor, and a handler's own `@Transactional` composes with it. Without a transaction executor a synchronous handler still runs before `execute` returns, but the write has already committed, so a throwing handler does not roll it back. Enabling synchronous subscriptions adds one read per event-producing write (to recover the global position), paid only while at least one synchronous subscription is registered. Available on the blocking and reactive, stream and DCB application services. Rationale in [ADR 57](doc/architecture/decisions/0057-synchronous-subscriptions.md).
* Added Java-friendly facades for running a `Decider` or `DcbDecider` through an application service, so Java callers get the same one-call helpers the Kotlin `execute(command, decider)` extensions already provide (those are Kotlin `inline`/`reified` and not callable from Java). Construct one around an existing application service and call `execute(command, decider)`. `org.occurrent.dsl.decider.DeciderApplicationService` wraps a blocking `ApplicationService` and runs a stream `Decider`, and `org.occurrent.dsl.dcb.blocking.DcbDeciderApplicationService` and `org.occurrent.dsl.dcb.reactor.DcbDeciderApplicationService` wrap the blocking and reactive DCB application services and run a `DcbDecider` (with `execute`, `executeAndReturnState`, `executeAndReturnEvents`, and `executeAndReturnDecision`). The decider's event type must match the application service's event type. If a decider only handles a subset of the events, for example one feature's events while the application service handles them all, convert it to the service's event type first with `Decider.adapt(...)` or `DcbDecider.adapt(...)`.
* Added `DcbDecider.criteriaFor(command)` and `criteriaFor(commands)` to resolve the DCB read boundary for one or more commands, rejecting an unrecognized command and requiring all commands in a single execute to share a boundary since they are appended atomically under one condition. The Kotlin DCB decider extensions now delegate to it.
* DCB reads can now select a position range, direction, skip count, and limit through composable `DcbReadOptions`. For example, `DcbReadOptions.fromBeginning().backwards().limit(1)` reads the single highest-position event matching a criterion in one round trip, so a gapless business sequence such as an invoice number can look up its last entry without working through the whole history. `skip(n)` skips matches from the selected direction before `limit` is applied, which makes `fromBeginning().backwards().skip(1).limit(2)` select the 2 matches before the newest one. These options only select which matching events are returned. A `DcbEventStream` is always ascending by position, and its consistency token still reflects the whole matching set, so a partial read guards an append against any later matching event. All four DCB stores implement the same contract. Rationale in [ADR 56](doc/architecture/decisions/0056-composable-dcb-read-selection.md).
* Added the `dcb-patterns` example module, a catalog covering the remaining [dcb.events](https://dcb.events/examples/) patterns not already shown by `course-enrollment`, `hotel-booking`, and `word-guessing-game`: unique username, idempotency, dynamic product price, opt-in token, and gapless invoice numbers. Kotlin, in-memory, no Docker.
* The BOM now includes version management for the synchronous-subscription modules (`occurrent-subscription-synchronous-blocking` and `occurrent-subscription-synchronous-reactor`) and the native-transaction and reactor application-service modules (`occurrent-application-service-transaction-mongodb-native` and `occurrent-application-service-reactor`).
* The `ResumeBehavior` and `StartupMode` enums move out of `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, and `@Projection`, where each declared its own identical copy, and become shared top-level types, `org.occurrent.annotation.ResumeBehavior` and `org.occurrent.annotation.StartupMode`. This is a breaking change for 0.30.0 callers referencing a nested enum, for example `Subscription.ResumeBehavior` or `DcbSubscription.StartupMode`. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites these references for you. The `StartPosition` enum is hoisted the same way and becomes `org.occurrent.annotation.StartPosition`, shared by `@Subscription`, `@DcbSubscription`, `@Projection`, and `@Snapshot`, and `@DcbSubscription`'s `DcbStartPosition` is renamed to it (the constants are unchanged). `@StreamSubscription` keeps its own `StartPosition` because its `BEGINNING_OF_TIME` constant genuinely differs. The `Capability` and `Mode` enums shared between `@Projection` and `@Snapshot` are hoisted to `org.occurrent.annotation.Capability` and `org.occurrent.annotation.Mode` as well (those two only exist on the unreleased annotations, so they are not a breaking change). See the [upgrade guide](doc/migration/upgrading-to-0.31.0.md) and rationale in [ADR 60](doc/architecture/decisions/0060-unify-resumebehavior-and-startupmode-enums.md), and the Breaking changes section below.
* The four subscription checkpoint-storage modules are renamed from `-position-storage` to `-checkpoint-storage`, so the module coordinate matches the `CheckpointStorage` type it ships. 0.30.0 renamed the `SubscriptionPosition` type family to `Checkpoint` (ADR 46), including these adapter classes, but left the module coordinates saying `position-storage`. The four artifacts are `occurrent-subscription-mongodb-native-blocking-checkpoint-storage`, `occurrent-subscription-mongodb-spring-blocking-checkpoint-storage`, `occurrent-subscription-mongodb-spring-reactor-checkpoint-storage`, and `occurrent-subscription-redis-spring-blocking-checkpoint-storage` (was `-position-storage`). The `org.occurrent` groupId, the packages, and the classes are unchanged, so this is a coordinate-only change. This is a breaking change for anyone depending on the old coordinates. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites them for Maven and Gradle, the mapping is in the [upgrade guide](doc/migration/upgrading-to-0.31.0.md), and the rationale is in [ADR 65](doc/architecture/decisions/0065-rename-checkpoint-storage-module-coordinates.md).
* `EventMetadata` moves from `org.occurrent.dsl.subscription.EventMetadata` (module `dsl/subscription-dsl/common`) to `org.occurrent.cloudevents.EventMetadata` (module `cloudevents-extension`), and is rewritten from a Kotlin `data class` to a plain Java class. [ADR 68](doc/architecture/decisions/0068-first-grade-event-metadata-in-the-dsls.md) made it the shared metadata currency for the saga, projection, and view DSLs, so it had outgrown the subscription module its name and package still pointed at, and `cloudevents-extension` already owns the CloudEvent extension keys it reads. This is a breaking change for a 0.30.0 caller referencing the old FQN, importing it, or using the Kotlin-only surface (reified `get<T>`, operator `get`, `copy`), which is dropped in the rewrite because it was essentially unused. The typed accessors (`getStreamId`, `getStreamVersion`, `getPosition`, `getData`, `empty()`, `from(CloudEvent)`) are preserved with identical behavior. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites every affected reference for you. See [the upgrade guide](doc/migration/upgrading-to-0.31.0.md) and rationale in [ADR 71](doc/architecture/decisions/0071-relocate-eventmetadata-to-cloudevents-extension.md).

* The Spring Boot annotation machinery moves out of the MongoDB starters into store-neutral modules, so a second event store can reuse it instead of copying it. `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@SynchronousSubscription`, `@Projection`, `@Snapshot` and `@Saga` are registered by the same code as before, now published as `occurrent-blocking-spring-boot-autoconfigure` and `occurrent-reactor-spring-boot-autoconfigure`, with the shared pieces in `occurrent-spring-boot-autoconfigure` (renamed from `occurrent-mongodb-spring-boot-autoconfigure`). Nothing changes for an application that uses a starter: `@EnableOccurrent` and `@EnableOccurrentReactive` stay where they are, every annotation behaves identically, and every `occurrent.*` property keeps its name, since the properties class binds a hard-coded prefix. Two things do change for a 0.30.0 caller, and the `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites both for you: `OccurrentProperties` and the seven other public types in `org.occurrent.springboot.mongo.common` move to `org.occurrent.springboot.common`, and the autoconfigure artifact changes coordinate. The zero-config MongoDB defaults behind a store-less `@Projection`, `@Snapshot` or `@Saga` still work, supplied by the MongoDB starter through new provider seams rather than hardcoded, which is what lets a SQL starter offer its own. Resolves [#409](https://github.com/johanhaleby/occurrent/issues/409). See [the upgrade guide](doc/migration/upgrading-to-0.31.0.md) and rationale in [ADR 72](doc/architecture/decisions/0072-store-neutral-spring-boot-annotation-modules.md).

#### Breaking changes

* The annotation start-position, resume, and startup enums move to shared top-level types in `org.occurrent.annotation`, so a 0.30.0 caller that references a nested enum has to update the reference. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites every affected reference for you. See the [upgrade guide](doc/migration/upgrading-to-0.31.0.md).
  * `ResumeBehavior` and `StartupMode` move out of `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, and `@Projection` to `org.occurrent.annotation.ResumeBehavior` and `org.occurrent.annotation.StartupMode`.
  * `StartPosition` on `@Subscription` and `@Projection`, and `@DcbSubscription`'s `DcbStartPosition`, become the shared `org.occurrent.annotation.StartPosition` (the `BEGINNING`, `NOW`, `DEFAULT` constants are unchanged). `@StreamSubscription` keeps its own `StartPosition` because its `BEGINNING_OF_TIME` constant differs.
  * Migration summary:
    * Before: `@Subscription(resumeBehavior = Subscription.ResumeBehavior.DEFAULT)`
    * After: `@Subscription(resumeBehavior = ResumeBehavior.DEFAULT)`
    * Before: `@DcbSubscription(startAt = DcbSubscription.DcbStartPosition.BEGINNING)`
    * After: `@DcbSubscription(startAt = StartPosition.BEGINNING)`
* The four subscription checkpoint-storage module coordinates are renamed from `-position-storage` to `-checkpoint-storage`, finishing the `SubscriptionPosition` to `Checkpoint` rename (ADR 46) that 0.30.0 applied to the types but not the module coordinates. A consumer depending on one of them updates its coordinate, and the same `org.occurrent.UpgradeToOccurrent_0_31` recipe rewrites it. See the [upgrade guide](doc/migration/upgrading-to-0.31.0.md) and [ADR 65](doc/architecture/decisions/0065-rename-checkpoint-storage-module-coordinates.md).
  * `occurrent-subscription-mongodb-native-blocking-position-storage` -> `occurrent-subscription-mongodb-native-blocking-checkpoint-storage`
  * `occurrent-subscription-mongodb-spring-blocking-position-storage` -> `occurrent-subscription-mongodb-spring-blocking-checkpoint-storage`
  * `occurrent-subscription-mongodb-spring-reactor-position-storage` -> `occurrent-subscription-mongodb-spring-reactor-checkpoint-storage`
  * `occurrent-subscription-redis-spring-blocking-position-storage` -> `occurrent-subscription-redis-spring-blocking-checkpoint-storage`
* `EventMetadata` moves from `org.occurrent.dsl.subscription.EventMetadata` to `org.occurrent.cloudevents.EventMetadata`, and from a Kotlin `data class` to a plain Java class. A 0.30.0 caller referencing the old FQN, importing it, or using its Kotlin-only surface (reified `get<T>`, operator `get`, `copy`, all dropped) updates the reference. The typed accessors are unchanged in name and behavior. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites every affected reference for you. See the [upgrade guide](doc/migration/upgrading-to-0.31.0.md) and [ADR 71](doc/architecture/decisions/0071-relocate-eventmetadata-to-cloudevents-extension.md).
  * Before: `import org.occurrent.dsl.subscription.EventMetadata`
  * After: `import org.occurrent.cloudevents.EventMetadata`
* `OccurrentProperties` and the seven other public types in `org.occurrent.springboot.mongo.common` move to `org.occurrent.springboot.common`, and the autoconfigure artifact is renamed from `occurrent-mongodb-spring-boot-autoconfigure` to `occurrent-spring-boot-autoconfigure`. Property keys are unchanged. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites every affected reference and the module coordinate for you. See the [upgrade guide](doc/migration/upgrading-to-0.31.0.md) and [ADR 72](doc/architecture/decisions/0072-store-neutral-spring-boot-annotation-modules.md).
  * Before: `import org.occurrent.springboot.mongo.common.OccurrentProperties`
  * After: `import org.occurrent.springboot.common.OccurrentProperties`
  * Before artifactId: `occurrent-mongodb-spring-boot-autoconfigure`
  * After artifactId: `occurrent-spring-boot-autoconfigure`

### 0.30.0 (2026-07-13)

#### Highlights

This release adds Dynamic Consistency Boundary (DCB) support. DCB lets a command enforce a consistency rule that spans more than one entity without forcing those entities into a single stream or aggregate. You describe the events the decision depends on as a query over DCB tags and CloudEvent types, read them, and append the new events on the condition that nothing matching that query has changed since the read. That gives optimistic concurrency across a boundary you define per decision rather than per stream.

It is useful when a rule crosses entities. Enrolling a student in a course depends on both the course, for its capacity, and the student, for how many courses they are already in. Modeling that as one aggregate is awkward, and splitting it into two streams loses the cross-entity guarantee. With DCB the enrollment command reads both through one query and appends only if neither changed in the meantime.

DCB is a capability layered on the existing CloudEvent storage, not a new store or a new event format. A DCB event is a normal CloudEvent with `dcbtags` and the shared global `position`, so stream consumers and subscriptions still see it. To support it the event store gained an explicit capability set (`STREAM`, `DCB`, or both), a query and append-condition model with a consistency token for the optimistic check, an application service that runs the read, decide, and append cycle, a DSL for queries and subscriptions, a `@DcbSubscription` annotation, and catch-up that replays by `position`. The default stays stream-only, so existing applications are untouched.

#### Changes

* Every published artifact now carries an `occurrent-` prefix, for example `subscription-inmemory` is now `occurrent-subscription-inmemory`. The `org.occurrent` groupId is unchanged and this is a coordinate-only change, so no packages or types moved. The two Spring Boot starters also moved to Spring's third-party convention, so `spring-boot-starter-mongodb` is now `occurrent-mongodb-spring-boot-starter` and `spring-boot-starter-mongodb-reactive` is now `occurrent-mongodb-reactive-spring-boot-starter`. Unpublished modules (the aggregator POMs, `test-support`, and the examples) keep their names. This is a breaking change to every dependency coordinate. The OpenRewrite recipe rewrites them for Maven and Gradle, the full old-to-new mapping is in the [upgrade guide](doc/migration/upgrading-to-0.30.0.md), and the rationale is in [ADR 55](doc/architecture/decisions/0055-uniform-occurrent-artifact-coordinate-naming.md).
* The write side of the API moved from `Stream`/`Sequence` to `List`, while reads and queries stay lazy. This is a breaking change to the released stream API. The original `Stream` write API was chosen for a lazy insert that never materialized (every store collected the stream on its first line, and an append is a bounded transaction that may retry), so this drops that ceremony and matches the DCB `append(List<CloudEvent>)` that already existed. Rationale and scope are in [ADR 54](doc/architecture/decisions/0054-list-instead-of-stream-for-event-store-writes.md). The full surface:
  * Event store (blocking): `EventStore.write(String, List<CloudEvent>)` and `write(String, WriteCondition, List<CloudEvent>)` replace the `Stream<CloudEvent>` overloads (the single `CloudEvent` and `expectedStreamVersion` convenience overloads are unchanged). The reactor `write(String, Flux<CloudEvent>)` is untouched.
  * Application service (blocking and reactor): the domain function is now `Function<List<E>, List<E>>` instead of `Function<Stream<E>, Stream<E>>`, on `ApplicationService.execute(...)` and `DcbApplicationService.execute(...)`. The post-append side effect is `Consumer<List<E>>` on the blocking stack and `Function<List<E>, Mono<Void>>` on the reactor stack (`ExecuteOptions`/`DcbExecuteOptions`). Only the synchronous decision function changed, the reactive `Mono`/`Flux` I/O is unchanged.
  * Cloud event converter: `CloudEventConverter.toCloudEvents(List<T>)` returns `List<CloudEvent>` (was `Stream`). The read-direction `toDomainEvents(Stream<CloudEvent>)` still returns `Stream`.
  * Reads stay lazy and are unchanged: `EventStoreQueries.query`/`all`, `PositionOrderedReader.readInPositionOrder`, `EventStream.events()` (blocking `Stream`), the query DSLs (`DomainEventQueries`, `DcbDomainEventQueries`, including their Kotlin `queryForSequence` helpers), and the reactor `Flux` reads.
  * Kotlin: `executeSequence` and the blocking `executeList` are both removed, call `execute { events: List<E> -> ... }` instead (the `(List<E>) -> List<E>` lambda binds to the Java `ApplicationService.execute(..., Function<List<E>, List<E>>)` member). `sideEffectOnSequence` is removed, use `sideEffectOnList`. The `write(String, Sequence<CloudEvent>)` extensions are removed, call the Java `write(String, List<CloudEvent>)` with `listOf(...)`. The module DSL `command(... Sequence ...)` overloads are removed in favour of the `List` overload. The DCB `DcbApplicationService` keeps a Kotlin `executeOrNull(DcbCriteria, ...)` extension, which unwraps the Java `Optional<DcbAppendResult>` to a nullable result that no member provides.
  * Command composition: `CommandConversion` and `StreamCommandComposition` are removed. Compose with `ListCommandComposition` (or the Kotlin `List`-based `andThen`/`composeCommands`), and pass a `Function<List<E>, List<E>>` straight to `execute`, so the `toStreamCommand`/`toListCommand`/`toSequenceCommand` adapters are no longer needed. `PartialFunctionApplication` is unchanged.
  * View DSL: `View.evolve`/`evolveAll`/`evolveFrom` gained `List<E>` and `Iterable<E>` overloads. The `Stream<E>` (Java) and `Sequence<E>` (Kotlin) forms are retained, and every collection form delegates to the `List` fold, so existing view code is unaffected.
  * In-memory: `InMemoryEventStore`'s post-write listener constructor parameter and `InMemorySubscriptionModel` now take `Consumer<List<CloudEvent>>` (was `Consumer<Stream<CloudEvent>>`).
  * `@JvmName` annotations that existed only to disambiguate a `Stream`/`Sequence` overload from its `List` twin are removed along with those overloads.
* The synchronous side-effect utility `PolicySideEffect` was renamed to `SideEffect`, with `executePolicy` becoming `executeSideEffect` and `andThenExecuteAnotherPolicy` becoming `andThenExecuteAnotherSideEffect`, in `org.occurrent:application-service-blocking` and `org.occurrent:application-service-reactor` (the reactor Kotlin extension moves accordingly). This is a breaking change for callers of those names. "Policy" was a poor fit: the type is a side-effect that runs synchronously right after a command's events are written, and the DSL already exposes it as `ExecuteOptions.sideEffect(...)`/`DcbExecuteOptions.sideEffect(...)`, so the concept now has one consistent name. It also stops overloading "subscription", which in Occurrent is the asynchronous, checkpointed counterpart, the opposite of this synchronous side-effect.
* The integration tests now run against MongoDB 8.0 instead of the end of life MongoDB 4.2, raising the MongoDB version Occurrent is validated against. As part of this the MongoDB event stores now treat error code 86 (IndexKeySpecsConflict) the same as error code 85 (IndexOptionsConflict) when an operator has created an incompatible stream version index out of band, since MongoDB 7.0 and later report that clash as 86. Combining a natural sort step with other sort steps in a MongoDB query now throws `IllegalArgumentException` instead of silently reducing to natural order alone, which is what MongoDB 4.x did. This is a minor breaking change for anyone who built such a sort, since natural order is already a total ordering and MongoDB 7.0 and later reject the combination server side anyway.
* The Spring blocking subscription now backs off on restart matching the reactor and native models.
* The MongoDB event stores now create two additional compound indexes when the DCB capability is enabled, `type_1_position_1` and `dcbTags_1_position_1`. A type only DCB read had no index to match both its type filter and its position sort, so it fell back to the position index and filtered out non matching types after the fact, examining every DCB event in range. A large tag boundary read had the same problem the other way, since the tag index cannot provide the position sort, so MongoDB sorted the matches in memory. Both compound indexes let the planner satisfy the filter and the position sort in a single index scan. Verified with explain on skewed datasets, in each case docsExamined or an in memory SORT stage dropped to match nReturned once the index was in place.
* `Decider.compose` now requires at least two deciders, matching its list overload. It previously accepted zero or one element and produced a degenerate composite, and now throws `IllegalArgumentException` for fewer than two. This is a breaking change for callers relying on the old zero or one element behavior.
* Blocking catch-up now fails loudly instead of silently dropping events when the resume token is unavailable.
  `StreamCatchupSubscriptionModel` and `DcbCatchupSubscriptionModel` used to fall back to `StartAt.subscriptionModelDefault()`
  when the delegated subscription model reported no resume token after a long replay, which silently dropped every
  event committed during that replay. Both now throw an `IllegalStateException` instead, matching the reactor catch-up
  pipeline's existing fail-loud handover. The shared position-windowed replay used by both the stream and DCB position
  paths was also extracted into one class, `isRunning` now reports an in-progress replay correctly, `stop()` interrupts
  an in-flight replay, `pauseSubscription` on a subscription still replaying is honored once it hands over to live
  delivery, `CatchupSubscriptionModelConfig.equals`/`hashCode` now include every configurable field, the in-memory
  handover cache is synchronized to match the reactor stack, and the time-based reconciliation delta is now read in
  bounded windows instead of one unbounded list.
* The catch-up-to-live handover dedup now scales to the during-replay overlap with a configurable ceiling, so a large
  rebuild with heavy concurrent writes gets far fewer duplicate deliveries while delivery stays at-least-once. The
  dedup id set previously capped at a fixed size (1000 in the reactor models, 100 in the blocking models) and, once the
  overlap the live subscription re-delivers grew past that cap, evicted ids the live stream then re-delivered as
  duplicates. It now grows to cover that overlap (bounded by the write volume during the replay, not by total history)
  up to a ceiling that defaults to 100000 and is configurable through the catch-up config. Exceeding the ceiling only
  yields extra duplicates, never loss, and dedup stays keyed by id so a low-position event that commits late is still
  delivered by the live stream. Both the reactor and blocking stream and DCB catch-up paths get this.
* Catch-up reconcile no longer livelocks under sustained writes. The reconciliation pass that drains events written
  during the bulk replay used to re-read the store head after every window and keep paging until the head stopped
  advancing, so a continuous write rate meant it never handed over to live delivery. It now snapshots the head once at
  reconcile start and drains up to that snapshot, then hands over to live. Anything committed after the snapshot is
  delivered by the live subscription, which resumes from the pre-bulk token, so the change loses nothing. Both the
  reactor and blocking stream and DCB catch-up paths get this.
* Occurrent now requires Java 21 instead of Java 17. This raises the minimum JDK needed to build and run Occurrent. Stored data is unaffected, so an existing application only needs to move its runtime to Java 21.
* Modernized Java dispatch code to use Java 21 pattern matching and exhaustive switches across sealed filters,
  criteria, start positions, checkpoints, deadlines, and examples.
* Added package-level JSpecify nullness defaults across the Java modules and tightened Kotlin wrappers around nullable
  state, retry results, and optional CloudEvent time and subject values.
* Fixed `RetryStrategy.none().execute(Function<RetryInfo, T>)` so the function receives a non-null first-attempt
  `RetryInfo` instead of `null`, and corrected the `Function` overload documentation and null-check message.
* Renamed `OccurrentSubscriptionFilter` to `StreamSubscriptionFilter` to make the stream-scoped subscription marker
  explicit next to `AgnosticSubscriptionFilter` and `DcbSubscriptionFilter`. This is a breaking API change for callers
  that construct subscription filters directly.
* Blocking catch-up subscriptions now run their replay handoff work on Java virtual threads instead of the common
  `ForkJoinPool`, avoiding common-pool starvation from blocking event-store reads and subscriber callbacks. The Spring
  Boot Mongo starter also honors `spring.threads.virtual.enabled=true` for its blocking Mongo subscription executor.
* Fixed example-profile compilation after recent DSL and query cleanup. The RPS decider web example now uses the stream
  subscription DSL expected by the view DSL, and the course-enrollment student management use case points at the renamed
  DCB query helper.
* `@Subscription` and the `Subscriptions` DSL, which have always driven stream subscriptions, are now
  capability-neutral. On a store with both `STREAM` and `DCB` capabilities they deliver both stream-written and
  DCB-appended events, filtered only by event type, with catch-up over the unified global position and resume via
  `GlobalCheckpoint`. `@StreamSubscription` and `@DcbSubscription` are the explicit capability-scoped forms. This is
  safe for existing applications because the neutral form only ever also sees DCB events on a DCB-enabled store, and
  those are new since DCB is unreleased. On a stream-only store it behaves exactly as `@Subscription` did before.
  * See [ADR 51](doc/architecture/decisions/0051-capability-agnostic-subscription.md).
* Added `DcbDecider`, which couples a `Decider` with its DCB read boundary and its event tags on one object, so a
  feature can describe its own read boundary and write tags right next to its decision logic instead of assembling
  them separately at the call site. The read boundary is derived from the command through a
  `Function<C, DcbCriteria>`, and the write tags come from a `TagGenerator<E>`. The DSL gains `execute(command, dcbDecider)`
  in both the blocking and reactor variants. `DcbDecider` composes the same way `Decider` does, through `adapt` and
  `compose`, with the composed criteria being `DcbCriteria.anyOf` over the recognizing children and the composed tags
  being the union of the children's tags. The global `TagGenerator` on the DCB application service is now optional,
  and `DcbExecuteOptions` gained a per-execute `TagGenerator` that overrides it. The Spring starters now
  auto-configure the `DcbApplicationService` even when no global `TagGenerator` bean exists, so a decider-only
  application needs none, while `@DcbTag` and raw-execute users still get a global tagger when one is present.
  * See [ADR 52](doc/architecture/decisions/0052-couple-decider-with-dcb-boundary-and-tags.md).
* Renamed the `SubscriptionPosition` type family to `Checkpoint` to stop overloading "position" for two different
  concepts: the ordering value (`position`) and the per-subscriber resume marker built from it. This is a breaking
  API change; there is no deprecated alias.
  * `SubscriptionPosition` -> `Checkpoint`, `GlobalSubscriptionPosition` -> `GlobalCheckpoint`,
    `StringBasedSubscriptionPosition` -> `StringBasedCheckpoint`, `TimeBasedSubscriptionPosition` -> `TimeBasedCheckpoint`,
    `MongoResumeTokenSubscriptionPosition` -> `MongoResumeTokenCheckpoint`,
    `MongoOperationTimeSubscriptionPosition` -> `MongoOperationTimeCheckpoint`.
  * `SubscriptionPositionStorage` -> `CheckpointStorage`, and its Mongo/Redis implementations
    (`SpringMongoSubscriptionPositionStorage`, `NativeMongoSubscriptionPositionStorage`,
    `ReactorSubscriptionPositionStorage`, `SpringRedisSubscriptionPositionStorage`) renamed to `*CheckpointStorage`.
  * `PositionAwareSubscriptionModel` -> `CheckpointAwareSubscriptionModel`, `globalSubscriptionPosition()` ->
    `globalCheckpoint()`. `PositionAwareCloudEvent` -> `CheckpointAwareCloudEvent`,
    `getSubscriptionPosition()`/`hasSubscriptionPosition(...)`/`getSubscriptionPositionOrThrowIAE(...)` ->
    `getCheckpoint()`/`hasCheckpoint(...)`/`getCheckpointOrThrowIAE(...)`.
  * `StartAt.subscriptionPosition(...)` -> `StartAt.checkpoint(...)`.
  * The catch-up `SubscriptionPositionStorageConfig` sealed type and its records/factories renamed to
    `CheckpointStorageConfig` (`dontUseSubscriptionPositionStorage()` -> `dontUseCheckpointStorage()`,
    `useSubscriptionPositionStorage(...)` -> `useCheckpointStorage(...)`, and the
    `andPersistSubscriptionPositionDuringCatchupPhase*` builder methods -> `andPersistCheckpointDuringCatchupPhase*`).
  * The Spring Boot autoconfiguration `occurrentSubscriptionPositionStorage` beans are now named
    `occurrentCheckpointStorage`.
  * See [ADR 46](doc/architecture/decisions/0046-rename-subscription-position-to-checkpoint.md).
* Migrated the generic Mongo checkpoint document field from `subscriptionPosition` to `checkpoint`, with a
  backward-compatible read: `MongoCommons` falls back to the legacy `subscriptionPosition` field when `checkpoint`
  is absent, so an existing subscription resumes correctly on first read after upgrading. All Mongo checkpoint
  storage adapters persist by replacing the whole document, so the first save after upgrade rewrites it under the
  new `checkpoint` field and the legacy field does not survive.
* Added a global, monotonic `position` to every event, stream and DCB alike, giving all consumers a single
  ordering axis across the whole store.
  * `position` is intrinsic to DCB. For a STREAM-only store it is a
    stream-scoped option that is on by default, so new stores get a global position out of the box. Opt out with
    `EventStoreConfig.withoutStreamPosition()` (blocking, native, and reactor builders) or
    `occurrent.event-store.stream.position=false` in Spring, for a store such as `entity-history` that only ever
    reads one stream at a time and never wants a global order. A combined `STREAM` and `DCB` store cannot opt out,
    since a combined store must position everything it writes.
  * Stream catch-up now reconciles on position, the same range-based mechanism DCB catch-up already used, instead
    of wall-clock time or `$natural` order, for any store that writes position. This is what closes the clock-skew
    data-loss bug class from #199 for streams, not just for DCB. A STREAM-only store that opts out of position
    keeps the previous time-based catch-up unchanged. On the blocking stack a subscription that starts at a specific
    wall-clock time still uses the time-based catch-up even on a position store, since a timestamp has no position to
    map to. Beginning-of-time and position starts use the position path.
  * Reactive `@StreamSubscription` can now replay history. The new `ReactorStreamCatchupSubscriptionModel` mirrors
    the existing reactive DCB catch-up model. It fails loud only for a store that has opted out of stream position.
  * `DomainEventQueries` (blocking and reactor) gained position-range reads, so a consumer can read events across
    the whole store by `position` directly, without going through a subscription, and reconcile stream and DCB
    consumers on one axis.
  * `EventMetadata.position` is now a general accessor available on subscribed events from both stacks, not only
    DCB events.
  * `EventMetadata` moved from `org.occurrent.dsl.subscription.blocking` to `org.occurrent.dsl.subscription` so the
    blocking and reactive subscription DSLs share one type. This is a breaking change, the old
    `org.occurrent.dsl.subscription.blocking.EventMetadata` is gone, so update the import to
    `org.occurrent.dsl.subscription.EventMetadata`.
  * **Upgrade hazard: read this before upgrading an existing deployment.** Because stream position defaults on, an
    existing deployment that upgrades in place gets position on new stream events but not on the events already in
    its collection. The store detects this on startup and logs a loud warning naming the migration runbook, with a
    config flag to make it a hard failure instead
    (`EventStoreConfig.Builder#requireBackfilledPosition` /
    `occurrent.event-store.position.require-backfilled-position`). A new module,
    `eventstore/migration/position-backfill`, is a throttled, resumable, idempotent backfill tool that seeds the
    position counter and backfills existing events in `_id` order. Follow
    [the migration runbook](doc/runbooks/position-backfill.md) before relying on position-based catch-up against an
    existing deployment. A store with no existing events, or a brand-new deployment, needs no backfill.
  * As an extra guard for that upgrade path, when stream position is only on by default (not enabled explicitly) and
    a MongoDB store starts against a collection that already holds events without a `position`, the store turns
    stream position off for itself and logs how to turn it on. This avoids building the `position` index over a large
    existing collection at startup, and it keeps working exactly as before the upgrade. An explicit
    `withStreamPosition()` (or `occurrent.event-store.stream.position=true`) is always honored, and DCB always writes
    position. Catch-up follows the store, so a store guarded off this way stays on the legacy time-based path until
    you enable position and backfill.
  * A combined `STREAM` and `DCB` reactive store now replays both stream and DCB history. One dual-mode catch-up
    model routes each subscription to the stream or DCB path by its filter and start position, matching the blocking
    stack. Stream catch-up no longer needs the DCB API on the classpath, on either stack. Stream catch-up moved into
    a new DCB-free `stream-catchup-subscription` module, and the general `CatchupSubscriptionModel` dispatcher stays
    in the `catchup-subscription` module, so a STREAM-only user (such as `entity-history`) can depend on stream
    catch-up without the DCB API.
  * See [ADR 45](doc/architecture/decisions/0045-unified-global-position.md).
* Fixed a bug where a live `NOW`/`DEFAULT` subscription on `ReactorMongoSubscriptionModel` could redeliver the event written just before the subscription started.
  * `globalCheckpoint()` used the server's raw operation time as the resume position instead of bumping it past the last written event, so a write landing on the same BSON timestamp as the resume point could be replayed. It now increments the timestamp's increment field by 1, the same fix `SpringMongoSubscriptionModel` already had.

* Added a reactive (Project Reactor) Spring Boot starter, `spring-boot-starter-mongodb-reactive`, alongside the blocking `spring-boot-starter-mongodb`, so a reactive application gets the same auto-configuration and annotation-driven subscriptions.
  * Enable it with `@EnableOccurrentReactive` and the new dependency. It auto-configures the reactive event store, transaction manager, application service (stream and DCB), query DSLs, subscription model, checkpoint storage, and the reactive `StreamSubscriptions` and `DcbSubscriptions` DSLs, driven by the same `occurrent.*` properties and capability set as the blocking starter.
  * `@StreamSubscription` and `@DcbSubscription` work on the reactive stack, with handler methods returning `Mono<Void>`. `@StreamSubscription` supports history replay when stream position is enabled, plus `NOW` and `DEFAULT` for live delivery and durable resume. `@DcbSubscription` replays by `position` through the reactive DCB catch-up model, same as blocking.
  * The stack-neutral autoconfiguration (`OccurrentProperties`, the Jackson3 `CloudEventConverter` configuration, the capability conditions) moved into a shared `spring-boot-autoconfigure-mongodb-common` module that both starters depend on. `OccurrentProperties` moved package from `org.occurrent.springboot.mongo.blocking` to `org.occurrent.springboot.mongo.common`; the `occurrent.*` property keys are unchanged.
  * Supporting reactive infrastructure: a reactive `StreamSubscriptions` DSL (`subscription-dsl-reactor`), named lifecycle subscribe on the reactive `DcbSubscriptions` DSL, and `ReactorDurableSubscriptionModel`/`ReactorDcbCatchupSubscriptionModel` now implement the reactive `Subscribable`/`CheckpointAwareSubscriptionModel`/`SubscriptionModelLifeCycle` interfaces so they compose into one `Durable(Catchup(mongo))` model. `ReactorDurableSubscriptionModel`'s previous `Mono<Void> subscribe(id, action)`, which only started the subscription once the caller subscribed to the returned `Mono` and returned no lifecycle handle, is replaced by the `Subscription`-returning `Subscribable` API, which starts the subscription immediately and hands back a handle to manage it.
  * There is no reactive competing-consumer model, so the reactive subscription model is not competing-consumer wrapped.
  * See [ADR 44](doc/architecture/decisions/0044-reactive-spring-boot-starter.md).

* Hardened `NativeMongoSubscriptionModel` against the MongoDB operational failures `SpringMongoSubscriptionModel` already survives in production: replica-set failovers, transient network errors, and change-stream history loss.
  * A change-stream error (a failover, a transient network error, or anything else the driver itself does not resume) now restarts the subscription with the existing `RetryStrategy` backoff instead of silently dying, resuming from the position of the last change-stream document read so recovery is gap-free rather than a replay or a skipped window.
  * `MongoCommandException`s with error code 286 (`ChangeStreamHistoryLost`) restart from `StartAt.now()` only when configured to via the new `NativeMongoSubscriptionModelConfig.restartSubscriptionsOnChangeStreamHistoryLost(true)` (default `false`, matching the Spring default); otherwise the subscription stops and logs an error, same as `SpringMongoSubscriptionModel`.
  * `resumeSubscription` now continues from the position of the last change-stream document read before the pause instead of the subscription's original `StartAt`, so pausing and resuming neither replays events nor drops events written while paused.
  * See [ADR 41](doc/architecture/decisions/0041-native-mongodb-subscription-model-restart-on-error.md).

* Hardened `ReactorMongoSubscriptionModel` against the MongoDB operational failures `SpringMongoSubscriptionModel` already survives in production: replica-set failovers, transient network errors, and change-stream history loss.
  * A change-stream error (a failover, a transient network error, or anything else the driver itself does not resume) now retries with exponential backoff instead of terminating the `Flux`, resuming from the position of the last change-stream document read so recovery is gap-free rather than a replay or a skipped window.
  * `MongoCommandException`s with error code 286 (`ChangeStreamHistoryLost`) restart from `StartAt.now()` only when configured to via the new `ReactorMongoSubscriptionModelConfig.restartSubscriptionsOnChangeStreamHistoryLost(true)` (default `false`, matching the Spring default); otherwise the subscription stops and logs an error, same as `SpringMongoSubscriptionModel`.
  * The `subscribe(filter, startAt) -> Flux<CloudEvent>` contract is unchanged, this is purely additive.
  * See [ADR 42](doc/architecture/decisions/0042-reactive-mongodb-subscription-model-resilience.md).

* Added named, lifecycle-managed subscriptions to `ReactorMongoSubscriptionModel`, so the reactive stack has the same subscription lifecycle as the blocking one.
  * New `Subscribable` and `SubscriptionModelLifeCycle` interfaces in `subscription-api-reactor` mirror their blocking counterparts: `subscribe(subscriptionId, filter, startAt, action)` returning a `Subscription`, plus `pauseSubscription`, `resumeSubscription`, `cancelSubscription`, `isRunning`, `isPaused`, `start`, `stop`, and `shutdown`. The action is `Function<CloudEvent, Mono<Void>>`, matching `ReactorDurableSubscriptionModel`'s existing convention.
  * The new reactive `Subscription` interface's `waitUntilStarted()` returns a `Mono<Void>` instead of blocking, with a `Mono<Boolean>` timeout variant.
  * `resumeSubscription` continues from the position of the last event delivered before the pause, the same gap-free guarantee the change-stream error recovery from the previous change has.
  * `ReactorMongoSubscriptionModel` now implements `CheckpointAwareSubscriptionModel, Subscribable, SubscriptionModelLifeCycle`. The existing `subscribe(filter, startAt) -> Flux<CloudEvent>` primitive is unchanged, this is purely additive.
  * See [ADR 43](doc/architecture/decisions/0043-reactive-mongodb-subscription-lifecycle-parity.md).

* Added a reactive query DSL, so the reactive stack has the same typed-query ergonomics as the blocking one, and the reactive DCB DSL's domain event queries now delegate to it.
  * `DomainEventQueries` in `query-dsl-reactor` wraps a reactive `EventStoreQueries` and a `CloudEventConverter`, returning `Flux<E>` from its query methods and `Mono<E>` from `queryOne`, with the same `Class`/`Filter`/`SortBy` overloads and Kotlin `KClass` extensions as the blocking version.
  * `DcbDomainEventQueries` in `dcb-dsl-reactor` now wraps a `DomainEventQueries<E>` and delegates every plain stream-query method to it, exactly like the blocking version, instead of only exposing the DCB query family. This is a breaking change to its constructor: `new DcbDomainEventQueries(DcbEventStore, CloudEventConverter)` no longer compiles, build a `DomainEventQueries<E>` first and pass that instead.
  * See [ADR 40](doc/architecture/decisions/0040-reactive-query-dsl-and-dcb-domain-event-queries-delegation.md).

* Added a reactive stream application service, so the reactive stack has the same application-service ergonomics as the blocking one.
  * `ApplicationService` in `application-service-reactor` runs the read, decide, and write cycle against the reactive event store and returns a `Mono<WriteResult>`, retrying from a fresh read on a `WriteConditionNotFulfilledException`. The domain function stays a synchronous `Function<Stream<E>, Stream<E>>`, the side-effect is reactive (`Function<Stream<E>, Mono<Void>>`), and it mirrors the blocking surface including the `ExecuteFilter` read filter and the Kotlin extensions.
  * `ExecuteFilter` moved to the shared `application-service-common` module (package `org.occurrent.application.service`) so both stacks share one copy. This is a breaking change, the old `org.occurrent.application.service.blocking.ExecuteFilter` is gone, so update the import to `org.occurrent.application.service.ExecuteFilter`.
  * See [ADR 39](doc/architecture/decisions/0039-reactive-stream-application-service.md).

* Added reactive DCB catch-up, which completes reactive DCB support.
  * `ReactorDcbCatchupSubscriptionModel` replays DCB history by `position` and hands over to a live subscription, so a reactive read model can be rebuilt from the beginning. It mirrors the blocking DCB catch-up (the live resume token is captured before the replay so an event committing during the replay is still delivered), with id-based handover dedup because the reactive resume token is inclusive. Reactive DCB now matches the blocking stack across the store, application service, query DSL, and subscriptions.
  * See [ADR 38](doc/architecture/decisions/0038-reactive-dcb-catch-up.md).

* Added live reactive DCB subscriptions.
  * A reactive `DcbSubscriptionModel` facade (`subscription-api-reactor`) subscribes to DCB events matching a `DcbCriteria` as a `Flux<CloudEvent>`, filtered server-side, and the reactor DCB DSL gains `DcbSubscriptions` with `Flux<E> subscribe(...)` and `Flux<DcbEvent<E>> subscribeWithMetadata(...)`. Live only for now. The `DcbStartAt` is passed through to the underlying subscription model, and the current reactive models have no DCB catch-up, so a `DcbStartAt.beginning()` behaves like a live start rather than replaying history.
  * See [ADR 37](doc/architecture/decisions/0037-live-reactive-dcb-subscriptions.md).

* Added a reactive DCB DSL (`dcb-dsl-reactor`).
  * Reactive `DcbDomainEventQueries` returns matched domain events as a `Flux` and a `Mono<DcbDomainEventStream>` with the consistency token for a conditional append, built directly on the reactive DCB event store. Kotlin decider extensions run a decider through the reactive DCB application service and return a `Mono`. The live `subscribeDcb` helper and DCB subscription metadata are not part of this and come with reactive DCB subscriptions.
  * See [ADR 36](doc/architecture/decisions/0036-reactive-dcb-dsl.md).

* Added a reactive DCB application service (`application-service-reactor`).
  * `DcbApplicationService` runs the read, decide, and append cycle against the reactive DCB event store and returns a `Mono`, retrying from a fresh read on a DCB conflict. The domain function stays a synchronous `Function<Stream<E>, Stream<E>>`, and the post-append side-effect is reactive (`Function<Stream<E>, Mono<Void>>`). This is the first reactive application service in Occurrent.
  * The raw reactive `DcbApplicationService.execute` returns `Mono<DcbAppendResult>` rather than `Mono<Optional<DcbAppendResult>>`. An empty `Mono` is the reactive representation of no value, so an empty `Mono` now means the domain function produced no new events (a no-op). This matches the Kotlin `execute` decider extension, which already flattened to `Mono<DcbAppendResult>`.
  * See [ADR 35](doc/architecture/decisions/0035-reactive-dcb-application-service.md).

* DCB now works on the reactive Spring MongoDB event store, which completes DCB support across every event store.
  * `ReactorMongoEventStore` implements a new reactive `DcbEventStore` (in `eventstore-api-dcb-reactor`) with `Mono` and `Flux`, reusing the same per-attribute marker model and storage contract as the blocking and native stores. It defaults to stream-only. A reactive DCB application service, DSL, and subscriptions are not part of this and remain to be done.
  * See [ADR 34](doc/architecture/decisions/0034-reactive-spring-mongodb-dcb-support.md).

* DCB now works on the native MongoDB driver event store, not only the Spring store.
  * `MongoEventStore`, the plain synchronous-driver store, implements the same DCB read and append API as the Spring store, with the same per-attribute marker model and consistency-token semantics. The shared model now lives in a new `eventstore-mongodb-dcb-common` module so the two stores cannot drift on the storage contract, and the capability set moved to a shared `EventStoreCapability` enum in `eventstore-api-common`. The native store defaults to stream-only, so existing applications are untouched.
  * See [ADR 33](doc/architecture/decisions/0033-native-mongodb-driver-dcb-parity-via-shared-marker-model.md).

* The stream-only subscription DSL is now named `StreamSubscriptions`.
  * The stream DSL is now `StreamSubscriptions` with builder `streamSubscriptions(...)`, so its name matches `@StreamSubscription`, `StreamSubscriptionModel`, and the DCB counterpart `DcbSubscriptions`. The `Subscriptions` name and its `subscriptions(...)` builder were not retired, they were repurposed as the capability-neutral DSL (see the entry above about `@Subscription` and `Subscriptions` becoming capability-neutral), so a `0.20.5` caller's `subscriptions(...)` keeps compiling and behaves the same on a stream-only store, and picks up DCB events only once a store also enables the `DCB` capability.
  * See [ADR 29](doc/architecture/decisions/0029-rename-subscriptions-dsl-to-stream-subscriptions.md).

* DCB append conditions capture the consistency token for a multi-marker boundary in a single read.
  * The consistency token for a query with more than one marker (for example `tags("t1","t2")`) is captured in a single consistent read, so an append committing between per-marker reads cannot produce a token that masks a real conflict (a write skew). See [ADR 31](doc/architecture/decisions/0031-capture-dcb-consistency-token-in-a-single-read.md).
  * A `DuplicateKeyException` from two transactions first-creating the same conflict marker at once is retried (as the position counter is), so a brand-new tag or type under concurrent appends does not surface a spurious failure.
* A `MatchAll` DCB append condition is a whole-store lock.
  * `DcbCriteria.all()` used as a `DcbAppendCondition` boundary is skew-safe only against other whole-store conditions, not against concurrent scoped appends, so it is meant for single-writer or empty-store guards. See [ADR 30](doc/architecture/decisions/0030-keep-matchall-dcb-append-condition-with-documented-limit.md).

* The Java DCB subscription DSL can wait until a subscription has started.
  * `DcbSubscriptions.subscribe(...)` and `subscribeWithMetadata(...)` take a `waitUntilStarted` boolean. When it is `true` the call blocks until the subscription has started, and for a replaying DCB subscription that means until catch-up completes, matching the Kotlin DSL. The overloads without the flag return without waiting.

* The subscription DSL now derives its default subscription id from the CloudEvent type rather than the domain event class simple name. The reified `subscribe<MyEvent>()` overloads (blocking and reactor, stream and capability-agnostic) default the subscription id to `cloudEventConverter.getCloudEventType(MyEvent::class.java)` through a non-inline helper, so the id follows the configured CloudEvent type rather than the domain class name, never returns null, and a future change to the derivation reaches already-compiled callers. The id is stable across a class rename only when a stable custom `CloudEventTypeMapper` is configured; the default reflection-based type mapper uses the fully-qualified class name, so a rename still changes the type and orphans the checkpoint. The reactor subscription DSL documents that it has no `waitUntilStarted` flag because the returned `Subscription` exposes `waitUntilStarted()` as a `Mono<Void>` the caller composes into their own chain, and the reactor DCB module gains a Kotlin `subscribeDcb` extension for parity with the blocking one. The blocking and capability-agnostic stream `subscribe` overloads without an event-metadata callback now also accept a `waitUntilStarted` flag, so a caller can opt out of blocking on start without switching to the metadata callback shape.

* DCB criteria are built with a fluent API.
  * `DcbCriteria.type("OrderPlaced").tags(Tag.of("order", "1"))` builds one alternative, `DcbCriteria.anyOf(...)` ORs several, and `DcbCriteria.tagsAnyOf(Tag.of("a", "1"), Tag.of("b", "2"))` is the shorthand for an or of single-tag alternatives. A single alternative is itself a `DcbCriteria`.
  * DCB tags are a first-class `Tag` type, so `TagGenerator` returns `Set<Tag>`. Following the DCB specification, a `Tag` is an opaque non-blank string: `Tag.of("order", "1")` builds the common `key:value` form (canonical `"order:1"`), and `Tag.of("premium")` (or `Tag.parse(...)`) builds a value-less marker. The `key:value` and string forms produce the same tag, and matching is by string equality.
  * See [ADR 32](doc/architecture/decisions/0032-fluent-dcb-query-construction.md) and [ADR 47](doc/architecture/decisions/0047-dcb-criteria-tag-type-and-typed-class-construction.md).
* Added an optional annotation-driven `TagGenerator`. Annotate a domain event's fields with `@DcbTag` (in the dependency-free `annotations` jar, `@DcbTag String email` produces the tag `email:<value>`, `@DcbTag("customer") String id` produces `customer:<value>`, and `key` is an alias so `@DcbTag(key = "customer")` means the same) and use `AnnotationTagGenerator` (new `dcb-annotation-taggenerator` module) to derive the tags by reflection, with per-class caching. It works with Java records and Kotlin `data class`es, reading values through accessors (record accessors and getters, made accessible so the event class need not be public) and falling back to the annotated field where no getter exists. It is strictly opt-in: the hand-written `TagGenerator` remains the default, and annotating events couples the domain model to a tiny Occurrent annotation, so teams that want zero coupling keep writing the generator by hand.
  * `AnnotationTagGenerator` can also scan custom runtime annotations. `new AnnotationTagGenerator(MyTag.class)` uses a `String key()` annotation element when present and otherwise falls back to the member name, while `new AnnotationTagGenerator(MyTag.class, keyResolver)` supports annotations whose key is stored in another attribute. Custom annotations are validated up front and must be annotated with `@Retention(RUNTIME)`.
  * The Spring Boot starters auto-configure an `AnnotationTagGenerator` as the default `TagGenerator` only when the module is on the classpath and no `TagGenerator` bean is defined. The module is an optional starter dependency, so it is never dragged in transitively, and behavior is unchanged when it is absent.
  * See [ADR 48](doc/architecture/decisions/0048-annotation-driven-dcb-tag-generator.md).

* Added the `@DcbSubscription` annotation, the declarative DCB counterpart to `@StreamSubscription`.
  * A DCB read model is declared as a single annotated method. `eventTypes` and `tags` express the `DcbCriteria`, and `startAt` (BEGINNING, NOW, DEFAULT) or `startAtDcbPosition` (an explicit position, the DCB counterpart to the stream `startAtTimeEpochMillis`) together with `resumeBehavior` give history replay, resume from the stored position, and an always-replay in-memory mode that disables the competing consumer and checkpoint storage. It routes through the DCB DSL, so it gets the server-side filter, and the method can take the event plus an optional `EventMetadata` or `DcbEventMetadata`. `DcbStartAt` has a `dynamic` factory to back the resume logic. The course-enrollment dashboard subscriber uses `@DcbSubscription` (combining `BEGINNING` with `SAME_AS_START_AT`, since it is an in-memory model rebuilt on every boot).
  * See [ADR 27](doc/architecture/decisions/0027-dcb-subscription-annotation.md).

* A STREAM-and-DCB application catches up both kinds of subscription.
  * `CatchupSubscriptionModel` has a dual-mode constructor that holds both the stream query API and the DCB event store and routes each subscription to the right catch-up. A DCB subscription replays by `position`. A stream subscription also replays by `position` when the stream store writes one, and otherwise keeps the time-based replay path. The Spring Boot starter wires this when the event store has both the STREAM and the DCB capability, so a combined application can rebuild both stream and DCB read models from history.
  * See [ADR 25](doc/architecture/decisions/0025-dual-mode-catch-up-for-stream-and-dcb.md).

* Stream and DCB subscriptions have separate typed start positions and model views.
  * `DcbStartAt` is the DCB counterpart to `StartAt`, expressing only DCB starts (`now`, `subscriptionModelDefault`, `beginning`, `afterPosition`). `DcbSubscriptionModel` and `StreamSubscriptionModel` are typed facades over the shared `SubscriptionModel`, obtained with `from(subscriptionModel)`. A DCB subscription cannot be handed a time-based checkpoint and a stream subscription cannot be handed a DCB-specific start, so a mismatch is a compile-time error rather than a runtime surprise. The shared durable, competing-consumer, and catch-up machinery stays in place, the split is types and thin adapters.
  * `DcbSubscriptions` takes a `DcbStartAt` rather than a generic `StartAt`.
  * See [ADR 24](doc/architecture/decisions/0024-stream-and-dcb-subscription-model-split.md).

* DCB subscriptions filter server-side.
  * `DcbSubscriptionFilter`, wrapping a `DcbCriteria`, is a first-class `SubscriptionFilter` alongside the stream `StreamSubscriptionFilter`. The Spring and native MongoDB subscription models translate it into a change stream `$match`, so a DCB read model that cares about a few event types or a tag boundary receives only the matching events rather than every DCB event. The in-memory model honors it in process. `DcbSubscriptions` subscribes with it and keeps a small in-process check only as a correctness floor for backends that do not filter.
  * Tag containment matches the indexed `dcbTags` array the event store already writes, exposed as the public constant `OccurrentCloudEventMongoDocumentMapper.DCB_TAGS_INDEX_FIELD`.
  * See [ADR 23](doc/architecture/decisions/0023-server-side-filtering-for-dcb-subscriptions.md).

* The DCB subscription DSL can cancel a subscription.
  * `DcbSubscriptions.cancel(subscriptionId)` stops and removes a subscription, so per-connection teardown goes through the DSL instead of reaching into the subscription model. An SSE activity feed, for example, can subscribe when a client connects and cancel when it disconnects, all through `DcbSubscriptions`. The DSL wraps a `SubscriptionModel` rather than a bare `Subscribable` to make this possible.

* DCB subscriptions catch up from history in DCB-only mode.
  * In a DCB-only application the Spring Boot starter wraps the subscription model in a DCB-mode `CatchupSubscriptionModel`, so a subscription started from `DcbStartAt.beginning()` or `DcbStartAt.afterPosition(...)` replays past events by `position` before switching to live delivery. A read model can therefore be rebuilt from history on startup. Started without such a position, a DCB subscription stays live only.
  * Request a replay from the start with `DcbStartAt.beginning()`. A STREAM-and-DCB application uses the dual-mode catch-up route described above.
  * See [ADR 22](doc/architecture/decisions/0022-wire-dcb-catch-up-in-dcb-only-mode.md).

* 0.30.0 ships an OpenRewrite recipe, `org.occurrent:occurrent-rewrite`, that automates the mechanical renames and package moves in this release and the safe part of the `Stream` to `List` write-side migration. See [the migration guide](doc/migration/upgrading-to-0.30.0.md) for the plugin setup and what still needs a manual pass.

#### Details

* `DcbEventStore.append` derives the Occurrent storage stream from the appended events' DCB tags, so callers reason in DCB terms (tags and append conditions) rather than storage stream ids. Placement is configured on the store through a `DcbStreamIdGenerator` (in the `eventstore-api-dcb` module, defaulting to `PartitionedDcbStreamIdGenerator`), set on `InMemoryEventStore` via a constructor and on the Spring Mongo store via `EventStoreConfig.Builder.dcbStreamIdGenerator(..)`.
* Added initial Dynamic Consistency Boundary (DCB) support.
  * New module: `org.occurrent:eventstore-api-dcb`.
  * New core API types include `DcbEventStore`, `DcbCriteria`, `DcbCriterion`, `DcbReadOptions`, `DcbEventStream`, `DcbAppendCondition`, `DcbAppendResult`, `DcbAppendConditionNotFulfilledException`, and `DcbCloudEvents`.
  * `DcbCriteria` is a sealed type, either `DcbCriteria.MatchAll`, `DcbCriteria.Items`, or a single `DcbCriterion`, built through a fluent API (`all()`, `type(..)`/`types(..)`, `tags(..)`, `anyOf(..)`, and `tagsAnyOf(..)`) for an OR across query items, where a single alternative is itself a `DcbCriteria`.
  * `DcbReadOptions` scopes a read with an optional exclusive lower bound (`afterSequencePosition`) and an optional inclusive upper bound (`upToSequencePosition`).
  * `DcbEventStore` exposes `exists(DcbCriteria)` and `count(DcbCriteria)` for checking a boundary without materializing the matching events, plus `exists(DcbCriteria, DcbReadOptions)` and `count(DcbCriteria, DcbReadOptions)` overloads that scope the check to a position window.
  * DCB is implemented as an optional capability over the existing CloudEvent storage model, not as a separate event representation.
  * DCB metadata is stored as CloudEvent extensions:
    * `dcbtags` for canonical DCB tags.
    * `position` for the shared global sequence position.
  * DCB-written events remain normal CloudEvents with Occurrent stream metadata, so existing CloudEvent consumers and subscription models can still observe them.
* Added DCB support to:
  * `InMemoryEventStore`
  * `SpringMongoEventStore`
* Added blocking DCB application-service support.
  * New package: `org.occurrent.application.service.blocking.dcb`.
  * New types: `DcbApplicationService`, `GenericDcbApplicationService`, `TagGenerator`, `DcbStreamIdGenerator`, and `PartitionedDcbStreamIdGenerator`.
  * `GenericDcbApplicationService` reads with a `DcbCriteria`, invokes the domain function, converts new domain events to CloudEvents, adds DCB tags, and appends with a DCB append condition.
  * `DcbExecuteOptions` adds a post-append side-effect, so a policy can run on the newly written events after a successful append, mirroring the stream `ExecuteOptions` side-effect. The side-effect runs once after the append, not on the no-new-events path and not per retry attempt, and the existing `PolicySideEffect` is reused. There is deliberately no read-filter option, because in DCB the `DcbCriteria` is both the read filter and the consistency boundary. Kotlin gets a reified `dcbSideEffect` builder.
  * Kotlin callers get an `executeOrNull` extension on `DcbApplicationService` that returns a nullable `DcbAppendResult?` (null on a no-op command) instead of the Java `Optional<DcbAppendResult>`, with an optional `DcbExecuteOptions`. The Kotlin decider `execute` extensions now return `DcbAppendResult?` as well.
  * The Kotlin decider `execute` extensions widen a decider's event type for you, so a feature decider over its own narrow event type can be passed straight to a `DcbApplicationService` over a broader event type without calling `adapt` or `adaptEvents` first. This matters because the injected `DcbApplicationService` is typically over the whole domain's event type, so a feature decider would otherwise need widening at every call site.
* Added Spring Mongo event-store capabilities.
  * New enum: `SpringMongoEventStoreCapability`.
  * `EventStoreConfig` now accepts a non-empty set of capabilities: `STREAM`, `DCB`, or both.
  * The backward-compatible default is `{STREAM}`.
  * Spring Boot property: `occurrent.event-store.capabilities=stream`, `dcb`, or `stream,dcb`.
  * `SpringMongoEventStore` now creates indexes/support collections based on enabled capabilities and fails fast when callers invoke a disabled API family.
  * The Spring Mongo DCB append path uses query-scoped concurrency. The `position` counter is reserved outside the append transaction, so appends to disjoint boundaries do not contend on a single hot document (`position` may have gaps, which the DCB contract permits). Optimistic concurrency is enforced with a consistency token rather than a position. A read captures a `DcbConsistencyToken` (a distinct type from the `long` sequence position) from the versions of its query's per-attribute conflict markers, and the append fails if those markers advanced since the read. Because marker versions move at commit and never at reservation, this is sound even though the read head can run ahead of committed data, where a position-based check would silently miss a conflict. The markers also serialize concurrent appends that can match a common event and are provably skew-safe for tag-scoped and type-scoped boundaries, including unconditional appends, and transient MongoDB transaction conflicts are retried instead of surfacing as a spurious command failure. The token is derived from positive markers only, so excluded types and multi-attribute conjunctions are a safe over-approximation in the conflict check: reads still apply them precisely, and a false conflict self-heals through the application-service retry. A `MatchAll` append condition is a whole-store lock and is not skew-safe against concurrent tag or type scoped appends. See [ADR 21](doc/architecture/decisions/0021-dcb-write-path-query-scoped-concurrency.md). Adversarial multi-threaded tests prove the type-versus-tag, tag-versus-tag, and type-versus-type cases plus the read-watermark and unconditional-append scenarios, and `explain` confirms the conflict and read queries are index-backed.
  * A no-token `failIfEventsMatch(query)` append condition (the "fail if any matching event exists" guard) checks the live events rather than the conflict markers, so on the Spring Mongo store it means "currently exists" and survives deletes, matching the in-memory store. Token-carrying conditions are unchanged.
  * DCB tag queries match with a single `dcbTags` array-containment predicate.
  * The Spring Boot starter now auto-configures application services from the same capability set: stream `ApplicationService` for `STREAM`, `DcbApplicationService` for `DCB`, and both for `stream,dcb`.
  * DCB-only Spring Boot auto-configuration also exposes `DomainEventQueries` so DCB query DSL extensions can reuse the starter-provided converter while stream application services remain disabled.
  * DCB application-service auto-configuration requires a user-provided `TagGenerator` bean, since DCB tags are domain-specific. When the DCB capability is enabled but no `TagGenerator` bean is found, the starter logs a warning explaining that `DcbApplicationService` is not auto-configured.
  * The auto-configured `DcbApplicationService` bean, on both the blocking and reactive starters, is now a normal, generically-typed `@Bean` method instead of a `BeanFactoryPostProcessor` registration. IDEs such as IntelliJ statically resolve `@Bean` methods but not beans registered by a `BeanFactoryPostProcessor`, so every injection site previously showed a false "Could not autowire" warning even though the bean existed at runtime. The method resolves the `TagGenerator` through an `ObjectProvider`, so a user's `TagGenerator` bean is still found reliably regardless of declaration order, and returns `null` (behaving as a genuinely absent bean for `@Autowired`/constructor injection, with the same warning log) when none exists. The bean definition itself still exists either way, though, so name-based lookups (`containsBean`, `getBean("occurrentDcbApplicationService")`) and by-type introspection (`getBeanNamesForType`) can observe it even when injection sees nothing; only the wiring mechanism changed, not that narrower slice of observable behavior.
  * The Spring Boot starter now also auto-configures the DCB DSL when the `DCB` capability is enabled: a `DcbDomainEventQueries` wrapping the auto-configured `DomainEventQueries`, and a `DcbSubscriptions` over the subscription model. Both back off to a user-provided bean of the same type.
  * In DCB-only mode the auto-configured subscriptions replay by `position` when started from a DCB start position, using the DCB-mode catch-up model.
  * Occurrent creates missing indexes/collections only. It never removes indexes or collections automatically.
* Added DCB query excluded-type support.
  * `DcbCriterion` has `excludedTypes`.
  * Excluded event types are expressed with `excludingTypes(..)` on a query.
  * Included types are any-of, tags are all-of, and excluded types are none-of within each query item.
  * Reads respect excluded types, so excluded events are filtered from DCB query results. The Spring Mongo append-condition check over-approximates excluded types as a safe conservatism (see the query-scoped concurrency note), so an excluded event that still carries a query's positive tag can trigger a self-healing conflict rather than being ignored.
* Added a blocking DCB DSL module.
  * New module: `org.occurrent:dcb-dsl-blocking`.
  * Java helpers: `DcbDomainEventQueries` and `DcbDomainEventStream`.
  * `DcbDomainEventQueries` wraps a `DomainEventQueries`, reusing its configured `CloudEventConverter` and delegating the regular stream query API, so a DCB application uses one object for both DCB queries and stream queries instead of passing a `DcbEventStore` directly.
  * The DSL can build criteria from domain event `Class` objects, resolved through the same `CloudEventTypeMapper` used at write time, via `queries.criteria().types(SomeEvent.class)` (Kotlin: `typeOf<SomeEvent>()`), so callers no longer hand-write type strings.
  * Kotlin query extensions on `DcbDomainEventQueries`: `queryForSequence` and `queryForList`. The `queryWithPosition` overloads are member functions on `DcbDomainEventQueries`.
  * `DcbDomainEventStream` and `queryWithPosition` carry the `DcbConsistencyToken` alongside the sequence position, so a caller can read through the DSL and then run a sound conditional append. The Kotlin `queryForListWithPosition`/`queryForSequenceWithPosition` extensions return the token as the third element of a `Triple`.
  * Kotlin live subscription extension on `Subscribable`: `subscribeDcb`.
  * DCB subscription helpers subscribe to CloudEvents and post-filter DCB-tagged events by `DcbCriteria`. They are live subscription conveniences, not DCB-consistent reads.
  * DCB subscription metadata callbacks reuse the existing `EventMetadata` type and expose the shared `position` (on `EventMetadata`) and the DCB `dcbTags` Kotlin extension property.
  * Kotlin decider extensions on `DcbApplicationService` mirror the stream decider helpers while using `DcbCriteria` as the decision boundary.
  * `DcbEventMetadata` gives Java callers an `OptionalLong` `position()` and the `dcbTags()` of a subscription event. Kotlin reads `position` on `EventMetadata` and `dcbTags` through the extension property.
  * `DcbSubscriptions` is an instance wrapper over a `Subscribable` and a `CloudEventConverter`, so DCB subscriptions can be created without passing the converter on every call, mirroring `DcbDomainEventQueries`.
  * Kotlin `queryForListWithPosition` and `queryForSequenceWithPosition` extensions return the matching events together with the observed DCB sequence position.
* Added DCB catch-up subscription support to `CatchupSubscriptionModel`.
  * A new DCB-mode constructor takes a `DcbEventStore` and a `DcbCriteria`. In this mode the catch-up phase replays historic DCB events ordered by `position` and the subscription resumes by `position`, so a DCB application can rebuild a read model from history rather than only subscribing live.
  * The replay pages through the DCB sequence in position windows, so a large rebuild does not load the whole matched set at once. The window size is configurable through `CatchupSubscriptionModelConfig.dcbCatchupPositionWindowSize`.
  * Reconciliation of events written during the replay is by `position` rather than by a count, so the DCB catch-up is immune to the clock-skew loss and the `estimatedDocumentCount` undercount the legacy time-based stream catch-up has to defend against.
  * The stream catch-up constructors remain available. Later unified-position work made stream catch-up use the same position-based replay path when the store writes position.
  * DCB start positions use the shared `position` vocabulary.
* Added two DCB word-guessing MongoDB Spring examples.
  * `example-domain-word-guessing-game-es-mongodb-spring-dcb` shows manual DCB wiring with explicit DCB queries, tags, application-service usage, and live durable subscriptions.
  * `example-domain-word-guessing-game-es-mongodb-spring-dcb-autoconfig` shows Spring Boot auto-configuration, `@EnableOccurrent`, DCB-only event-store capabilities, annotation subscriptions, and DCB decider command handling.
  * The DCB autoconfig example uses the starter-provided Jackson 3 `CloudEventConverter` with a domain-specific `ReflectionCloudEventTypeMapper`, instead of a custom converter.
  * Both examples assert DCB-only stream API rejection while keeping DCB-written events observable as normal CloudEvents with DCB and Occurrent storage metadata.
* Added ADRs for the DCB design:
  * [ADR 17](doc/architecture/decisions/0017-introduce-dcb-as-shared-cloudevent-capability.md)
  * [ADR 18](doc/architecture/decisions/0018-spring-mongo-event-store-capabilities.md)
  * [ADR 19](doc/architecture/decisions/0019-dcb-dsl-module.md)
  * [ADR 20](doc/architecture/decisions/0020-dcb-catch-up-subscription-by-dcbposition.md)
  * [ADR 21](doc/architecture/decisions/0021-dcb-write-path-query-scoped-concurrency.md)
* Renamed the package-private `OccurrentAnnotationBeanPostProcessor` to `OccurrentBlockingAnnotationBeanPostProcessor` for symmetry with the reactive starter's `OccurrentReactiveAnnotationBeanPostProcessor`. Internal only, no public API impact.
* Extensively broadened the integration test coverage for `@StreamSubscription` and `@DcbSubscription` annotation processing on both the blocking and reactive starters: the full `startAt` matrix (including the ISO8601 and epoch millis attributes, previously untested at the annotation level), durable resume and replay across an actual application restart for every `resumeBehavior`, metadata parameter binding for the reactive stack and for blocking stream subscriptions, reactive handler return type adaptation (`void`, `Mono<Void>`, and non-`Void` `Mono<T>`), the observable behavior of a permanently failing handler on each stack, and `startupMode = WAIT_UNTIL_STARTED`. This is what caught the `ReactorDcbCatchupSubscriptionModel` bug above.
#### Notes

* Existing stream-based APIs remain backward compatible by default.
* Historical stream-written events are not automatically DCB-readable. They need explicit DCB tag metadata before they can participate in DCB reads, and old events that predate the shared `position` field need position backfill as well.
* Enabling a new Spring Mongo capability may create indexes on startup. For large production collections, create required indexes out-of-band before changing application configuration.
* DCB-only Spring Mongo usage still stores normal CloudEvents with Occurrent stream metadata. If stream support is enabled later, DCB-written events can be read by their storage stream ids, typically DCB partition streams.

### 0.20.5 (2026-06-29)

* Added `adapt` and `compose` decider combinators to the `dsl/decider` module.
  * `adapt` widens a decider over a feature's own command and event subtypes into one over the shared supertypes, ignoring foreign events and treating foreign commands as no-ops, so a `Decider<CourseCommand, CourseState, CourseEvent>` can run against a service over a common `DomainEvent`. It is available as a Java static taking `Class` tokens and as a Kotlin `reified` extension that reads `courseDecider.adapt()` at the call site.
  * `compose` combines several feature deciders into one whose state is the product of the individual states. Each command routes to the decider that recognizes it, each state slice evolves independently, and the composed decider is terminal once every constituent is. The two and three decider overloads `adapt` each decider for you and return a typed `Pair` or `Triple`, so you can write `compose(courseDecider, studentDecider, enrollmentDecider)` over the feature deciders directly. The two decider case also has an infix form, `courseDecider compose studentDecider`. For four or more, a vararg `compose(d1, d2, d3, d4, ...)` and a `compose(list)` form both return a positional `CompositeState` and take deciders that already share the command and event type.
  * Both combinators are pure decider algebra and add no new dependency to the module, in particular no dependency on any DCB module.
  * See [ADR 15](doc/architecture/decisions/0015-adapt-and-compose-decider-combinators.md).
* The decider `execute` extensions on `ApplicationService` now widen a decider's event type for you.
  * A feature decider over its own narrow event type can be passed straight to an `ApplicationService` over a broader event type, without calling `adapt` or `adaptEvents` first. This removes a papercut, because an injected application service is typically over the broadest event type, so a feature decider previously always had to be widened by hand at every call site.
  * Added `adaptEvents`, the event-only counterpart to `adapt`. It widens only the event type and leaves the command type unchanged. The `execute` extensions use it internally.
* The CloudEvent converter can now truncate the CloudEvent time to a configured precision.
  * `Instant.now()` and `OffsetDateTime.now()` carry nanoseconds on modern JVMs, which `TimeRepresentation.DATE` cannot store, so an append failed with a "contains micro-/nanoseconds" error. The Jackson CloudEvent converter builder gains `timePrecision(ChronoUnit)`, and the Spring Boot starter adds the `occurrent.cloud-event-converter.time-precision` property (a `ChronoUnit`, for example `millis`).
  * When that property is unset and the event store `time-representation` is `DATE`, the converter now defaults to truncating to `MILLIS`, so the common case works with no configuration. `RFC_3339_STRING` keeps full precision.
* The Spring Boot starter's fallback CloudEvent converter now registers the Jackson modules found on the classpath.
  * The default Jackson 3 converter built a bare `ObjectMapper`, and Jackson 3, unlike Jackson 2, does not auto-register modules. So the fallback converter could not serialize or deserialize Kotlin data classes or `java.time` types even when their modules were on the classpath, failing with a "no Creators" error. The fallback now uses `JsonMapper.builder().findAndAddModules()` to discover and register them. Supplying your own `CloudEventConverter` or `tools.jackson` `ObjectMapper` bean still overrides this.
* Fixed a remaining silent event loss in `CatchupSubscriptionModel` at the handover from the catch-up phase to the live subscription.
  * The delta reconciliation sized its read from a count of matching events and then read the newest N of them. An event written in the window between that count and the read shifted the newest-N window forward and pushed the oldest during-catch-up event out of the read. That event sat at or before the live subscription's resume position, so the live subscription did not redeliver it either, and it was lost. This is the residual case left open by the 0.20.4 fix, which closed the clock-skew variant but not the count-to-read window.
  * The reconciliation now re-reads the recent tail until the matching count stops growing, so an event that arrives in the count-to-read window is picked up by a later pass instead of being skipped. Overlapping passes are deduplicated through the handover cache, so at-least-once delivery is preserved without introducing duplicates.
  * See [ADR 14](doc/architecture/decisions/0014-reconcile-catchup-events-by-insertion-order-to-avoid-loss-under-clock-skew.md).
* Upgraded Spring Boot from 4.0.4 to 4.1.0. This pulls in Spring Framework 7.0.8, Spring Data 2026.0.0, Reactor 2025.0.x, the MongoDB driver 5.8.0, Kotlin 2.3.21, and Jackson 2.21.4 / 3.1.4 transitively.
  * The explicit Reactor and MongoDB driver version overrides were removed from the root build, so their versions are now governed by the Spring Boot dependency BOM.

### 0.20.4 (2026-06-18)

* Fixed a silent event loss in `CatchupSubscriptionModel` at the handover from the catch-up phase to the live subscription.
  * An event written during the catch-up replay whose CloudEvent `time` was earlier than the replay cursor (a writer with a clock running behind) could be skipped by the delta reconciliation and also sit below the live subscription's resume position, so no delivery path saw it. Since it was a miss and not a duplicate, idempotent consumers could not recover it.
  * The delta now reconciles events written during the replay by insertion order (`SortBy.natural` descending with a limit) instead of the time-based `skip`, so a clock-skewed event is still among the newest events and is delivered. It also reads only the recent tail instead of walking the whole backlog, so it is faster on large stores, and it removes a spurious duplicate the old reconciliation produced.
  * `InMemoryEventStore` now makes `SortBy.natural` reflect global insertion order, matching MongoDB's `$natural`, so the catch-up reconciliation behaves the same on both stores.
  * See [ADR 14](doc/architecture/decisions/0014-reconcile-catchup-events-by-insertion-order-to-avoid-loss-under-clock-skew.md).
* Fixed a `ConcurrentModificationException` that could occur when consuming a stream returned by `InMemoryEventStore.query(..)` while another thread writes to the store.
  * The returned stream was lazy, so its sort, skip, and limit ran after the query lock was released. A concurrent `write(..)` modifies the backing map at that point, which could throw or expose an in-flight stream.
  * `query(..)` now snapshots the per-stream event lists while holding the lock and filters and sorts on that snapshot afterwards, so the lock is no longer held during filtering.
  * This matters more now because the `CatchupSubscriptionModel` delta reconciliation above queries with `SortBy.natural` descending, so the in-memory store can be read concurrently with writes during a catch-up.

### 0.20.3 (2026-03-29)

* Completed the Spring Boot starter fallback fix for composed `CloudEventConverter` setups.
  * The starter's fallback `CloudEventConverter` no longer requires a `CloudEventTypeMapper` bean to exist and now falls back to `ReflectionCloudEventTypeMapper.qualified()` when needed.
  * The fallback converter and fallback type mapper are now lazy, so they are not pre-instantiated when a library or application already provides its own converter.
  * This fixes startup failures in composed support-library setups where `occurrentCloudEventConverter` was still created and failed before a custom converter could take effect.

### 0.20.2 (2026-03-29)

* Made the default `CloudEventConverter` in `org.occurrent:spring-boot-starter-mongodb` behave as a fallback bean.
  * Custom `CloudEventConverter` beans provided by applications or composed support libraries now take precedence also when Occurrent is enabled indirectly through another `@Enable...` annotation.
  * This fixes duplicate `CloudEventConverter` conflicts in frameworks that enable Occurrent indirectly.

### 0.20.1 (2026-03-29)

* Made `org.occurrent:spring-boot-starter-mongodb` Jackson 3-only for its built-in CloudEvent converter autoconfiguration.
  * The starter no longer brings in the Jackson 2 converter lane by default.
  * If you want to keep using `org.occurrent:cloudevent-converter-jackson`, define your own `CloudEventConverter` bean explicitly.
  * Starter behavior is now:
    * user-provided `CloudEventConverter` wins
    * otherwise the starter configures a Jackson 3 converter
    * if no Jackson 3 `ObjectMapper` bean exists, the starter creates a default Jackson 3 mapper internally
  * The starter still includes the Jackson annotations dependency required by the Jackson 3 lane.

### 0.20.0 (2026-03-28)

#### Highlights

* Added Spring Boot 4 support in the MongoDB starter while keeping the Spring-facing API stable (`@EnableOccurrent`, `occurrent.*` properties, and the existing opt-in model are unchanged).
* Added a new Jackson 3-native converter artifact, `org.occurrent:cloudevent-converter-jackson3`, as the preferred choice for new Boot 4 applications.
* Added `StreamReadFilter` and optional `ReadEventStreamWithFilter` capabilities for filtered stream reads.
* Added filtered read support to `ApplicationService` through `ExecuteOptions<T>`, with side effects and overload reduction as supporting API cleanup.
* Finalized the Kotlin `ApplicationService` and typed-filter naming cleanup around `executeSequence(...)`, `executeList(...)`, and `ExecuteFilters`.

* Upgraded the Spring Boot line from `3.5.x` to `4.0.4`.
  * The MongoDB starter now supports both Jackson 2 and Jackson 3 converter wiring on Boot 4.
  * The starter now picks a user-provided `CloudEventConverter` first, otherwise Jackson 3 when a Jackson 3 `ObjectMapper` is present, and otherwise Jackson 2 as the fallback.
* Added a Jackson 3-native converter artifact: `org.occurrent:cloudevent-converter-jackson3`.
  * This is the intended default choice for new code on the Boot 4 stack.
  * The Jackson 3 path uses the `tools.jackson.*` packages.
  * The new Jackson 3 converter shares the same CloudEvent conversion semantics as the Jackson 2 converter through a common internal converter core.
* Kept the existing Jackson 2 converter artifact `org.occurrent:cloudevent-converter-jackson` as a compatibility lane.
  * Existing public APIs that already expose Jackson 2 types continue to work.
  * Existing applications can stay on `cloudevent-converter-jackson` while migrating incrementally.
* Split dependency management so Jackson 2 and Jackson 3 are resolved intentionally rather than through one shared root override.
  * Jackson 2 compatibility modules continue to resolve Jackson 2.
  * Jackson 3-native modules resolve Jackson 3 locally where needed.
  * The BOM now publishes both converter lanes alongside the upgraded Spring Boot starter.
* Migrated the example reactor to the Boot 4 / Jackson 3 path.
  * This includes both starter-based examples and manually wired Spring applications.
  * One adhoc projection example now uses a local ISO `LocalDateTime` serializer/deserializer instead of the unavailable Jackson 3 `jsr310` artifact.
* Upgrade guidance:
  * New applications should prefer `cloudevent-converter-jackson3`.
  * Existing applications that already depend on the Jackson 2 converter API can continue using `cloudevent-converter-jackson`.
  * If you previously relied on Jackson default typing in example-style setups, prefer explicit CloudEvent converter configuration or Jackson 3 builder-based configuration instead.
* Added `StreamReadFilter` support for stream reads with validation of reserved stream fields (`streamid`, `streamversion`).
  This is useful when:
    * Your command only depends on a subset of events in a stream (for example one or two event types), and reading the full stream adds unnecessary IO and deserialization work.
    * You want to keep command handling code readable when combining stream filtering and post-write side-effects.
    * You want to gradually adopt filtered stream reads without changing existing write/concurrency semantics.
    * This is a read optimization, not a correctness feature. If your invariant depends on all events, do not filter away relevant events.
  * New filter type: `org.occurrent.eventstore.api.StreamReadFilter`
  * New optional capability interface for blocking event stores:
    `org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter`
  * New optional capability interface for reactor event stores:
    `org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter`
  * Implemented support in:
    * `InMemoryEventStore`
    * `SpringMongoEventStore`
    * `MongoEventStore` (native)
    * `ReactorMongoEventStore`
* Added `ExecuteOptions<T>` to bring filtered reads and side effects into the main `ApplicationService` API.
  * The main new capability is `.filter(...)`, which lets execute flows read only the relevant events from a stream.
  * The options object also reduces overload pressure by grouping filtering and side-effect configuration in one place.
  * New entry points:
    * `execute(String streamId, ExecuteOptions<T> options, Function<Stream<T>, Stream<T>> fn)`
    * `execute(UUID streamId, ExecuteOptions<T> options, Function<Stream<T>, Stream<T>> fn)`
  * New builder entrypoint:
    * `ExecuteOptions.options()`
  * Example:
    ```java
    WriteResult result = applicationService.execute(
            streamId,
            ExecuteOptions.<DomainEvent>options()
                    .filter(StreamReadFilter.type("com.acme.NameDefined"))
                    .sideEffect(newEvents -> newEvents.forEach(this::publish)),
            domainFn
    );
    ```
* Deprecated legacy `ApplicationService` side-effect overloads in favor of `ExecuteOptions`:
  * `execute(String, Function<Stream<T>, Stream<T>>, Consumer<Stream<T>>)`
  * `execute(UUID, Function<Stream<T>, Stream<T>>, Consumer<Stream<T>>)`
* Kotlin `ApplicationService` helpers now use explicit collection-oriented names and direct-import `ExecuteOptions` helper functions:
  * New Kotlin helpers:
    * `executeSequence(...)`
    * `executeList(...)`
    * `options()`
    * `filter(...)`
    * `sideEffect(...)`
    * `sideEffectOnSequence(...)`
    * `sideEffectOnList(...)`
  * You can now write either:
    ```kotlin
    applicationService.executeSequence(
        gameId,
        options().sideEffect(
            revealCharacterInWordHintAfterPlayerGuessedTheWrongWord::invoke,
            awardPointsToPlayerThatGuessedTheRightWord::invoke
        )
    ) { events ->
        guessWord(events, timeOfGuess, playerId, word)
    }
    ```
    or:
    ```kotlin
    applicationService.executeSequence(
        gameId,
        sideEffect(
            revealCharacterInWordHintAfterPlayerGuessedTheWrongWord::invoke,
            awardPointsToPlayerThatGuessedTheRightWord::invoke
        )
    ) { events ->
        guessWord(events, timeOfGuess, playerId, word)
    }
    ```
* Added application-service-level typed filter conveniences for `ExecuteOptions` and `ApplicationService`.
  * New type:
    * `ExecuteFilter<T>`
  * Java can now express filtered reads in terms of domain event classes instead of raw CloudEvent type strings:
    ```java
    WriteResult result = applicationService.execute(
            streamId,
            ExecuteOptions.<DomainEvent>options()
                    .filter(ExecuteFilter.type(NameDefined.class))
                    .sideEffect(newEvents -> newEvents.forEach(this::publish)),
            domainFn
    );
    ```
  * Java also supports direct execute-filter convenience overloads:
    ```java
    WriteResult result = applicationService.execute(
            streamId,
            ExecuteFilter.excludeTypes(NameWasChanged.class, NameDefined.class),
            domainFn
    );
    ```
  * Kotlin can now use reified type helpers:
    ```kotlin
    applicationService.executeSequence(
        streamId,
        options().filter(ExecuteFilters.type<NameDefined>())
    ) { events ->
        handle(events)
    }
    ```
    and namespaced multi-type helpers:
    ```kotlin
    applicationService.executeSequence(
        streamId,
        ExecuteFilters.excludeTypes(NameDefined::class, NameWasChanged::class)
    ) { events ->
        handle(events)
    }
    ```
  * Kotlin typed execute-filter helpers now live under `ExecuteFilters` to keep `ApplicationService` clean and preserve stronger typing for multi-type filters.
  * Earlier top-level Kotlin typed filter helpers such as `type<MyEvent>()`, `excludeTypes<A, B>()`, and `includeTypes<A, B>()` are deprecated in favor of the namespaced `ExecuteFilters` API.
  * These helpers resolve domain event classes through `CloudEventTypeGetter` / `CloudEventConverter`, not through `Class.getName()`.
  * Upgraded kotlin from 2.2.20 to 2.3.10

#### Breaking changes

* Upgrading to the Boot 4 path means Jackson 3 is now the preferred lane for new applications.
  * New Spring Boot applications should use `org.occurrent:cloudevent-converter-jackson3`.
  * The Jackson 3 lane uses the `tools.jackson.*` packages.
  * If your existing code depends on Occurrent APIs that expose `com.fasterxml.jackson.*`, stay on `org.occurrent:cloudevent-converter-jackson` until you migrate.
* Kotlin collection-oriented `ApplicationService` extensions are exposed as `executeSequence(...)` and `executeList(...)`.
  * The shorter Kotlin `execute(...)` collection aliases are not part of the final `0.20.0` API.
  * Migration summary:
    * Before: `applicationService.execute(...) { events: Sequence<T> -> ... }`
    * After: `applicationService.executeSequence(...) { events -> ... }`
    * Before: `applicationService.execute(...) { events: List<T> -> ... }`
    * After: `applicationService.executeList(...) { events -> ... }`
* Kotlin typed execute-filter helpers are namespaced under `ExecuteFilters`.
  * Migration summary:
    * Before: `type<MyEvent>()`, `excludeTypes<A, B>()`, `includeTypes<A, B>()`
    * After: `ExecuteFilters.type<MyEvent>()`, `ExecuteFilters.excludeTypes(A::class, B::class)`, `ExecuteFilters.includeTypes(A::class, B::class)`

#### Why this changed

* Kotlin prefers Java members over extension functions during overload resolution.
* That caused Kotlin call sites to bind to the Java `Stream`-based `execute(...)` overloads instead of the intended Kotlin `Sequence` or `List` extensions.
* The symptom was confusing code and compiler errors, for example in `MakeGuess.kt`, where Kotlin users could be forced to add explicit lambda parameter types such as `events: Sequence<GameEvent>` just to make the intended overload resolve.
* We explicitly avoided introducing a second options type or hiding Java members from Kotlin, because this is an open source library used by third parties and the Java API should stay predictable.
* We also explicitly chose not to add Java `executeList(...)` in this change. Doing so would increase API surface without solving the Kotlin overload-resolution problem that motivated these changes.

Before:
```kotlin
applicationService.execute(
    gameId,
    options<GameEvent>().sideEffect(
        revealCharacterInWordHintAfterPlayerGuessedTheWrongWord::invoke,
        awardPointsToPlayerThatGuessedTheRightWord::invoke
    )
) { events: Sequence<GameEvent> ->
    guessWord(events, timeOfGuess, playerId, word)
}
```

After:
```kotlin
applicationService.executeSequence(
    gameId,
    options().sideEffect(
        revealCharacterInWordHintAfterPlayerGuessedTheWrongWord::invoke,
        awardPointsToPlayerThatGuessedTheRightWord::invoke
    )
) { events ->
    guessWord(events, timeOfGuess, playerId, word)
}
```

Read more in [ADR 12](doc/architecture/decisions/0012-avoid-kotlin-extension-name-collisions-with-java-applicationservice-members.md).
Read more about Kotlin typed execute-filter namespacing in [ADR 13](doc/architecture/decisions/0013-namespace-kotlin-typed-execute-filters-under-executefilters.md).

### 0.19.14 (2025-10-20)
* Added additional `@Nullable` annotation to a method in blocking `SubscriptionFilter` that could lead to errors when passing null (thanks to Kirill Gavrilov for PR)
* Changed the build so that Kotlin sources are included in the release to maven central

### 0.19.13 (2025-10-13)
* Improvements to view-dsl (`org.occurrent:view-dsl`)  
  1. The `update` method in `org.occurrent.dsl.view.MaterializedView` now takes a `RetryStrategy` so that updates can be retried
  2. Calling the kotlin extension function `materialized` on a `org.occurrent.dsl.view.View` now takes a `org.occurrent.dsl.view.SpringMongoViewConfig` that allows you to configure how to handle `DuplicateKeyException` and `OptimisticLockingFailureException` thrown by Spring Repositories or `MongoOperations`.
     By default, `DuplicateKeyException` is ignored and `OptimisticLockingFailureException`'s are retried with exponential backoff between 100 ms to 5s. This can be configured, for example:  
     ```kotlin
     @Document(collection = "name-state")
     @TypeAlias("NameState")
     data class NameState(@Id val userId: String, val name: String, @Version val version: Long? = null)
     
     val mongoOperations = .. 
     val nameView = view<NameState?, DomainEvent>(null) { s, e ->
                 when (e) {
                     is NameDefined -> NameState(e.userId(), e.name)
                     is NameWasChanged -> s!!.copy(name = e.name)
                 }
             }      
     val config = SpringMongoViewConfig.config(duplicateKeyHandling = ignore(), optimisticLockingHandling = rethrow())                          
     val materializedNameView = nameView.materialized(mongoOperations, config, DomainEvent::userId)
     // Now you can do this to update the view in the MongoDB database from an event
     val e = NameChangedEvent(..)
     materializedNameView.update(e)
     ```
  3. Several new overloaded evolve methods to make it easier to evolve the view from multiple events and not just one. Also, new kotlin extension functions for this defined in `org.occurrent.dsl.view.ViewExtensions.kt`.  
 * Replaced recursive retry logic with iterative loop in the `retry` module
 * Migrated from jetbrains annotations to jspecify and introduce jspecify to almost all modules and API's (and fixed some bugs detected while introducing JSpecify)
 * Upgraded spring-boot from 3.4.2 to 3.5.6
 * Upgraded spring-data-mongodb from 4.4.2 to 4.4.3
 * Upgraded mongodb-driver-sync from 5.3.1 to 5.6.1
 * Upgraded jobrunr from 7.4.0 to 8.1.0
 * Upgraded kotlin from 2.1.10 to 2.2.20
 * Upgraded project reactor from 2024.0.3 to 2024.0.10 
 * Upgraded jackson from 2.18.2 to 2.19.2 

### 0.19.12 (2025-09-26)
* Internal changes including lots of changes to the build pipeline

### 0.19.11 (2025-05-26)
* Forward isRunning(String subscriptionId) to the proper method in CatchupSubscriptionModel (thanks to David Göransson for PR). This fixes a hairy issue with subscription restart logic after MongoDB downtime. 

### 0.19.10 (2025-02-15)
* Upgraded logback from 1.5.6 to 1.5.16
* Fixed an issue in the CompetingConsumerSubscriptionModel in which the model could be started automatically if consumption was granted, even though it was explicitly stopped
* Fixed ConcurrentModificationException's that could occur when doing queries and writing at the same time to an `InMemoryEventStore` instance 

### 0.19.9 (2025-02-14)
* Added toString() to subscription models for better debug output
* Fixed issue in `MongoListenerLockService` (used by competing subscription models) in which two subscribers could "race" to catch the lease one more time unnecessarily
* Translating DataIntegrityViolationException's correctly to WriteConditionNotFulfilledException during write conflicts in `ReactorMongoEventStore`  
* Upgraded spring-boot from 3.4.1 to 3.4.2
* Upgraded spring-data-mongodb from 4.4.2 to 4.4.3
* Upgraded mongodb-driver-sync from 5.3.0 to 5.3.1
* Upgraded jobrunr from 7.3.2 to 7.4.0
* Upgraded kotlin from 2.1.0 to 2.1.10
* Upgraded project reactor from 3.7.2 to 3.7.3
* Upgraded xstream from 1.4.20 to 1.4.21

### 0.19.8 (2025-01-17)
* Converted `org.occurrent.subscription.StreamSubscriptionFilter` from a Java class to a record. This means that the `public final` filter instance field is now a record property. So if you ever used `streamSubscriptionFilter.filter` to access the underlying filter, you now need to do `streamSubscriptionFilter.filter()` instead.
* Fixed a bug in MongoLeaseCompetingConsumerStrategySupport in which it was not marked a running on start. This could affect retries of certain competing consumer errors.
* Adding equals/hashcode and toString to SpringMongoSubscriptionModel, this is useful in certain debug logging scenarios
* Upgraded spring-boot from 3.3.5 to 3.4.1
* Upgraded spring-data-mongodb from 4.3.5 to 4.4.2
* Upgraded mongodb-driver-sync from 5.2.0 to 5.3.0d
* Upgraded jobrunr from 7.3.1 to 7.3.2
* Upgraded project reactor from 3.6.11 to 3.7.2
* Upgraded kotlin from 2.0.21 to 2.1.0
* Upgraded jackson from 2.17.2 to 2.18.2

### 0.19.7 (2024-11-01)
* Implemented "in" conditions so you can now do e.g. `subscriptionModel.subscribe("id", StreamSubscriptionFilter.filter(Filter.streamVersion(Condition.in(12L, 14L))`. There's also a Kotlin extension function, `isIn`, which can be imported from `org.occurrent.condition.isIn`.
* Upgraded kotlin from 2.0.20 to 2.0.21
* Upgraded spring-boot from 3.3.3 to 3.3.5
* Upgraded spring-data-mongodb from 4.3.3 to 4.3.5
* Upgraded mongodb-driver-sync from 5.1.4 to 5.2.0
* Upgraded jobrunr from 7.2.3 to 7.3.1
* Upgraded project reactor from 3.6.9 to 3.6.11

### 0.19.6 (2024-10-11)
* Fixed so that inserting events with "any" WriteCondition never fails even if more than two threads are writing events to the same stream at the same time. (Fixed in MongoEventStore and SpringMongoEventStore)   

### 0.19.5 (2024-09-27)
* Fixed so that blocking and reactive EventStoreQueries really uses `SortBy.unsorted()` by default as was intended in the previous release.

### 0.19.4 (2024-09-27)
* Added better debug logging
* Improved queryOne performance in DomainEventQueries
* Fixed issue in Kotlin extensions for EventStoreQueries which made them unusable
* Introduced `SortBy.unsorted()` which is now the default sort specification used when no one is specified explicitly in queries
* Upgraded spring-boot from 3.3.3 to 3.3.4 

### 0.19.3 (2024-09-11)
* Added two kotlin extension functions to DomainEventQueries:
  1. `queryForList` that just takes a filter and a "SortBy"
  2. `queryForSequence` that just takes a filter and a "SortBy" 
  
  The reason for this is to avoid ambiguity with other extension function when only specifying these values. 

### 0.19.2 (2024-09-10)
* `OccurrentAnnotationBeanPostProcessor` is only applied if `occurrent.subscription.enabled` property is missing or is `true`.
* Added ability to disable the creation a default instance of an `ApplicationService` when using the `spring-boot-starter-mongodb` module by specifying `occurrent.application-service.enabled=false`. 
* Upgraded cloudevents from 3.0.0 to 4.0.1
* Upgraded jackson from 2.17.1 to 2.17.2
* Upgraded jobrunr from 7.2.1 to 7.2.3
* Upgraded kotlin from 2.0.0 to 2.0.20
* Upgraded mongodb-driver-sync from 5.1.1 to 5.1.4
* Upgraded project reactor from 3.6.7 to 3.6.9
* Upgraded spring-boot from 3.3.1 to 3.3.3
* Upgraded spring-data-mongodb from 4.3.1 to 4.3.3
* Upgraded kotlinx-html-jvm from 0.7.2 to 0.11.0

### 0.19.1 (2024-07-04)
* Ignoring NoSuchBeanDefinitionException when getting the springApplicationAdminRegistrar bean when working around https://github.com/spring-projects/spring-framework/issues/32904

### 0.19.0 (2024-06-27)
* Made OccurrentAnnotationBeanPostProcessor a static bean in OccurrentMongoAutoConfiguration to avoid Spring warning logs when booting up
* Fixed a bug in OccurrentAnnotationBeanPostProcessor that caused `@Subscription(id="myId", startAt = BEGINNING_OF_TIME)` not to replay events from the beginning of time
* CompetingConsumerSubscriptionModel supports delegating to parent subscription model if the `StartAt` position returns `null`. This means that the CompetingConsumerSubscriptionModel can be bypassed for certain subscriptions. This is useful if you have an in-memory subscription on multiple nodes with a `CompetingConsumerSubscriptionModel`. 
* Added an overloaded method to SubscriptionModelLifeCycle (implemented by most SubscriptionModels) start allows you to start a subscription model without automatically starting all paused subscriptions. This method is called `start` and takes a boolean that tells if all subscriptions should be automatically started when the subscription model starts.
* When using "in-memory" subscriptions, by doing e.g. `@Subscription(id="myId", startAt = BEGINNING_OF_TIME, resumeBehavior = SAME_AS_START)`, the subscription will be started on all nodes even when a CompetingConsumerSubscriptionModel is used.
* The waitUntilStarted() method in the Subscription interface is now a default method.
* CatchupSubscriptionModel subscriptions are now started in a background thread by default. Call the "waitUntilStarted()" on the Subscription to make is synchronous.
* The java.util.Stream returned from SpringMongoEventStore is now automatically closed when the last element is consumed.
* Added ability to specify whether the subscription should "waitUntilStarted" in the Subscriptions DSL. 
* Upgraded Spring Boot from 3.2.5 to 3.3.1
* Upgraded Kotlin from 1.9.23 to 2.0.0
* Upgraded Mongo sync driver from 4.11.2 to 5.1.1
* Upgraded Jackson from 2.15.4 to 2.17.1
* Upgraded reactor from 3.6.5 to 3.6.7
* Upgraded jobrunr from 7.1.1 to 7.2.1
* Upgraded amqp-client from 5.20.0 to 5.21.0
* Upgraded spring-aspects from 6.1.1 to 6.1.10
* Upgraded spring-retry from 2.0.3 to 2.0.6
* Upgraded spring-hateoas from 2.2.0 to 2.3.0
* Upgraded spring-data-mongodb from 4.3.0 to 4.3.1
* Upgraded kotlinx-collections-immutable-jvm from 0.3.4 to 0.3.7
* Upgraded arrow-core from 1.2.1 to 1.2.4
* Upgraded jetbrains annotations from 22.0.0 to 24.1.0
* Upgraded logback-classic from 1.4.14 to 1.5.6

### 0.18.0 (2024-05-17)
* Major improvements to `CatchupSubscriptionModel`, it now handles and includes events that have been written while the catch-up subscription phase runs. Also, the "idempotency cache" is only used while switching from catch-up to continuous mode, and not during the entire catch-up phase.
* Major changes to the `spring-boot-starter-mongodb` module. It now includes a `CatchupSubscriptionModel` which allows you to start subscriptions from an historic date more easily.
* `StartAt.Dynamic(..)` now takes a `SubscriptionModelContext` as a parameter. This means that subscription models can add a "context" that can be useful for dynamic behavior. For example, you can prevent a certain subscription model to start (and instead delegate to its parent) if you return `null` as `StartAt` from a dynamic position.
* Added annotation support for subscriptions when using the `spring-boot-starter-mongodb` module. You can now do: 
  ```java
  @Subscription(id = "mySubscription")
  void mySubscription(MyDomainEvent event) {
      System.out.println("Received event: " + event);
  }  
  ```
  It also allows you to easily start the subscription from a moment in the past (such as beginning of time). See javadoc in `org.occurrent.annotation.Subscription` for more info.
* Added `org.occurrent.subscription.blocking.durable.catchup.StartAtTime` as a help to the `CatchupSubscriptionModel` to easier specify an `OffsetDateTime` or "beginning of time" when starting a subscription catchup subscription model. Before you had to do:
  ```java
  subscriptionModel.subscribe("myId", StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), System.out::println);
  ```
  but now you can do:
  ```java
  subscriptionModel.subscribe("myId", StartAtTime.beginningOfTime(), System.out::println);
  ```                                                                                     
  which is shorter. You're using Kotlin you can import `org.occurrent.subscription.blocking.durable.catchup.beginningOfTime` and do:
  ```kotlin
  subscriptionModel.subscribe("myId", StartAt.beginningOfTime(), ::println)
  ```  
* Changed the default behavior of `CatchupSubscriptionModel`. Before it replayed all historic events by default if no specific start at position was supplied, but now it delegates to the wrapped subscription and no historic events will be replayed. Instead, you need to explicitly specify `beggingOfTime` or an `OffsetDateTime` as the start position. For example:
  ```java
  subscriptionModel.subscribe("myId", StartAtTime.beginningOfTime(), System.out::println);
  ```
* Upgraded Spring Boot from 3.2.1 to 3.2.5
* Upgraded Mongo sync driver 4.11.1 to 4.11.2
* Upgraded jobrunr from 6.3.3 to 7.1.1
* Upgraded project reactor from 3.6.0 to 3.6.5
* Upgraded jackson from 2.15.3 to 2.15.4
* Upgraded Kotlin from 1.9.22 to 1.9.23
* Upgraded spring-data-mongodb from 4.2.0 to 4.2.5
* Upgraded cloudevents from 2.5.0 to 3.0.0

### 0.17.2 (2024-02-27)
* Fixed issue in CompetingConsumerSubscriptionModel in which it failed to reacquire consumption rights in some cases where MongoDB connection was lost.   

### 0.17.1 (2024-02-23)
* Fixed issue in [Subscription DSL](https://occurrent.org/documentation#subscription-dsl) when using "subscribe" functions with a single event type different from the "base event type", i.e. this didn't work in previous version in Java:
  ```java                
  // GameEvent is the "base event type"
  Subscriptions<GameEvent> subscriptions = new Subscriptions<>(..);
  
  // GameStarted has GameEvent as parent, the following didn't compile in version 0.17.0 
  subscriptions.subscribe("mysubscription", GameStarted.class, gameStarted -> System.out.println("gameStarted: " + gameStarted));
  ```
* Using slf4j-api and not logback-classic in several modules that accidentally brought logback in as a compile time dependency.                                                                                 
* Upgraded slf4j-api from 2.0.5 to 2.0.12
* In the `spring-boot-starter-mongodb` module, it's now possible to enable/disable the event store or subscriptions from the `application.yaml` file. For example, you can disable the event store like this:

  ```yaml
  occurrent:
    event-store:
      enabled: false # Disable the creation of an event store Spring bean
  ```
  
  and the subscriptions like this:

  ```yaml
  occurrent:
    subscription:
      enabled: false # Disable the creation of beans related to subscriptions
  ```                                                                        
  
  This is useful if you have an application where you only need the event store or only need the subscriptions.
* Added `queryForList` Kotlin extension function to `EventStoreQueries` and `DomainEventQueries`. It works in a similar way as `queryForSequence`, but returns a `List` instead of a Kotlin `Sequence`.
* Fixed an issue with `CatchupSubscriptionModel` in which it threw an IllegalArgumentException when storing the position of stored events when using Atlas free tier. 

### 0.17.0 (2024-01-19)
* spring-boot-starter-mongodb no longer autoconfigures itself by just importing the library in the classpath, instead you need to bootstrap by annotating your Spring Boot class with @EnableOccurrent.   
* Fixed bug in spring-boot-starter-mongodb module in which it didn't automatically configure MongoDB.
* Domain event subscriptions now accepts metadata as the first parameter, besides just the event. The metadata currently contains the stream version and stream id, which can be useful when building projections.
* Fixed a bug in SpringMongoSubscriptionModel in which it didn't restart correctly on non DataAccessException's 
* Introducing Decider support (experimental)
* Fixed a rare ConcurrentModificationException issue in SpringMongoSubscriptionModel if the subscription model is shutdown while it's restarting 
* Upgraded from Kotlin 1.9.20 to 1.9.22
* Upgraded amqp-client from 5.16.0 to 5.20.0
* Upgraded Spring Boot from 3.1.4 to 3.2.1
* Upgraded reactor from 3.5.10 to 3.6.0 
* Upgraded Spring data MongoDB from 4.1.4 to 4.2.0 
* Upgraded jobrunr from 6.3.2 to 6.3.3
* Upgraded mongodb drivers from 4.10.2 to 4.11.1
* Upgraded lettuce core from 6.2.6.RELEASE to 6.3.1.RELEASE
* Upgraded spring-aspects from 6.0.10 to 6.1.1
* Upgraded jackson from 2.15.2 to 2.15.3

### 0.16.11 (2023-12-01)
* Removed `isFinalError` method from `ErrorInfo` used by `RetryStrategy`, use `isLastAttempt()` instead.
* Added `RetryInfo` as argument to the `exec` extension function in `RetryStrategy`.
* Added `retryAttemptException` as an extension property to `org.occurrent.retry.AfterRetryInfo` so that you don't need to use the `getFailedRetryAttemptException` method that returns an `Optional` in the Java interface. Instead, the `retryAttemptException` function returns a `Throwable?`.  Import the extension property from the `org.occurrent.retry.AfterRetryInfoExtensions` file. 
* Added `nextBackoff` as an extension property to `org.occurrent.retry.ErrorInfo` so that you don't need to use the `getBackoffBeforeNextRetryAttempt` method that returns an `Optional` in the Java interface. Instead, the `nextBackoff` function returns a `Duration?`.  Import the extension property from the `org.occurrent.retry.ErrorInfoExtensions` file.
* In the previous version, in the retry strategy module, `onBeforeRetry`, `onAfterRetry`, `onError` etc, accepted a `BiConsumer<Throwable, RetryInfo>`. The arguments have now been reversed, so the types of the BiConsumer is now `BiConsumer<RetryInfo, Throwable>`.
* Added `onRetryableError` method to `RetryStrategy` which you can use to listen to errors that are retryable (i.e. matching the retry predicate). This is a convenience method for `onError` when `isRetryable` is true.
* Added Kotlin extensions to `JacksonCloudEventConverter`. You can import the function `org.occurrent.application.converter.jackson.jacksonCloudEventConverter` and use like this:
  
  ```kotlin
   val objectMapper = ObjectMapper()
   val cloudEventConverter: JacksonCloudEventConverter<MyEvent> =
      jacksonCloudEventConverter(
          objectMapper = objectMapper,
          cloudEventSource = URI.create("urn:myevents"),
          typeMapper = MyCloudEventTypeMapper()
      )
  ```
* Fixed problem with spring-boot autoconfiguration in which it previously failed to create a default cloud event converter if no type mapper was specified explicitly.
* Upgraded to Kotlin 1.9.20
* Added a "deleteAll" method to InMemoryEventStore which is useful for testing
* The `org.occurrent.eventstore.api.WriteConditon` has been converted to a java record.
* Removed the deprecated method "getStreamVersion" in `org.occurrent.eventstore.api.WriteConditon`, use `newStreamVersion()` instead. 

### 0.16.10 (2023-10-21)
* Several changes to `RetryStrategy` again:
  1. `onError` is will be called for each throwable again. The new `ErrorInfo` instance, that is supplied to the error listener, can be used to determine whether the error is "final" or if it's retryable.
  2. In the previous version, `onBeforeRetry` and `onAfterRetry`, accepted a `BiConsumer<RetryInfo, Throwable>`. The arguments have now been reversed, so the types of the BiConsumer is now `BiConsumer<Throwable, RetryInfo>`.  

### 0.16.9 (2023-10-20)
* Added `onAfterRetry` to `RetryStrategy`

### 0.16.8 (2023-10-20)
* Upgraded jakarta-api from 1.3.5 to 2.11 (which means that all javax annotations have been replaced by jakarta)
* Fixed a bug in CatchupSubscriptionModel that prevented it from working in MongoDB clusters that doesn't have access to the `hostInfo` command such as Atlas free-tier.
* Several changes to the `RetryStrategy`:
  1. Renamed `getNumberOfAttempts` to `getNumberOfPreviousAttempts`
  2. Added `getAttemptNumber` which is the number of the _current_ attempt
  3. `onError` is now _only_ called if the _end result_ is an error. I.e. it will only be called at most once, and not for intermediate errors. Because of this, the variant of `onError` that took a `BiConsumer<RetryInfo, Throwable>` has been removed (because there's no need for `RetryInfo` when the operation has failed). 
  4. Added the `onBeforeRetry` method, which is called before a _retry attempt_ is made. This function takes a `BiConsumer<RetryInfo, Throwable>` in which the `RetryInfo` instance contains details about the current retry attempt.    

### 0.16.7 (2023-09-29)
* Added equals/hashcode and toString to RetryInfo
* Small changes to how retries are performed in the competing consumer strategies for MongoDB
* Improved debug logging in competing consumer implementations
* Upgraded Spring Boot from 3.0.8 to 3.1.4
* Upgraded kotlin from 1.9.0 to 1.9.10
* Upgraded jobrunr from 6.3.0 to 6.3.2
* Upgraded spring data mongodb from 4.0.8 to 4.1.4
* Upgraded jackson from version 2.14.3 to 2.15.2
* Upgraded project reactor from 3.5.8 to 3.5.10
* Upgraded spring-retry from 2.0.0 to 2.0.3
* Upgraded lettuce-core from 6.2.2.RELEASE to 6.2.6.RELEASE

### 0.16.6 (2023-08-15)
* The SpringMongoSubscriptionModel is now restarted for all instances of `org.springframework.dao.DataAccessException` instead of just instances of `org.springframework.data.mongodb.UncategorizedMongoDbException`.
* Upgraded cloudevents from 2.4.2 to 2.5.0
* Upgraded Spring Boot from 3.0.7 to 3.0.8
* Upgraded project reactor from 3.5.6 to 3.5.8
* Upgraded spring data mongodb from 4.0.6 to 4.0.8
* Upgraded mongo driver from 4.8.1 to 4.10.2
* Upgraded jobrunr from 6.1.4 to 6.3.0

### 0.16.5 (2023-07-7)
* Improved debug logging in `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel`

### 0.16.4 (2023-07-7)
*  A kotlin extension function that makes it easier to execute a `RetryStrategy` with a "Supplier".
    
    The reasons for this is that when just doing this from kotlin:
    
    ```
    val string = retryStrategy.execute { "hello" }
    ```
    
    This will return `Unit` and not the "hello" string that you would expect.
    This is because execute in the example above delegates to org.occurrent.retry.RetryStrategy.execute(java.lang.Runnable)
    and not org.occurrent.retry.RetryStrategy.execute(java.util.function.Supplier<T>) which one would expect.
    Thus, you can use this function instead to avoid specifying the `Supplier` SAM explicitly.
    
    I.e. instead of doing:
    
    ```kotlin
      val string : String = retryStrategy.execute(Supplier { "hello" })
    ```
    
    you can do:
    
    ```kotlin
      val string : String = retryStrategy.exec { "hello" }
    ```
    
    after having imported `org.occurrent.retry.exec`.
* Kotlin jvm target is set to 17
* Added ability to map errors with `RetryStrategy`, either by doing:

  ```
  retryStrategy
              .mapError(IllegalArgumentException.class, IllegalStateException::new)
              .maxAttempts(2)
              .execute(() -> {
                  throw new IllegalArgumentException("expected");
              }));
  ```
  
  In the end, an `IllegalStateException` will be thrown. You can also do like this:

  ```
  retryStrategy
              .mapError(t -> {
                  if (t instanceof IllegalArgumentException iae) {
                      return new IllegalStateException(iae.getMessage());
                  } else {
                      return t;
                  }
              })
              .maxAttempts(2)
              .execute(() -> {
                  throw new IllegalArgumentException("expected");
              }));
  ```
* Added a new `execute` Kotlin extension function to the `ApplicationService` that allows one to use a `java.util.UUID` as a streamId when working with lists of events (as opposed to `Sequence`).
* Upgraded xstream from 1.4.19 to 1.4.20
* Added better logging to `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel`, including some debug logs that can be used for detailed information about what's going on. 
* Upgraded Kotlin from 1.8.21 to 1.9.0
* Upgraded Spring Boot from 3.0.6 to 3.0.7
* Upgraded Spring Aspects from 6.0.9 to 6.0.10

### 0.16.3 (2023-05-12)
* Added support to the retry module to execute retries with a function that takes an instance of `org.occurrent.retry.RetryInfo`. This is useful if you need to know the current state of your of the retry while retrying. For example:
  ```java  
  RetryStrategy retryStrategy = RetryStrategy
                                  .exponentialBackoff(initialDelay, maxDelay, 2.0)
                                  .maxAttempts(10)
  retryStrategy.execute(info -> {
        if(info.getNumberOfAttempts() > 2 &&  info.getNumberOfAttempts() < 6) {
            System.out.println("Number of attempts is between 3 and 5");
        }
        ...     
  });
  ```
* Fixed bug in the retry module, in which error listeners where not called for the last error.
* Upgraded jobrunr from 5.3.0 to 6.1.4
* Upgraded Kotlin from 1.8.0 to 1.8.21
* Upgraded Jackson from 2.14.1 to 2.14.3
* Upgraded project reactor from 3.5.0 to 3.5.6
* Upgraded to Spring Boot from 3.0.3 to 3.0.6
* Upgraded to Spring from 6.0.6 to 6.0.9
* Upgraded to Spring Data MongoDB from 4.0.0 to 4.0.6
* Upgraded cloudevents from 2.4.1 to 2.4.2

### 0.16.2 (2023-03-03)
* Upgraded Kotlin from 1.7.20 to 1.8.0
* Upgraded cloudevents to 2.4.1
* Improvements to `SpringMongoSubscriptionModel` in which it'll restart the subscription from the default subscription position instead of now on unknown or query-related MongoDB errors. This eliminates the risk of loosing messages if using a durable subscription model.
* Fixed a subtle bug in `SpringMongoLeaseCompetingConsumerStrategy` in which it _could_ crash in some cases where MongoDB was down for more than 30 seconds.
* Upgraded to Spring Boot 3.0.3
* Upgraded spring-aspects from 6.0.2 to 6.0.6

### 0.16.1 (2023-02-11)
* Fix error in the sequence command composition that leaves old events in the sequence (issue #131) (thanks to chrisdginn for pull request)

### 0.16.0 (2022-12-09)
* Occurrent now require Java 17 instead of Java 8. This is major change to support the latest Spring client libraries for various databases such MongoDB and Redis. 
  This was also done to better support Spring Boot 3 and jakartaee.
* Lots of changes under the hood, refactorings to make use of records, sealed classes and built-in functional constructs available in Java 17.
* Refactored SubscriptionPositionStorageConfig to sealed interface
* Refactored CompetingConsumerSubscriptionModel
* Refactored StartAt to a sealed interface
* Refactored ClassName to a sealed interface
* Refactored RetryStrategy to a sealed interface
* Converted Deadline to a sealed interface
* Converted CompetingConsumer in CompetingConsumerSubscriptionModel to a record
* Converting Backoff to sealed interface
* Converting Condition and WriteCondition to sealed interfaces
* Converting SortBy to a sealed interface
* Refactor MaxAttempts to sealed interface and implementations to records

### 0.15.1 (2022-12-02)
* The spring-boot-starter module now supports Spring Boot 3 (thanks to Kirill Gavrilov for pull request)

### 0.15.0 (2022-11-24)
Introducing deadline scheduling. Scheduling (aka deadlines, alarm clock) is a very handy technique to schedule to commands to be executed in the future or periodically.  
Imagine, for example, a multiplayer game, where we want to game to end automatically after 10 hours of inactivity.  
This means that as soon as a player has made a guess, we’d like to schedule a “timeout game command” to be executed after 10 hours.

The way it works in Occurrent is that you schedule a `org.occurrent.deadline.api.blocking.Deadline` using a `org.occurrent.deadline.api.blocking.DeadlineScheduler` implementation.
The `Deadline` is a date/time in the future when the deadline is up. You also register a `org.occurrent.deadline.api.blocking.DeadlineConsumer` to a 
`org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry` implementation, and it'll be invoked when a deadline is up. For example: 


```java
// In some method we schedule a deadline two hours from now with data "hello world" 
var deadlineId = UUID.randomUUID(); 
var deadlineCategory = "hello-world"; 
var deadline = Deadline.afterHours(2)
deadlineScheduler.schedule(deadlineId, deadlineCategory, deadline, "hello world");

// In some other method, during application startup, we register a deadline consumer to the registry for the "hello-world" deadline category
deadlineConsumerRegistry.register("hello-world", (deadlineId, deadlineCategory, deadline, data) -> System.out.println(data));
```

In the example above, the deadline consumer will print "hello world" after 2 hours.

There are two implementations of `DeadlineScheduler` and `DeadlineConsumerRegistry`, one that uses [JobRunr](https://www.jobrunr.io/) and one in-memory implementation.
Depend on `org.occurrent:deadline-jobrunr:0.15.0` to get the JobRunr implementation, and `org.occurrent:deadline-inmemory:0.15.0` to get the in-memory implementation. 

### 0.14.9 (2022-11-10)
* Upgraded Kotlin from 1.7.10 to 1.7.20
* Upgraded cloudevents from 2.3.0 to 2.4.0
* Upgraded Spring Boot from 2.7.3 to 2.7.5
* Changed toString() on StreamVersionWriteCondition when condition is null from "any stream version" to "any"
* Fixed a bug in SpringMongoEventStore when several writes happened in parallel to the same stream and write condition was "any". 
  This could result in a WriteConditionNotFulfilledException since the underlying MongoDB transaction failed. Now, after the fix, the events are stored as indented.

### 0.14.8 (2022-10-10)
* Fixed NPE issue in the toString() method in `org.occurrent.eventstore.api.StreamVersionWriteCondition` when stream condition was `any`.

### 0.14.7 (2022-09-22)
* Fixed issue in `SpringMongoSubscriptionModel` that prevented restart of subscriptions when MongoDB goes into leader election mode.
* Upgraded spring-boot to 2.7.3
* Upgraded Spring Data MongoDB from 3.3.4 to 3.3.7

### 0.14.6 (2022-08-17)
* InMemoryEventStore now checks for duplicate events. You can no longer write two events with the same cloud event id and source to the same stream.
* Fixed an issue with command composition in Kotlin in which, in version 0.14.5, returned _all_ events in a stream and not only _new_ events. 

### 0.14.5 (2022-07-29)
* Updated Kotlin extension functions for partial function application (`org.occurrent.application.composition.command.PartialExtensions`)
  to work on any type of function instead of just those that has `List` or `Sequence`.
* Fixed an issue in JacksonCloudEventConvert in which it didn't use the CloudEventTypeMapper correctly when calling `toCloudEvent` ([issue 119](https://github.com/johanhaleby/occurrent/issues/119)). 

### 0.14.4 (2022-07-15)
  
* Removed `PartialListCommandApplication`, `PartialStreamCommandApplication` and `PartialApplicationFunctions` in package 
  `org.occurrent.application.composition.command.partial` of module `command-composition`. They have all been replaced by
  `org.occurrent.application.composition.command.partial.PartialFunctionApplication` which is a generic form a partial function
  application that works on all kinds of functions, not only those taking `Stream` and/or `List`. A simple search and replace
  should be enough to migrate.
* Upgraded Jackson from 2.13.2 to 2.13.3
* Upgraded project reactor to 3.4.16 to 3.4.21
* Upgraded Spring Boot from 2.6.7 to 2.7.1
* Upgraded Java MongoDB driver from 4.5.1 to 4.6.1
* Upgraded Kotlin from 1.6.21 to 1.7.10

### 0.14.3 (2022-04-27)

* Upgraded to Kotlin from 1.6.0 to 1.6.21
* Upgraded project reactor to 3.4.12 to 3.4.16
* Upgraded Spring Data MongoDB from 3.3.0 to 3.3.0
* Upgraded Spring Boot from 2.5.6 to 2.6.7
* Upgraded Java MongoDB driver from 4.4.0 to 4.5.1
* Upgraded Java cloudevents SDK from 2.2.0 to 2.3.0 
* Upgraded Jackson from 2.13.0 to 2.13.2 
* Upgraded Jackson Databind from 2.13.0 to 2.13.2.1

### 0.14.2 (2021-12-10)

* Improved `SpringMongoEventStore`, `MongoEventStore` and `ReactorMongoEventStore` so that they never does in-memory filtering of events that we're not interested in.
* Added `oldStreamVersion` to `WriteResult` (that is returned when calling `write(..)` on an event store). The `getStreamVersion()` method has been deprecated in favor of `getNewStreamVersion()`.
* Upgraded to Kotlin 1.6.0
* Upgraded Java MongoDB driver to 4.4.0
* Upgraded Spring Data MongoDB to 3.3.0
* Upgraded Jackson to 2.13.0
* Upgraded amqp-client to 5.14.0

### 0.14.1 (2021-11-12)

* Using `insert` from `MongoTemplate` when writing events in the `SpringMongoEventStore`. Previously, the vanilla `mongoClient` was (accidentally) used for this operation.
* When using the spring boot starter project for MongoDB (`org.occurrent:spring-boot-starter-mongodb`), the transaction manager used by default is now configured to use "majority" read- and write concerns.
  To revert to the "default" settings used by Spring, or change it to your own needs, specify a `MongoTransactionManager` bean. For example:

  ```java                                                                       
  @Bean
  public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
    return new MongoTransactionManager(dbFactory, TransactionOptions.builder(). .. .build());
  }
  ```
* Separating read- and query options configuration so that you can e.g. configure queries made by `EventStoreQueries` and reads from the `EventStore.read(..)` separately.  
  This useful if you want to e.g. allow queries from `EventStoreQueries` to be made to secondary nodes but still force reads from `EventStore.read(..)` to be made from the primary.
  You can configure this by supplying a `readOption` (to configure the reads from the `EventStore`) and `queryOption` (for `EventStoreQueries`) in the `EventStoreConfig`. 
  This has been implemented for `SpringMongoEventStore` and `ReactorMongoEventStore`.

### 0.14.0 (2021-11-06)

* Non-backward compitable change: CloudEventConverter's now has a third method that you must implement:
  ```java
  /**
   * Get the cloud event type from a Java class.
   *
   * @param type The java class that represents a specific domain event type
   * @return The cloud event type of the domain event (cannot be {@code null})
   */
  @NotNull String getCloudEventType(@NotNull Class<? extends T> type);
  ```
  The reason for this is that several components, such as the [subscription dsl](https://occurrent.org/documentation#subscription-dsl), needs to get the cloud event type from the domain event class. And since this is highly related to "cloud event conversion", 
  this method has been added there to avoid complicating the API. 
* Introduced the concept of CloudEventTypeMapper's. A cloud event type mapper is component whose purpose it is to get the [cloud event type](https://occurrent.org/documentation#cloudevents) from a domain event type and vice versa.
  Cloud Event Type mappers are used by certain `CloudEventConverter`'s to define how they should derive the cloud event type from the domain event as well as a way to reconstruct the domain event type from the cloud event type.
  and the new domain queries DSL. You should use the same type mapper instance for all these components. To write a custom type mapper, depend on the `org.occurent:cloudevent-type-mapper-api` module and implement the `org.occurrent.application.converter.typemapper.CloudEventTypeMapper`
  (functional) interface.
* Introduced a blocking Query DSL. It's a small wrapper around the [EventStoreQueries](https://occurrent.org/documentation#eventstore-queries) API that lets you work with domain events instead of CloudEvents. 
  Depend on the `org.occurrent:query-dsl-blocking` module and create an instance of `org.occurrent.dsl.query.blocking.DomainEventQueries`. For example:

  ```java                                                      
  EventStoreQueries eventStoreQueries = .. 
  CloudEventConverter<DomainEvent> cloudEventConverter = ..
  DomainEventQueries<DomainEvent> domainEventQueries = new DomainEventQueries<DomainEvent>(eventStoreQueries, cloudEventConverter);
   
  Stream<DomainEvent> events = domainQueries.query(Filter.subject("someSubject"));
  ```
  
  There's also support for skip, limits and sorting and convenience methods for querying for a single event:

  ```java                                                      
  Stream<DomainEvent> events = domainQueries.query(GameStarted.class, GameEnded.class); // Find only events of this type
  GameStarted event1 = domainQueries.queryOne(GameStarted.class); // Find the first event of this type
  GamePlayed event2 = domainQueries.queryOne(Filter.id("d7542cef-ac20-4e74-9128-fdec94540fda")); // Find event with this id
  ```
  
  There are also some Kotlin extensions that you can use to query for a `Sequence` of events instead of a `Stream`:

  ```kotlin
  val events : Sequence<DomainEvent> = domainQueries.queryForSequence(GamePlayed::class, GameWon::class, skip = 2) // Find only events of this type and skip the first two events
  val event1 = domainQueries.queryOne<GameStarted>() // Find the first event of this type
  val event2 = domainQueries.queryOne<GamePlayed>(Filter.id("d7542cef-ac20-4e74-9128-fdec94540fda")) // Find event with this id
  ```
* Introducing spring boot starter project to easily bootstrap Occurrent if using Spring. Depend on `org.occurrent:spring-boot-starter-mongodb` and create a Spring Boot application annotated with `@SpringBootApplication` as you would normally do.
  Occurrent will then configure the following components automatically:
    * Spring MongoDB Event Store instance (`EventStore`)
    * A Spring `SubscriptionPositionStorage` instance 
    * A durable Spring MongoDB competing consumer subscription model (`SubscriptionModel`)
    * A Jackson-based `CloudEventConverter`
    * A `GenericApplication` instance (`ApplicationService`)
    * A subscription dsl instance (`Subscriptions`)
    * A reflection based type mapper that uses the fully-qualified class name as cloud event type (you _should_ absolutely override this bean for production use cases) (`CloudEventTypeMapper`)
      For example, by doing:
      ```java
      @Bean
      public CloudEventTypeMapper<GameEvent> cloudEventTypeMapper() {
        return ReflectionCloudEventTypeMapper.simple(GameEvent.class);
      }
      ```
      This will use the "simple name" (via reflection) of a domain event as the cloud event type. But since the package name is now lost, the `ReflectionCloudEventTypeMapper` will append the package name of `GameEvent` to when converting back into a domain event. 
      This _only_ works if all your domain events are located in the exact same package as `GameEvent`. If this is not that case you need to implement a more advanced `CloudEventTypeMapper` such as:

      ```kotlin
      class CustomTypeMapper : CloudEventTypeMapper<GameEvent> {
          override fun getCloudEventType(type: Class<out GameEvent>): String = type.simpleName
      
          override fun <E : GameEvent> getDomainEventType(cloudEventType: String): Class<E> = when (cloudEventType) {
              GameStarted::class.simpleName -> GameStarted::class
              GamePlayed::class.simpleName -> GamePlayed::class
              // Add all other events here!!
              ...
              else -> throw IllegalStateException("Event type $cloudEventType is unknown")
          }.java as Class<E>
      }
      ```
  See `org.occurrent.springboot.OccurrentMongoAutoConfiguration` if you want to know exactly what gets configured.
* Upgraded spring-boot from 2.5.4 to 2.5.6.

## 0.13.1 (2021-10-03)

* No longer using transactional reads in `ReactorMongoEventStore`, this also means that the `transactionalReads` configuration property could be removed since it's no longer used. 

## 0.13.0 (2021-10-03)

* Reading event streams from `MongoEventStore` and `SpringMongoEventStore` are now much faster and more reliable. Before there was a bug in both implementation in which
  the stream could be abruptly closed when reading a large number of events. This has now been fixed, and as a consequence, Occurrent doesn't need to start a MongoDB transaction
  when reading an event stream, which improves performance.
* Removed the `transactionalReads` property (introduced in previous release) from `EventStoreConfig` for both `MongoEventStore` and `SpringMongoEventStore` since it's no longer needed.
* Upgraded jackson from version 2.11.1 to 2.12.5

## 0.12.0 (2021-09-24)

* Added ability to map event type to event name in subscriptions DSL from Kotlin
* Upgraded Kotlin to 1.5.31
* Upgraded spring-boot used in examples to 2.5.4
* Upgraded spring-mongodb to 3.2.5
* Upgraded the mongodb java driver to 4.3.2
* Upgraded project reactor to 3.4.10
* Upgrading to cloudevents sdk 2.2.0
* Minor tweak in ApplicationService extension function for Kotlin so that it no longer converts the Java stream into a temporary Kotlin sequence before converting it to a List
* Allow configuring (using the `EventStoreConfig` builder) whether transactional reads should be enabled or disabled for all MongoDB event stores.
  This is an advanced feature, and you almost always want to have it enabled. There are two reasons for disabling it:
  1. There's a bug/limitation on Atlas free tier clusters which yields an exception when reading large number of events in a stream in a transaction.
     To workaround this you could disable transactional reads. The exception takes this form:
     ```
     java.lang.IllegalStateException: state should be: open
     at com.mongodb.assertions.Assertions.isTrue(Assertions.java:79)
     at com.mongodb.internal.session.BaseClientSessionImpl.getServerSession(BaseClientSessionImpl.java:101)
     at com.mongodb.internal.session.ClientSessionContext.getSessionId(ClientSessionContext.java:44)
     at com.mongodb.internal.connection.ClusterClockAdvancingSessionContext.getSessionId(ClusterClockAdvancingSessionContext.java:46)
     at com.mongodb.internal.connection.CommandMessage.getExtraElements(CommandMessage.java:265)
     at com.mongodb.internal.connection.CommandMessage.encodeMessageBodyWithMetadata(CommandMessage.java:155)
     at com.mongodb.internal.connection.RequestMessage.encode(RequestMessage.java:138)
     at com.mongodb.internal.connection.CommandMessage.encode(CommandMessage.java:59)
     at com.mongodb.internal.connection.InternalStreamConnection.sendAndReceive(InternalStreamConnection.java:268)
     at com.mongodb.internal.connection.UsageTrackingInternalConnection.sendAndReceive(UsageTrackingInternalConnection.java:100)
     at com.mongodb.internal.connection.DefaultConnectionPool$PooledConnection.sendAndReceive(DefaultConnectionPool.java:490)
     at com.mongodb.internal.connection.CommandProtocolImpl.execute(CommandProtocolImpl.java:71)
     at com.mongodb.internal.connection.DefaultServer$DefaultServerProtocolExecutor.execute(DefaultServer.java:253)
     at com.mongodb.internal.connection.DefaultServerConnection.executeProtocol(DefaultServerConnection.java:202)
     at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:118)
     at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:110)
     at com.mongodb.internal.operation.QueryBatchCursor.getMore(QueryBatchCursor.java:268)
     at com.mongodb.internal.operation.QueryBatchCursor.hasNext(QueryBatchCursor.java:141)
     at com.mongodb.client.internal.MongoBatchCursorAdapter.hasNext(MongoBatchCursorAdapter.java:54)
     at java.base/java.util.Iterator.forEachRemaining(Iterator.java:132)
     at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801)
     at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
     at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
     at java.base/java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:913)
     at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
     ```
     It's possible that this would work if you enable "no cursor timeout" on the query, but this is not allowed on Atlas free tier.
  2. You're set back by the performance penalty of transactions and are willing to sacrifice read consistency
  
  If you disable transactional reads, you _may_ end up with a mismatch between the version number in the `EventStream` and
  the last event returned from the event stream. This is because Occurrent does two reads to MongoDB when reading an event stream. First it finds the current version number of the stream (A),
  and secondly it queries for all events (B). If you disable transactional reads, then another thread might have written more events before the call to B has been made. Thus, the version number
  received from query A might be stale. This may or may not be a problem for your domain, but it's generally recommended having transactional reads enabled. Configuration example:
  ```java
  EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().transactionalReads(false). .. .build();
  eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
  ```
* Added ability to tweak query options for reads in the event store, for example cursor timeouts, allow reads from slave etc. You can configure this in the `EventStoreConfig` for each event store
  by using the `queryOption` higher-order function. For example:
  ```java
  EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.DATE)
                  .queryOptions(query -> query.noCursorTimeout().allowSecondaryReads()).build();
  var eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
  ```
  Note that you must <i>not</i> use this to change the query itself, i.e. don't use the `Query#with(Sort)` etc. Only use options such as `Query#cursorBatchSize(int)` that doesn't change the actual query or sort order.
  This is an advanced feature and should be used sparingly.
* Added ability to convert a `Stream` of cloud events to domain events and vice versa in the `CloudEventConverter` by overriding the new `toCloudEvents` and/or `toDomainEvents` methods. 
  The reason for overriding any of these methods is to allow adding things such as correlation id that should be the same for all events in a stream.
* Non-backward compatible change: The cloud event converter module name has changed from `org.occurrent:cloudevent-converter` to `org.occurrent:cloudevent-converter-api` 
* Non-backward compatible change: The generic cloud event converter (`org.occurrent.application.converter.generic.GenericCloudEventConverter`) has been moved to its own module, depend on `org.occurrent:cloudevent-converter-generic` to use it.
* Introduced a cloud event converter that uses XStream to (de-)serialize the domain event to cloud event data. Depend on `org.occurrent:cloudevent-converter-xstream` and then use it like this:
    ```java
    XStream xStream = new XStream();
    xStream.allowTypeHierarchy(MyDomainEvent.class);
    XStreamCloudEventConverter<MyDomainEvent> cloudEventConverter = new XStreamCloudEventConverter<>(xStream, URI.create("urn:occurrent:domain"));
    ```                                                                                                                                           
   You can also configure how different attributes of the domain event should be represented in the cloud event by using the builder, `new XStreamCloudEventConverter.Builder<MyDomainEvent>().. build()`. 
* Introduced a cloud event converter that uses Jackson to (de-)serialize the domain event to cloud event data. Depend on `org.occurrent:cloudevent-converter-jackson` and then use it like this:
    ```java
    ObjectMapper objectMapper = new ObjectMapper();
    JacksonCloudEventConverter<MyDomainEvent> cloudEventConverter = new JacksonCloudEventConverter<>(objectMapper, URI.create("urn:occurrent:domain"));
    ```                                                                                                                                           
   You can also configure how different attributes of the domain event should be represented in the cloud event by using the builder, `new JacksonCloudEventConverter.Builder<MyDomainEvent>().. build()`. 

## 0.11.0 (2021-08-13)

* Improved error message and version for write condition not fulfilled that may happen when parallel writers write to the same stream at the same time.
* Upgraded to cloud events java sdk to version 2.1.1
* Upgraded to Kotlin 1.5.21
* Added a `mapRetryPredicate` function to `Retry` that easily allows you to map the current retry predicate into a new one. This is useful if you e.g. want to add an additional predicate to the existing predicate. For example:

    ```java
    // Let's say you have a retry strategy:
    Retry retry = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(WriteConditionNotFulfilledException.class::isInstance);
    // Now you also want to retry if an IllegalArgumentException is thrown:
    retry.mapRetryPredicate(currentRetryPredicate -> currentRetryPredicate.or(IllegalArgument.class::isInstance))
    ```                                                                                                          
* The GenericApplicationService now has a RetryStrategy enabled by default. The default retry strategy uses exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
  each retry, if `WriteConditionNotFulfilledException` is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception. You can override the default strategy 
  by calling `new GenericApplicationService(eventStore, cloudEventConverter, retryStrategy)`. Use `new GenericApplicationService(eventStore, cloudEventConverter, RetryStrategy.none())` to revert to previous
  behavior.
* Upgraded spring-boot used in examples to 2.5.3
* Upgraded spring-mongodb to 3.2.3
* Upgraded the mongodb java driver to 4.3.1
* Added ability to write a single event to the event store instead of a stream. For example:

    ```java          
    CloudEvent event = ...
    eventStore.write("streamId", event);
    ```                                 
  This have been implemented for both the blocking and reactive event stores.

## 0.10.0 (2021-04-16)
                   
* The event store API's now returns an instance of `org.occurrent.eventstore.api.WriteResult` when writing events to the event store (previously `void` was returned). 
  The `WriteResult` instance contains the stream id and the new stream version of the stream. The reason for this change is to make it easier to implement use cases such
  as "read your own writes".
* The blocking ApplicationService `org.occurrent.application.service.blocking.ApplicationService` now returns `WriteResult` instead of `void`.
* Fixed bug in `InMemoryEventStore` that accidentally could skip version numbers when new events were inserted into the database.
* Improved detection of duplicate cloud event's in all MongoDB event stores
* Fixed a bug where `WriteConditionNotFulfilledException` was not thrown when a streams was updated by several threads in parallel (fixed for all mongodb event store implementations)
* Upgraded Spring Boot from 2.4.2 to 2.4.4
* Upgraded reactor from 3.4.2 to 3.4.4
* Upgraded spring-data-mongodb from 3.1.1 to 3.1.7
* Upgraded lettuce-core from 6.0.1 to 6.1.0
* Upgraded mongo java client from 4.1.1 to 4.2.2
* Upgraded spring-aspects from 5.2.9.RELEASE to 5.3.5
* Upgraded spring-retry from 1.3.0 to 1.3.1
* Upgraded kotlin from 1.4.31 to 1.4.32
* Upgraded kotlinx-collections-immutable-jvm from 0.3.2 to 0.3.4

## 0.9.0 (2021-03-19)
                                                                                                                                                                                        
* Fixed a bug in `InMemorySubscription` that accidentally pushed `null` values to subscriptions every 500 millis unless an actual event was received.
* Renamed `org.occurrent.subscription.mongodb.spring.blocking.SpringSubscriptionModelConfig` to `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig`.
* Upgraded to Kotlin 1.4.31
* All blocking subscriptions now implements the life cycle methods defined in the `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle` interface. A new interface, `org.occurrent.subscription.api.blocking.Subscribable`
  has been defined, that contains all "subscribe" methods. You can use this interface in your application if all you want to do is start subscriptions.
* Introduced a new default "StartAt" implementation called "default" (`StartAt.subscriptionModelDefault()`). This is different to `StartAt.now()` in that it will allow the subscription model 
  to choose where to start automatically if you don't want to start at an earlier position.
* Removed the ability to pass a supplier returning `StartAt` to the subscribe methods in `org.occurrent.subscription.api.blocking.Subscribable` interface. Instead, use `StartAt.dynamic(supplier)` to achieve the same result.
* Upgraded to CloudEvents Java SDK 2.0.0
* Waiting for internal message listener to be shutdown when stopping `SpringMongoSubscriptionModel`.
* Using a `org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor` as executor in `SpringMongoSubscriptionModel` instead of the default `org.springframework.core.task.SimpleAsyncTaskExecutor`. 
  The reason for this is that the `DefaultMessageListenerContainer` used internally in `SpringMongoSubscriptionModel` will wait for all threads in the `ThreadPoolTaskExecutor` to stop when stopping the
  `SpringMongoSubscriptionModel` instance. Otherwise, a race conditions can occur when stopping and then immediately starting a `SpringMongoSubscriptionModel`.
* Introducing competing consumer support! A competing consumer subscription model wraps another subscription model to allow several subscribers to subscribe to the same subscription. One of the subscribes will get a lock of the subscription
  and receive events from it. If a subscriber looses its lock, another subscriber will take over automatically. To achieve distributed locking, the subscription model uses a `org.occurrent.subscription.api.blocking.CompetingConsumerStrategy` to
  support different algorithms. You can write custom algorithms by implementing this interface yourself. Here's an example of how to use the `CompetingConsumerSubscriptionModel`. First add the `org.occurrent:competing-consumer-subscription` module to 
  classpath. This example uses the `NativeMongoLeaseCompetingConsumerStrategy` from module `org.occurrent:subscription-mongodb-native-blocking-competing-consumer-strategy`. It also wraps the [DurableSubscriptionModel](https://occurrent.org/documentation#durable-subscriptions-blocking) 
  which in turn wraps the [Native MongoDB](https://occurrent.org/documentation#blocking-subscription-using-the-native-java-mongodb-driver) subscription model.
  
  ```java
  MongoDatabase mongoDatabase = mongoClient.getDatabase("some-database");
  SubscriptionPositionStorage positionStorage = NativeMongoSubscriptionPositionStorage(mongoDatabase, "position-storage");
  SubscriptionModel wrappedSubscriptionModel = new DurableSubscriptionModel(new NativeMongoSubscriptionModel(mongoDatabase, "events", TimeRepresentation.DATE), positionStorage);
     // Create the CompetingConsumerSubscriptionModel
  NativeMongoLeaseCompetingConsumerStrategy competingConsumerStrategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(mongoDatabase);
  CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(wrappedSubscriptionModel, competingConsumerStrategy);
     // Now subscribe!
  competingConsumerSubscriptionModel.subscribe("subscriptionId", type("SomeEvent"));
  ```
  
  If the above code is executed on multiple nodes/processes, then only *one* subscriber will receive events.

## 0.8.0 (2021-02-20)

* Only log with "warn" when subscription is restarted due to "ChangeStreamHistoryLost".
* `InMemoryEventStore` now sorts queries by insertion order by default (before "time" was used)
* Added a new default compound index to MongoDB event stores, `{ streamid : 1, streamversion : 1}`. The reason for this is to get the events back in order when reading a stream from the event store _and_ 
  to make this efficient. Previous `$natural` order was used but this would skip the index, making reads slower if you have lots of data.
* Removed the index, `{ streamid : 1, streamversion : -1 }`, from all MongoDB EventStore's. It's no longer needed now that we have `{ streamid : 1, streamversion : 1}`.
* All MongoDB EventStore's now loads the events for a stream by leveraging the new `{ streamid : 1, streamversion : 1}` index.
* `CatchupSubscriptionModel` now sorts by time and then by stream version to allow for a consistent read order (see [MongoDB documentation](https://docs.mongodb.com/manual/reference/method/cursor.sort/#sort-consistency)).
  Note that the above is only true _if_ you supply a `TimeBasedSubscriptionPosition` that is _not_ equal to ``TimeBasedSubscriptionPosition.beginningOfTime()` (which is default if no filter is supplied).
* Major change in how you can sort the result from queries. Before you only had four options, "natural" (ascending/descending) and "time" (ascending/descending), now you can specify any support CloudEvent 
  field. This means that e.g. `SortBy.TIME_ASC` has been removed. It has been replaced with the `SortBy` API (`org.occurrent.eventstore.api.SortBy`), that allows you to do e.g.
  
  ```java
  SortBy.time(ASCENDING)
  ```
  
  Sorting can now be composed, e.g.

  ```java
  SortBy.time(ASCENDING).thenNatural(DESCENDING)  
  ```
  
  This has been implemented for all event stores.
* It's now possible to change how `CatchupSubscriptionModel` sorts events read from the event store during catch-up phase. For example:
  
  ```java
  var subscriptionModel = ...
  var eventStore = ..
  var cfg = new CatchupSubscriptionModelConfig(100).catchupPhaseSortBy(SortBy.descending(TIME));
  var catchupSubscriptionModel = CatchupSubscriptionModel(subscriptionModel, eventStore, cfg);  
  ```

  By default, events are sorted by time and then stream version (if two or more events have the same time).

## 0.7.4 (2021-02-13)

* Added better logging to `SpringMongoSubscriptionModel`, it'll now include the subscription id if an error occurs.
* If there's not enough history available in the mongodb oplog to resume a subscription created from a `SpringMongoSubscriptionModel`, this subscription model now supports restarting the subscription from the current 
  time automatically. This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It's disabled by default since it might not 
  be 100% safe (meaning that you can miss some events when the subscription is restarted). It's not 100% safe if you run subscriptions in a different process than the event store _and_ you have lot's of 
  writes happening to the event store. It's safe if you run the subscription in the same process as the writes to the event store _if_ you make sure that the
  subscription is started _before_ you accept writes to the event store on startup. To enable automatic restart, you can do like this:
  
  ```java
  var subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, SpringSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));
  ```
  
  An alternative approach to restarting automatically is to use a catch-up subscription and restart the subscription from an earlier date.
* Better shutdown handling of all executor services used by subscription models.
* Don't log to error when a `SpringMongoSubscriptionModel` subscription is paused right after it was created, leading to a race condition. This is not an error. It's now logged in "debug" mode instead.

## 0.7.3 (2021-02-11)

* Removed the automatic creation of the "streamid" index in all MongoDB event stores. The reason is that it's not needed since there's another (compound) index (streamid+version) and 
  queries for "streamid" will be covered by that index.

## 0.7.2 (2021-02-05)

* When running MongoDB subscriptions on services like Atlas, it's not possible to get the current time (global subscription position) when starting a new subscription since access is denied. 
  If this happens then the subscription will start at the "current time" instead (`StartAt.now()`). There's a catch however! If processing the very first event fails _and_ the application is restarted,
  then the event cannot be retried. If this is major concern, consider upgrading your MongoDB server to a non-shared environment.

## 0.7.1 (2021-02-04)
                                                                                                                                                   
* Removed `org.occurrent:eventstore-inmemory` as dependency to `org.occurrent:application-service-blocking` (it should have been a test dependency) 
* Including a "details" message in `DuplicateCloudEventException` that adds more details on why this happens (which index etc). This is especially useful
  if you're creating custom, unique, indexes over the events and a write fail due to a duplicate cloud event.
* Upgraded to Kotlin 1.3.40
* Upgraded project-reactor to 3.4.2 (previously 3.4.0 was used)
* When running MongoDB subscriptions on services like Atlas, it's not possible to get the current time (global subscription position) when starting a new subscription since access is denied. 
  If this happens then the local time of the client is used instead.

## 0.7.0 (2021-01-31)
                                 
* Introduced many more life-cycle methods to blocking subscription models. It's now possible to pause/resume individual subscriptions
  as well as starting/stopping _all_ subscriptions. This is useful for testing purposes when you want to write events 
  to the event store without triggering all subscriptions. The subscription models that supports this 
  implements the new `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle` interface.
  Supported subscription models are: `InMemorySubscriptionModel`, `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel`. 
* The `SpringMongoSubscriptionModel` now implements `org.springframework.context.SmartLifecycle`, which means that if you
  define it as a bean, it allows controlling it as a regular Spring life-cycle bean.
* Introduced the `org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel` interface. Subscription models
  that wraps other subscription models and delegates subscriptions to them implements this interface. 
  It contains methods for getting the wrapped subscription model. This is useful for testing
  purposes, if the underlying subscription model needs to stopped/started etc.
* Fixed a bug with command composition that accidentally included the "previous events" when invoking the generated composition function.
* Added more command composition extension functions for Kotlin. You can now compose lists of functions and not only sequences.
* The `SpringMongoSubscriptionModel` now evaluates the "start at" supplier passed to the `subscribe` method each time a subscription is resumed.
* Fixed a bug in `InMemorySubscription` where the `waitUntilStarted(Duration)` method always returned `false`.
* `InMemorySubscription` now really waits for the subscription to start when calling `waitUntilStarted(Duration)` and `waitUntilStarted`.
* Moved the `cancelSubscription` method from the `org.occurrent.subscription.api.blocking.SubscriptionModel` to the 
  `org.occurrent.subscription.api.blocking.SubscriptionModelCancelSubscription` interface. This interface is also extended by
  `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle`.
* Introduced a much improved `RetryStrategy`. You can now configure max attempts, a retry predicate, error listener as well as the backoff strategy.
  Retry is provided in its own module, `org.occurrent:retry`, but many modules already depend on this module transitively. Here's an example:
  
  ```java
  RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(200), 2.0)
                                     .retryIf(throwable -> throwable instanceof OptimisticLockingException)
                                     .maxAttempts(5)
                                     .onError((info, throwable) -> log.warn("Caught exception {}, will retry in {} millis")), throwable.class.getSimpleName(), info.getDuration().toMillis()));
  
  retryStrategy.execute(Something::somethingThing);  
  ```
  
  `RetryStrategy` is immutable, which means that you can safely do things like this:

  ```java
  RetryStrategy retryStrategy = RetryStrategy.retry().fixed(200).maxAttempts(5);
  // Uses default 200 ms fixed delay
  retryStrategy.execute(() -> Something.something());
  // Use 600 ms fixed delay
  retryStrategy.backoff(fixed(600)).execute(() -> SomethingElse.somethingElse());
  // 200 ms fixed delay again
  retryStrategy.execute(() -> Thing.thing());
  ```
  
## 0.6.0 (2021-01-23)

* Renamed method `shutdownSubscribers` in `DurableSubscriptonModel` to `shutdown`.
* Added default subscription name to subscription DSL. You can now do:

    ```kotlin
    subscriptions(subscriptionModel) {
        subscribe<NameDefined> { e ->
            log.info("Hello ${e.name}")
        }
    }
    ```
    
    The id of the subscription will be "NameDefine" (the unqualified name of the `NameDefined` class).
* Added `exists` method to `EventStoreQueries` API (both blocking and reactive). This means that you can easily check if events exists, for example:

    ```kotlin
    val doesSomeTypeExists = eventStoreQueries.exists(type("sometype"))
    ```
* Added retry strategy support to SpringMongoSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to NativeMongoSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to SpringRedisSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to SpringMongoSubscriptionModel. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when exceptions are thrown from the `action` callback (the callback that you implement to handle a `CloudEvent` instance from a subscription).
* All blocking subscription models will throw an `IllegalArgumentException` if a subscription is registered more than once.

## 0.5.1 (2021-01-07)

* Renamed `org.occurrent.subscription.redis.spring.blocking.SpringSubscriptionPositionStorageForRedis` to `SpringRedisSubscriptionPositionStorage`.
* Renamed `org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscription` to `ReactorMongoSubscriptionModel`.

## 0.5.0 (2021-01-06)

* Renamed `org.occurrent.subscription.api.blocking.BlockingSubscription` to `org.occurrent.subscription.api.blocking.SubscriptionModel`. The reason for this is that it was previously
  very confusing to differentiate between a `org.occurrent.subscription.api.blocking.BlockingSubscription` (where you start/cancel subscriptions) and a `org.occurrent.subscription.api.blocking.Subscription` 
  (the actual subscription instance). The same thinking has been applied to the reactor counterparts as well (`org.occurrent.subscription.api.reactor.ReactorSubscription` has now been renamed to `org.occurrent.subscription.api.reactor.SubscriptionModel`).
* Derivatives of `org.occurrent.subscription.api.blocking.BlockingSubscription` such as `PositionAwareBlockingSubscription` has been renamed to `org.occurrent.subscription.api.blockking.PositionAwareSubscriptionModel`.
* Derivatives of the reactor counterpart, `org.occurrent.subscription.api.reactor.PositionAwareReactorSubscription` has been renamed `to`, such as has been renamed to `org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel`.
* `org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSubscriptionModelConfig` has been renamed to `org.occurrent.subscription.blocking.catchup.CatchupSubscriptionModelConfig`. 
* `org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSubscriptionModel` has been renamed to `org.occurrent.subscription.blocking.catchup.CatchupSubscriptionModel`.
* `org.occurrent.subscription.util.blocking.AutoPersistingSubscriptionModelConfig` has been renamed to `org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig`.
* `org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence` has been renamed to `org.occurrent.subscription.blocking.durable.DurableSubscriptionModel`.
* `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionForMongoDB` has been renamed to `NativeMongoSubscriptionModel`.
* `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionPositionStorageForMongoDB` has been renamed to `NativeMongoSubscriptionPositionStorage`.
* Removed `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionWithPositionPersistenceInMongoDB`. Use an `org.occurrent.subscription.blocking.DurableSubscriptionModel` from module `org.occurrent:durable-subscription` instead.
* `org.occurrent.subscription.mongodb.spring.blocking.MongoDBSpringSubscription` has been renamed to `SpringMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB` has been renamed to `SpringMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoDBSubscriptionPositionStorage` has been renamed to `SpringMongoSubscriptionPositionStorage`.
* `org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionForMongoDB` has been renamed to `ReactorMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionPositionStorageForMongoDB` has been renamed to `ReactorSubscriptionPositionStorage`.
* `org.occurrent.subscription.util.reactor.ReactorSubscriptionWithAutomaticPositionPersistence` has been renamed to `org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel`.
* `org.occurrent.subscription.util.reactor.ReactorSubscriptionWithAutomaticPositionPersistenceConfig` has been renamed to `org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionConfig`.
* `org.occurrent.eventstore.mongodb.spring.reactor.SpringReactorMongoEventStore` has been renamed to `ReactorMongoEventStore` since "Spring" is implicit.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification` has been renamed to `MongoFilterSpecification`.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification` has been renamed to `MongoJsonFilterSpecification`.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification` has been renamed to `MongoBsonFilterSpecification`.
* `org.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer` has been renamed to `MongoCloudEventsToJsonDeserializer`.
* `org.occurrent.subscription.mongodb.internal.MongoDBCommons` has been renamed to `MongoCommons`.
* `org.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition` has been renamed to `MongoOperationTimeSubscriptionPosition`.
* `org.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition` has been renamed to `MongoResumeTokenSubscriptionPosition`.
* `org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper` has been renamed to `OccurrentCloudEventMongoDocumentMapper`.
* `org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore` has been renamed to `SpringMongoEventStore`.
* Renamed module `org.occurrent:subscription-util-blocking-catchup-subscription` to `org.occurrent:catchup-subscription`.
* Renamed module `org.occurrent:subscription-util-blocking-automatic-position-persistence` to `org.occurrent:durable-subscription`.
* Renamed module `org.occurrent:subscription-util-reactor-automatic-position-persistence` to `org.occurrent:reactor-durable-subscription`.
* Moved `org.occurrent.application.converter.implementation.GenericCloudEventConverter` to `org.occurrent.application.converter.generic.GenericCloudEventConverter`.
* Moved `org.occurrent.application.service.blocking.implementation.GenericApplicationService` to `org.occurrent.application.service.blocking.generic.GenericApplicationService`.
* Added a new "Subscription DSL" module that adds a domain event specific abstraction on-top of the existing subscription model api's. This DSL makes it easier to create subscriptions that are using
  domain events instead of cloud events. The module is called `org.occurrent:subscription-dsl`. For example:
  
  ```kotlin
  val subscriptionModel = SpringMongoSubscriptionModel(..)
  val cloudEventConverter = GenericCloudEventConverter<DomainEvent>(..)
  
  // Subscription DSL
  subscriptions(subscriptionModel, cloudEventConverter) {
    subscribe<GameStarted>("id1") { gameStarted ->
        log.info("Game was started $gameStarted")
    }
    subscribe<GameWon, GameLost>("id2") { domainEvent ->
        log.info("Game was either won or lost: $domainEvent")
    }
   subscribe("everything") { domainEvent ->
        log.info("I subscribe to every event: $domainEvent")
    }
  } 
  ```
* Implemented ability to delete cloud events by a filter in the in-memory event store.
* Added "listener" support to the in-memory event store. This means that you can supply a "listener" (a consumer) to the `InMemoryEventStore` constructor that
  will be invoked (synchronously) after new events have been written. This is mainly useful to allow in-memory subscription models.
* Added an in-memory subscription model that can be used to subscribe to events from the in-memory event store. Add module `org.occurrent:subscription-inmemory` and then instantiate it using:

  ```java
  InMemorySubscriptionModel inMemorySubscriptionModel = new InMemorySubscriptionModel();
  InMemoryEventStore inMemoryEventStore = new InMemoryEventStore(inMemorySubscriptionModel);
  
  inMemorySubscriptionModel.subscribe("subscription1", System.out::println);
  ```
* Renamed groupId `org.occurrent.inmemory` to `org.occurrent` for consistency. This means that you should depend on module `org.occurrent:eventstore-inmemory` instead of `org.occurrent.inmemory:eventstore-inmemory` when using the in-memory event store.
* Added support for querying the in-memory event store (all fields expect the "data" field works)
* Changed from `Executor` to `ExecutorService` in `NativeMongoSubscriptionModel` in the `org.occurrent:subscription-mongodb-native-blocking` module.
* Added a `@PreDestroy` annotation to the `shutdown` method in the `NativeMongoSubscriptionModel` implementation so that, if you're frameworks such as Spring Boot, you don't need to explicitly call the `shutdown` method when stopping.
* Added partial extension functions for `List<DomainEvent>`, import from the `partial` method from `org.occurrent.application.composition.command`. 

## 0.4.1 (2020-12-14)

* Upgraded to Kotlin 1.4.21
* Upgraded to cloud events 2.0.0.RC2

## 0.4.0 (2020-12-04)

* Upgraded to Kotlin 1.4.20
* Upgraded to cloud events 2.0.0.RC1
* Breaking change! The attributes added by the Occurrent cloud event extension has been renamed from "streamId" and "streamVersion" to "streamid" and "streamversion" to comply with the [specification](https://github.com/cloudevents/spec/blob/master/spec.md#attribute-naming-convention).
* Added optimized support for `io.cloudevents.core.data.PojoCloudEventData`. Occurrent can convert `PojoCloudEventData` that contains `Map<String, Object>` and `String` efficiently.
* Breaking change! Removed `org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventData` since it's no longer needed after the CloudEvent SDK has introduced `PojoCloudEventData`. Use `PojoCloudEventData` and pass the document or preferably, map, to it.
* Removed the `org.occurrent:application-service-blocking-kotlin` module, use `org.occurrent:application-service-blocking` instead. The Kotlin extension functions are provided with that module instead.
* Added partial function application support for Kotlin. Depend on module `org.occurrent:command-composition` and import extension functions from `org.occurrent.application.composition.command.partial`. This means that instead of doing:
    
  ```kotlin                                                
  val playerId = ...
  applicationService.execute(gameId) { events -> 
    Uno.play(events, Timestamp.now(), playerId, DigitCard(Three, Blue))
  }
  ```                                           
  
  you can do:

  ```kotlin                                                
  val playerId = ...
  applicationService.execute(gameId, Uno::play.partial(Timestamp.now(), playerId, DigitCard(Three, Blue))) 
  ```
* Added command composition support for Kotlin. Depend on module `org.occurrent:command-composition` and import extension functions from `org.occurrent.application.composition.command.*`. This means that you 
  can compose two functions like this using the `andThen` (infix) function:

    ```kotlin
    val numberOfPlayers = 4
    val timestamp = Timestamp.now()
    applicationService.execute(gameId, 
        Uno::start.partial(gameId, timestamp, numberOfPlayers) 
                andThen Uno::play.partial(timestamp, player1, DigitCard(Three, Blue)))
    ```  

    In the example above, `start` and `play` will be composed together into a single "command" that will be executed atomically.

    If you have more than two commands, it could be easier to use the `composeCommand` function instead of repeating `andThen`:
                                  
    ```kotlin
    val numberOfPlayers = 4
    val timestamp = Timestamp.now()
    applicationService.execute(gameId, 
        composeCommands(
            Uno::start.partial(gameId, timestamp, numberOfPlayers), 
            Uno::play.partial(timestamp, player1, DigitCard(Three, Blue)),
            Uno::play.partial(timestamp, player2, DigitCard(Four, Blue))
        )
    )
    ```
* Added Kotlin extension functions to the blocking event store. They make it easier to write, read and query the event store with Kotlin `Sequence`'s. Import extension functions from package `org.occurrent.eventstore.api.blocking`.
* Added support for deleting events from event store using a `org.occurrent.filter.Filter`. For example:

    ```java
    eventStoreOperations.delete(streamId("myStream").and(streamVersion(lte(19L)));
    ```
    
    This will delete all events in stream "myStream" that has a version less than or equal to 19. This is useful if you implement "closing the books" or certain types of snapshots, and don't need the old events anymore.
    This has been implemented for all MongoDB event stores (both blocking and reactive) but not for the InMemory event store.

## 0.3.0 (2020-11-21)
* Upgraded Java Mongo driver from 4.0.4 to 4.1.1
* Upgraded to cloud events 2.0.0-milestone4. This introduces a breaking change since the `CloudEvent` SDK no longer returns a `byte[]` as data but rather a `CloudEventData` interface.
  You need to change your code from:
  
  ```java
  byte[] data = cloudEvent.getData();
  ```           
  
  to 
  
  ```java
  byte[] data = cloudEvent.getData().toBytes();
  ```
* Fixed so that not only JSON data can be used as cloud event data. Now the content-type of the event is taken into consideration, and you can store any kind of data.
* Introduced `org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventData`, cloud event data will be represented in this format with loading events from an event store.
  This means that you could check if the `CloudEventData` returned by `cloudEvent.getData()` is instance of `DocumentCloudEventData` and if so extract the 
  underlying `org.bson.Document` that represent the data in the database.      
* Occurrent no longer needs to perform double encoding of the cloud event data if content type is json. Instead of serializing the content manually to a `byte[]` you can
  use either the built-in `JsonCloudEventData` class from the `cloudevents-json-jackson` module, or 
  use the `DocumentCloudEventData` provided by Occurrent to avoid this.
* Upgrading to spring-data-mongodb 3.1.1
* Upgrading to reactor 3.4.0
* The MongoDB event stores no longer needs to depend on the `cloudevents-json-jackson` module since Occurrent now ships with a custom event reader/writer. 
* The MongoDB event subscriptions no longer needs to depend on the `cloudevents-json-jackson` module since Occurrent now ships with a custom event reader/writer. 

## 0.2.1 (2020-11-03)
* Fixed typo in `CatchupSupportingBlockingSubscriptionConfig`, renamed method `dontSubscriptionPositionStorage` to `dontUseSubscriptionPositionStorage`.
* Added `getSubscriptionPosition()` to `PositionAwareCloudEvent` that returns `Optional<SubscriptionPosition>`.
* Removed duplicate `GenericCloudEventConverter` located in the `org.occurrent.application.service.blocking.implementation` package. Use `org.occurrent.application.converter.implementation.CloudEventConverter` instead.
* Handling if the domain model returns a null `Stream<DomainEvent>` in the `GenericApplicationService`. 

## 0.2.0 (2020-10-31)
* Renamed method `CloudEventWithSubscriptionPosition.getStreamPosition()` to `CloudEventWithSubscriptionPosition.getSubscriptionPosition()` since this was a typo.
* Added ability to provide a list of conditions when composing them with `and` and `or`.
* Added special convenience (overloaded) method for creating "or" with "equal to" conditions. For example you can now do: `filter(type(or("x", "y"))`. 
  Before you had to do: `filter(type(or(eq("x"), eq("y")))`.
* MongoDB event streams are now explicitly sorted by natural order by default. The reason for this is that just relying on default "sorting" on read lead to wrong order on certain occasions.
* Writing an empty stream to a mongodb-based event store will just ignore the stream and not try to persist the empty stream to the datastore.
* Upgraded to cloudevents sdk 2.0.0-milestone3
* Non-backward compatible change: `CatchupSupportingBlockingSubscription` no longer requires a subscription position storage during the catch-up phase. 
  Instead, you pass the storage implementation to `CatchupSupportingBlockingSubscriptionConfig` along with the position persistence predicate.
* `BlockingSubscriptionWithAutomaticPositionPersistence` now implements the `PositionAwareBlockingSubscription` interface
* Removed the generic type T from the `org.occurrent.subscription.api.blocking.SubscriptionModel` and `org.occurrent.subscription.api.reactor.SubscriptionModel`.
  The reason for this was the implementation returning different kinds of CloudEvent implementations where not compatible. For example if you created a Spring Bean
  with a `T` of `CloudEventWithSubscriptionPosition` then such a subscription couldn't be assigned to a field expecting a subscription with just `CloudEvent`.
  To avoid having users to know which cloud event implementation to expect, we change the API so that it always deals with pure `CloudEvent`'s. 
  Implementors now have to use `org.occurrent.subscription.PositionAwareCloudEvent.getSubscriptionPositionOrThrowIAE(cloudEvent)` to get the position.
  It's also possible to check if a `CloudEvent` contains a subscription position by calling `org.occurrent.subscription.PositionAwareCloudEvent.hasSubscriptionPosition(cloudEvent)`.
* Fixed several corner-cases for the `CatchupSupportingBlockingSubscription`, it should now be safer to use and produce fewer duplicates when switching from catch-up to continuous subscription mode.
* Added "exists" method to the `BlockingSubscriptionPositionStorage` interface (and implemented for all implementations of this interface).
* The global position of `PositionAwareBlockingSubscription` for MongoDB increases the "increment" of the current `BsonTimestamp` by 1 in order to avoid 
  duplicate potential duplication of events during replay.
* Added a generic application service implementation (and interfaces). You don't have to use it, it's ok to simply cut and paste and make custom changes. You 
  can also write your own class. The implementation, `org.occurrent.application.service.blocking.implementation.GenericApplicationService`, quite 
  simplistic but should cover most of the basic use cases. The application service uses a `org.occurrent.application.converter.CloudEventConverter` to
  convert to and from cloud events and your custom domain events. This is why both `CloudEventConverter` and `ApplicationService` takes a generic type parameter, `T`, 
  which is the type of your custom domain event. Note that the application service is not yet implemented for the reactive event store.
  The application service also contains a way to execute side-effects after the events are written to the event store. This is useful for executing 
  synchronous policies after the events are written to the event store. If policies write the same database as your event store,  you start a transaction
  and write both policies and events in the same transaction!         
  There are also Kotlin extension functions for the application service and policies in the `org.occurrent:application-service-blocking` module.
* Added utilities, `org.occurrent:command-composition` for to easier do command composition when calling an application service.
  This module also contains utilities for doing partial application of functions which can be useful when composing functions.    

## 0.1.1 (2020-09-26):

* Catchup subscriptions (blocking)
* EveryN for stream persistence (both blocking and reactive)
* Added "count" to EventStoreQueries (both blocking and reactive)
* Added ability to query for "data" attribute in EventStoreQueries and subscriptions
