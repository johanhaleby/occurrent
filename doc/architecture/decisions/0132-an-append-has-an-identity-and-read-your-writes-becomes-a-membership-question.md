# 132. An append has an identity, and read-your-writes becomes a membership question

Date: 2026-08-17

## Status

Accepted. Builds the design [ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md) recorded as the
direction it could not build, and defines the implementation that
[#740](https://github.com/johanhaleby/occurrent/issues/740) tracks. This ADR decides a design and writes no code.
The implementation is epic scale and reaches the cloud event extension, all four event stores, both result records,
`EventMetadata`, the projection DSL on both stacks, both Mongo starters, and the TCK.

[#361](https://github.com/johanhaleby/occurrent/issues/361), which ADR 122 reopened, closes when that
implementation ships, and it closes on different terms than it asked for. It requests a wait for a projection
having applied every event up to a global position, which is the mechanism ADR 122 refuted. What ships instead
answers whether a projection has applied a named append, and decision 10 states where that is weaker. The goal
#361 describes is met and the mechanism it names is not built.

## Context

A caller that has just written wants to know when an asynchronous projection has applied that write.
[ADR 111](0111-a-projection-records-the-position-it-has-applied.md) answered it by recording the position a
projection had applied and waiting for that value to pass the caller's position. ADR 122 withdrew the feature,
because a position on MongoDB is reserved before commit while a change stream delivers in commit order
([ADR 84](0084-what-a-position-guarantees.md)), so a recorded maximum claims events that have not arrived yet.

That is settled, and this ADR does not reopen it. Nothing below reasons about the order events are delivered in,
about contiguity, or about how far a projection has got. ADR 122 is the binding refutation of that whole family.

### The question changes shape

ADR 122 named the replacement without building it. Mint one identifier per append, stamp it on every event of that
append the way `OccurrentCloudEventExtension` already stamps a position, return it from `WriteResult` and
`DcbAppendResult`, and have a projection record which identifiers it has applied. The wait then asks whether this
particular projection has already applied this particular append, which is a question about membership of a set
rather than about a position on a line.

Membership is answerable where the position question is not, for two reasons. It assumes nothing about the order
events arrive in. The case that broke the position design, where a writer reserves position 471 and stalls while a
writer holding 500 commits ahead of it, does nothing to a membership answer, because the wait asks about the
stalled writer's own append rather than about anything that overtook it.

The second reason is filtering. A subscription whose filter is pushed server-side into the change stream can still
answer the membership question, because a projection does not need to have seen the events it filtered out to know
which appends it applied.

The same design ports unchanged to a future PostgreSQL or MySQL store, where `BIGSERIAL` and `AUTO_INCREMENT` are
non-transactional for the same reason MongoDB's counter is and produce the identical mismatch between position and
commit order.

### What a replay does to a record of membership

A projection that replays the event store from the beginning would otherwise record every historical append
identifier it meets, and three separate things go wrong when it does.

The first is volume. A full replay over a large store inserts one membership record per handled event for no
benefit, since a replay is exactly the period when the projection has nothing useful to answer.

The second is that a replayed update returning does not mean the read model changed.
`CoalescingMaterializedView` buffers updates in memory while a replay runs, writing a batch out whenever the buffer
reaches its batch size and again when the replay completes (`CoalescingMaterializedView.java:90-95`, `:107-110`).
An abandoned replay discards whatever has not been written yet (`:113-117`). So an identifier recorded when the
delegate's update returned would, for every event still in that buffer, describe state nothing ever wrote.

The third is rebuilds. `CancellableSubscriptions` discards the checkpoint by contract, and
`ProjectionAnnotationRegistrar` backfills a new or rebuilt projection from the event store. Membership records
keyed by projection identifier alone outlive that rebuild, so a wait would answer `true` about a read model that
was wiped and is being built again from scratch.

### What each stack can be asked about its own replay

`ReplayAwareSubscriptions.isCatchingUp(subscriptionId)` already answers whether a subscription is still replaying,
and `SagaAnnotationRegistrar.java:235` already builds `isRunning(id) && !isCatchingUp(id)` to hold a saga's timers
back until the handover. So the question this design needs is one the library already asks in production, and no
new catch-up machinery is required. What it needs is a way to reach the model that can answer.

The saga asks it on its timer poll rather than per delivery, since `SagaRunner` evaluates that supplier inside the
task it hands to `scheduleWithFixedDelay`. This design asks per delivery instead, which is a higher frequency than
the existing precedent, and that is affordable because the answer is a lookup in a map the catch-up model keeps in
memory (`AbstractCatchupSubscriptionModel.java:144-148`) rather than a read from any store.

The two stacks differ there, and the difference decides part of this design. Blocking
`SubscriptionModelCapability.capability(Class)` unwraps a `SubscriptionModelWrapper` chain until it finds the
capability (`SubscriptionModelCapability.java:50-57`), so a lookup reaches an inner catch-up model however many
wrappers are in front of it. The reactor version is a direct `instanceof` against the object handed to it and says
so in its own javadoc (`SubscriptionModelCapability.java:37-47`), because that stack has no wrapper type to unwrap.

The default reactive Mongo composition is arranged so that difference matters.
`OccurrentReactiveMongoAutoConfiguration.occurrentDurableSubscriptionModel` marks a
`ReactorDurableSubscriptionModel` as `@Primary` and gives it `composeCatchupLayer(...)` to wrap, so a real
`ReactorCatchupSubscriptionModel` is running one level in. `ReactorDurableSubscriptionModel` implements
`CheckpointAwareSubscriptionModel`, `SubscriptionModel` and `IntrospectableSubscriptions`, but not
`ReplayAwareSubscriptions`. Asking that bean whether it is replaying answers empty, and empty means the model
cannot say, which reads the same as a model that never replays.

A design that decided by asking the model alone would therefore treat the standard reactive wiring as one that
never replays, and record straight through a genuine replay. That is the exact untruth this design exists to
prevent, so decision 8 does not ask the model alone.
[#842](https://github.com/johanhaleby/occurrent/issues/842) tracks closing that difference between the stacks.

## Decision

### 1. `AppendId` is a value type, and the event store API owns it

`AppendId` is a value type wrapping a fresh UUID, never a bare `String` or `long`. This is the principle
[ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) applied on the DCB write path, and ADR 122 identified a
`waitUntilApplied(String, long, Duration)` signature as repeating the mistake ADR 21 built a type-level guard
against.

It lives in `eventstore/api/common`, package `org.occurrent.eventstore.api`, beside `WriteResult`. The owner is
argued on what defines the concept, not on which module happens to reach it. An append identifier is minted by a
store inside a write or append call, and what it means is defined entirely by that operation. `DcbConsistencyToken`
is the closest existing thing, a value type a store hands back, living beside the result it is returned with.

The alternative was `cloudevents-extension`, and it is worth saying what it would buy, because reachability alone
argues for it. Both event store API modules already depend on that module, so an `AppendId` there could be returned
from a typed `EventMetadata` accessor directly. Against that, `cloudevents-extension` describes how Occurrent
stamps a CloudEvent and contains `EventMetadata`, `OccurrentCloudEventExtension` and two helpers, and no value type
at all. Putting `AppendId` there would make the module that defines the wire format also define what an append is,
which is the wrong way round. Deciding placement by which module can already reach a type is how a concept ends up
owned by whatever happened to import it first, so the placement is decided on ownership instead.

`eventstore/api/dcb` depends on `eventstore/api/common`, so `DcbAppendResult` reaches the type, and
`dsl/projection-dsl/common` already depends on `occurrent-eventstore-api-dcb`, so the projection layer reaches it
with no new dependency edge.

The extension key stays on `OccurrentCloudEventExtension` regardless of where the type lives, next to
`STREAM_ID`, `STREAM_VERSION` and `POSITION`.

Two readers exist rather than one. `EventMetadata` gains `getAppendId()` returning a nullable `String`, nullable
for the same reason `getPosition()` on that class is, so a caller holding metadata asks it directly instead of
routing through another type. `AppendId.from(EventMetadata)` returns an `Optional<AppendId>` and lives with `AppendId`, so
both stacks' recorders share one decision about what a missing extension means instead of writing that null check
twice.

### 2. The key is `appendid`, and stamping does not depend on `streamPositionEnabled`

The extension key is `appendid`, following the lowercase unseparated form of `streamid`, `streamversion` and
`position`. A write or append call that persists at least one event mints one identifier, and every event that call
persists is stamped with it. A call that persists nothing mints none, which is what decision 4 rests on.

Stamping happens in each store's own per-append loop, and not in
`OccurrentCloudEventMongoDocumentMapper.convertToDocument`, which is called per event and knows nothing about the
call it belongs to. Each of the four stores therefore stamps in its own code, on both the stream path and the DCB
path, and the two in-memory stamping methods already run under `synchronized(state)`.

Stamping is unconditional. `streamPositionEnabled`
(`eventstore/mongodb/native/.../nativedriver/EventStoreConfig.java:62`) governs whether the stream path
stamps a position, and an append identifier is not a position, so an application that turned position stamping off
still gets read-your-writes. Making the identifier depend on that setting would disable the feature through a
setting that names something else.

`updateEvent` keeps the identifier already stored on the event. `MongoEventStore.updateCloudEvent`
(`MongoEventStore.java:739`) calls the same `convertToDocument` the write path uses, and that path rewrites an event
that was persisted by some earlier append. Restamping it would move an event out of the append it belongs to and
into an append that never happened, so the stored value is preserved.

An `appendid` supplied by a caller on the way in is overwritten by the store. This follows `position`, which the
store also assigns rather than accepts. It differs from `dcbtags`, which the stream write path refuses outright
(`InMemoryEventStore.rejectDcbTaggedEvents`), and the difference is what the store can do about the value. A
DCB-tagged event written through the stream path would be invisible to DCB reads afterwards, which the store cannot
put right, so it refuses. An append identifier the store overwrites is simply replaced by the correct one.

### 3. `WriteResult` and `DcbAppendResult` gain an `Optional<AppendId>` component

Both records gain an `Optional<AppendId> appendId` component, and both keep their current arity as a secondary
constructor that delegates with `Optional.empty()`. `DcbReadOptions` in the same module family is the precedent for
both halves, since it already has an `OptionalInt` record component and two delegating secondary constructors
(`DcbReadOptions.java:44,76-78,83-85`).

`Optional` rather than a nullable component is decided here rather than left to the implementation, because the
empty-append rule in decision 4 depends on it. The compiler puts the absent case in the caller's hands before a
wait can be attempted. A nullable accessor in the `EventMetadata.getPosition()` idiom would instead hand the
routine empty-append path a null that `waitUntilApplied` rejects at runtime, which is the footgun this project
prefers to prevent at the type level.

Keeping the old arity is the decision that needed an argument, since
[AGENTS.md](../../../AGENTS.md) prefers a clean break with a migration path over keeping a design known to be
wrong. The derivation is that `Optional<AppendId>` is the correct long-term component whichever migration is
chosen, because an empty append has no identifier permanently rather than temporarily. Both routes therefore
converge on the identical API, and AGENTS.md's preference is for breaking rather than preserving a mistake, not for
breaking where compatibility preserves nothing wrong. A three-argument constructor that delegates to
`Optional.empty()` states a true fact about a result built without an identifier, so it preserves no mistake. The
hard break, a four-argument canonical constructor only, was considered and rejected on that reasoning.

Two things break anyway, and the secondary constructor hides neither.

The first is equality. Both records now compare an identifier that is freshly minted per call whenever the append
persisted an event, so an external test asserting `isEqualTo(new WriteResult(a, b, c))` starts failing against any
result that has one. An append that persisted nothing compares `Optional.empty()` on both sides under decision 4,
so those assertions keep passing, which makes this a break that shows up only on the paths that wrote something.

The second is record deconstruction. A canonical constructor with four components means an external
`case WriteResult(var streamId, var oldVersion, var newVersion)` pattern stops compiling, whatever secondary
constructors exist, because a record pattern has to name every component. Nothing in this repository uses those
patterns on either record, and external callers are not observable from here, so this is stated as a second shape
the break takes rather than as a measured impact.

Both go in a `#### Breaking changes` changelog entry and a migration-guide section, the equality one with the move
to per-component assertions and the pattern one with the added component.

The equality break gets no OpenRewrite recipe, and the guide says why rather than staying silent about it, because
a recipe cannot derive the identifier a rewritten assertion would have to expect. The record patterns are a
different case, since the canonical arity does change from three components to four and adding the fourth binding
is a mechanical rewrite, so whether a recipe covers them is for the implementation to decide rather than for this
ADR to rule out.

### 4. An empty append has no identifier

`ApplicationService.execute` writes zero events as a matter of routine.
`GenericApplicationService.java:141-147` passes whatever the domain function returned straight to
`eventStore.write` with no emptiness check, and `InMemoryEventStore.java:220-226` has an explicit branch building a
result for an append that added nothing. An append that persisted no events stamped nothing, so its result has an
absent identifier, which is the only honest answer available.

What the documentation must not say is that an absent identifier means nothing was written, because absence has a
second cause. The retained three-argument constructors answer `Optional.empty()` for any result built through
them, including a write that persisted events, so a third-party or not-yet-upgraded store returns an absent
identifier on every write it does. Reading that as an empty append would tell a caller their events never happened.

So absence is documented as no append identity being available, with two causes named. Either the append persisted
nothing, and then there is genuinely nothing to wait for, or the store did not supply an identity, and then events
were written but this feature cannot answer for them. A caller that needs to tell the two apart has the rest of the
result, since a store that wrote something still reports its stream versions or its event count.

The TCK asserts the absence for the empty append, which is the case Occurrent's own stores produce.

### 5. `AppliedAppendStore` answers `hasApplied`, and the wait polls it

The membership record is store-backed, not in-process. A wait can run on a different node than the projection that
did the recording, because competing consumers move a subscription between nodes and nothing else shares
projection-side state across them.

`AppliedAppendStore` lives in `dsl/projection-dsl/common` with four operations. `recordApplied(projectionId,
appendId)` writes one membership record. `hasApplied(projectionId, appendId)` answers whether that record exists.
`clear(projectionId)` deletes every record for a projection, which decision 7 needs. A static `inMemory()` returns
the implementation for tests and for single-node applications.

The wait is a default method on the interface rather than a fifth operation each implementation writes again,
`waitUntilApplied(String projectionId, AppendId appendId, Duration timeout)` with an overload taking a `Backoff`.
It polls `hasApplied` on the backoff schedule the withdrawn machinery used, starting at 25 ms and settling at
250 ms, so a repeated wait does not turn into a tight loop against the store.

`appendId` is not nullable and there is no overload accepting an absent one. Decision 3's `Optional` component is
what makes that a contract rather than a request, since an empty append hands the caller no value that reaches the
wait without being opened first.

Mongo implementations on both stacks live in the starters and take a `RetryStrategy` in the shape
`NativeMongoCheckpointStorage` established, so a transient outage of the store does not turn into a failed wait.

Each poll's read is limited to the time the wait has left, and that is part of this decision rather than an
implementation detail. A default `RetryStrategy` retries without a limit, since `RetryImpl`'s no-argument
constructor builds itself with `infinite()` attempts, so a poll that inherits it can go on retrying straight
through the caller's timeout and then throw instead of answering `false`. The withdrawn machinery shipped that
defect and fixed it in [#730](https://github.com/johanhaleby/occurrent/issues/730) by limiting each poll's read to
the wait's remaining time. Both stacks apply that limit here, so the timeout the caller asked for is the one they
get.

### 6. Nothing is recorded while a projection is reading history

A recorder records an identifier only when its projection is past the history its replay set out to read. While that
history is being read it records nothing at all.

One rule closes all three problems from the context above. A full replay inserts nothing, so the volume problem
disappears. An identifier is never recorded for an update that a coalescing view buffered rather than wrote,
because the recorder does not run during the replay when buffering happens. And an abandoned replay has nothing
recorded to become untrue when the buffer is discarded.

A catch-up is two parts, not one, and this rule is about the first of them. Every catch-up in this library reads the
history that already existed when it started, and then delivers the events written while it was doing that. The
blocking stream model reads a delta by insertion order after its bulk read, the position models reconcile up to a
freshly read head, and the push model drains the live events it buffered during the replay. All three deliver those
events through the same action the history went through, and for some of them that is the only delivery there will
ever be, because the live subscription resumes past them or the broker was already told they were handled.

So a recorder that treated the whole catch-up as a replay would record nothing for an append the projection did
apply, and a wait for that append would never finish. That is what
[#890](https://github.com/johanhaleby/occurrent/issues/890) reports. The three reasons above are reasons about
history and none of them reaches the second part. Its volume is what the application wrote during the replay, which
is the volume the same projection would record if it were already live. A coalescing view buffers only after
`ReplayAware.replayStarted()`, which no subscription-fed composition ever calls, so on the path this matters for
nothing is buffered at all. And decision 7's clear is a precondition there exactly as it is everywhere else.

The recorder is therefore told where it is, rather than asking. `ReplayAwareSubscriptions` gains
`listenForCatchup(subscriptionId, listener)` on both stacks, and a model that has catch-ups sends that listener two
signals per catch-up. One when the catch-up begins, before it has delivered anything, and one when the history it
set out to read has been read. Between them the projection records nothing, and after the second, as when no
catch-up is running at all, it records.

Told rather than asked, because asking only ever produces samples. A recorder that read the model once per delivery
has to work out what happened between two of its own readings, and a catch-up that started and finished in between
looks like no catch-up at all, which is exactly what a poll misses whenever a history read matches nothing. Every
guard tried against that turned out to be one more thing to sample. The signals remove the question rather than
answering it.

Each signal names the catch-up that sent it, and that name is the catch-up itself, compared by identity and never
interpreted. A catch-up that has lost its subscription can still be running when its replacement starts, and its
boundary signal arriving late names the catch-up it belongs to, so a recorder the replacement has since started
ignores it and goes on reading the replacement's history as history.

Where each model sends the start matters as much as that it does. It goes where the model takes ownership of the
subscription id, inside the same lock the registration uses and before the thread or subscriber that produces the
deliveries exists, so nothing this catch-up delivers can precede it. Neither signal touches a store, since both run
on a thread the subscription needs back.

The boundary has to fall after the last history event has been applied and before the first of the others is, which
is not the same as when the source of history runs out. The reactor models send it from inside the replay pipeline
for that reason, since `concatMap` prefetches and a signal placed between the two halves upstream of it would fire
while a prefetch worth of history was still waiting to be applied. The push models send it at the buffer drain
rather than beside `replayCompleted()`, since a projection that was already caught up skips the replay entirely and
never reaches that call.

A model that cannot tell its catch-ups apart answers `false` from the default `listenForCatchup` and registers
nothing. A caller then falls back to polling `isCatchingUp(subscriptionId)` and driving the same two signals from
the edges of that reading, which is what the Spring registrars do through `PolledCatchupSignals`. That fallback is
worse in two ways worth stating rather than leaving to be rediscovered. The whole catch-up counts as history,
because the reading says nothing about where inside it the history ends, so an append written while one runs is
answered from the event store instead of from what the projection recorded. And a catch-up that starts and finishes
between two readings is not seen at all, so the projection records the history that catch-up replayed as though it
were live. Both are reasons for a model that can send the signals to send them.

On the pull paths, where `DomainEventFeed` and `CatchupProjectionFeed` drive the replay, the `ReplayAware` lifecycle
the handover engines already forward supplies both signals instead. `replayStarted()` is the start and
`replayCompleted()` is the boundary. So is `replayAbandoned()`, because a pull feed goes on delivering live events
to the same projection after a replay it cut short, and those events are applied and are recorded. A subscription
model sends nothing for a catch-up a stop truncated, because it delivers nothing more until a new one announces
itself.

An event the recorder cannot record is skipped with a debug log rather than throwing. Every event written before
this feature exists has no `appendid` extension, and neither does an event from a push feed unless whatever
produced it supplied one. A per-event throw would stop a rolling upgrade on the first pre-upgrade event and, on a
push feed, turn that into a redelivery loop, since
[ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) refuses an undeliverable push event
rather than acknowledging it.

There is no configuration-time refusal for a feed that can never supply the extension either, and the reasoning is
ADR 122's. Nothing at registration time can tell a feed that will never supply it from one that will, since a push
feed can be given events this library wrote or events it did not, and pre-0.34 history is indistinguishable from
post-0.34 history until an event actually arrives. A refusal that cannot fire precisely fires on every
configuration or on none, and ADR 122 already rejected building one on exactly that ground. The only refusals are
the two that can be decided at wiring time, no store bean and `mode = SYNCHRONOUS`.

### 7. A completed clear is a precondition for recording again

A projection clears its membership records before recording resumes after every catch-up it is told about.
`clear(projectionId)` deletes them, and until that delete has come back successfully the recorder records nothing.

That holds in the second part of a catch-up too, and there it needs one addition, because that part records. An
append applied while the clear is still owed cannot be written yet, since the delete that is owed would remove it
again, and it cannot be skipped either, since that part of the catch-up is the only delivery it gets. So it
waits, and is written once the clear lands. The wait is bounded at a thousand appends, and past that the oldest are
dropped with a warning. That bound reads like the delivery caches elsewhere in this library and behaves in the
opposite direction. An id evicted from a `BoundedIdCache` costs a second delivery, which the at-least-once contract
already allows, while an append dropped here is never recorded and a wait for it times out. The bound is high enough
that reaching it means a clear has been failing for a long time, which decision 7 already logs loudly.

Everything waiting is dropped whenever the next catch-up starts, rather than only when one ends. A stop parks a
catch-up and a start reads that history from the beginning, with nothing live in between, so a rule keyed on the end
of a catch-up would let a parked one's appends be written into a read model that is being rebuilt. That is the
untruth this decision exists to prevent, arriving by a different route.

The precondition is what makes the rule safe rather than best effort. A clear that exhausts its retries stops the
recorder and logs loudly, and never lets recording continue as though the clear had happened, because a transient
delete failure must not reinstate the exact untruth the rule exists to remove.

The catch-up's start signal is what marks the clear as owed, and it deliberately runs no store call itself. Two
things run it. A delivery does, and a scheduled poll does too, which is what a catch-up whose deliveries are all
filtered out server-side needs, since no delivery ever reaches the recorder there.

The poll ticks each registered recording projection on an exponential schedule, starting at 200 ms and settling at
5 seconds, and a projection goes back to the fast end whenever a tick has something to react to. For a projection
whose model sends the signals, a tick retries a clear that is still owed and reports whether one is, which is the
whole of what a poll can do there. For one behind a model that has to be polled instead, a tick also reads whether
a catch-up is running, drives the two signals from the edges of that reading, and reports either condition, so the
interval stays at 200 ms for the whole catch-up rather than seeing its end up to 5 seconds late.

That second case has a residual and the documentation states it rather than implying the rule is complete. A
catch-up that both starts and finishes inside one interval is not seen at all, so the records it should have
cleared survive it. At the settled interval that is a catch-up shorter than 5 seconds. A projection whose model
sends the signals has no such residual, because nothing about the catch-up is inferred from a reading.

There is a second window, on the read side, and it is worth stating separately because the clear is what removes
the records rather than what stops them being read. Recording stops as soon as an observation shows the projection
replaying, but the records stay readable until the clear finishes, so a wait running in between can be told `true`
about an append whose read model the rebuild is in the middle of discarding. On a rebuild that delivers events the
projection handles, that window runs from the replay starting to the first such delivery. On one that delivers
none, it runs to the next poll. So this rule makes the untrue answer a window rather than removing it, and the
guarantee in the consequences is written to say exactly that.

The poll has one owner and one lifecycle, because this repository has shipped four subscription lifecycle leaks
already. One scheduler is shared by every recording projection, retained by the registrar the way it already
retains what `close()` has to stop (`ProjectionAnnotationRegistrar.java:314-316`), with its thread named in the
`occurrent-*` convention and a disposed reactor `Scheduler` on the reactive stack. A test asserts that closing the
application context stops it, on both stacks.

The poll is deliberately a registrar-level service rather than part of the recorder. `dsl/projection-dsl` has no
place to hook a `close()` and this design does not add one, so the recording wrapper exposes the two signals and
the clear retry, and nothing more.

An application that composes its projections itself, outside Spring, is the one caller that has to wire this by
hand, and the factory's javadoc says so. It calls `listenForCatchup(projectionId, view)` on the model its
subscription runs on before subscribing, and it schedules `pollForClear()` so a clear that failed while a catch-up
ran is retried. A caller that does neither still records, and gets a projection that records the history of every
catch-up as though it were live, which is the defect this decision exists to prevent.

There is no clear at startup. An application that restarts with its checkpoint intact replays nothing and keeps its
records, which is correct because the read model is intact too, and both catch-up models classify a checkpoint that
is neither global nor time-based as live. An application that restarts with a wiped checkpoint replays, and either
the per-delivery check or the poll observes it, but only for a projection that declared a replaying start position
(`startAt = BEGINNING`, or a global position of at least zero). On Occurrent's own shipped composition, the default
start position with no global start position set bypasses the catch-up layer. Setting `startAtGlobalPosition` to
zero or above takes precedence over it and replays from that position. The checkpoint is never consulted there, so wiping it
changes nothing, and such a projection never replays and so never clears its own records on a rebuild. A custom
composition's own default behavior is its own to declare, not something this framework can verify, so the same
claim does not automatically extend to one.

The alternative was a per-run key, an epoch or incarnation value that would make a rebuilt projection read a
different set of records than its predecessor. It is the mechanism that could tell "this projection was rebuilt"
apart from "this instance restarted", which the rule above cannot, and that is worth stating plainly rather than
dismissing. It was rejected because nothing observable supplies the rebuild signal it needs, so the value has to be
managed by an operator, and the machinery is not repaid by avoiding timeouts that already fail in the safe
direction.

### 8. The owner of a composition says which model to listen to

Whoever composed a projection's subscription says which model the recorder registers with. It is never found by
asking the subscription bean alone, for the reason in the context above, that the reactive lookup cannot see a
catch-up model behind the durable wrapper and would leave the default reactive Mongo wiring listening to nothing.

On the blocking annotation path the registrar finds it with `ReplayAwareSubscriptions.findIn(...)`, whose wrapper
walk is sound there and is what `SagaAnnotationRegistrar` already relies on.

On the reactive annotation path it comes from whoever composed the model. The registrar has it directly where it
composed the model itself, which is what it already does for push projections. For the default asynchronous path
the composition is built by `OccurrentReactiveMongoAutoConfiguration`, not by the registrar, so the catch-up layer
that `composeCatchupLayer(...)` returns is handed to a `ComposedCatchupModel` bean the registrar reads.

The programmatic factories take no such argument. A recording view is built from a projection id and a store, and
whoever composed it registers it with the model afterwards, since only they know which model that is. A composition
with no catch-ups at all registers with nothing, which is the same thing as never being told about one.

For a blocking model that does have catch-ups, sending the signals is treated as part of the contract of being a
subscription model that replays. A third-party model that replays and answers `false` from `listenForCatchup`
puts its projections on the polled fallback, with the two costs decision 6 names. A subscription-TCK assertion
for it is a candidate this ADR notes and does not build.

### 9. A composition that never replays records, with no automatic clear

Some compositions have no replay to observe at all, and it matters that "no replay" and "cannot say" are treated
differently, because a design that refused both would refuse working applications.

`InMemorySubscriptionModel`, a durable-only model with no catch-up layer, and a push model with `catchup = NONE`
never replay history. Recording is allowed there and the check answers live, because there is no replay to record
through and no replay-driven rebuild to protect against.

What such a projection does not get is protection against having its read model wiped by hand, and this ADR states
that as outside the feature's contract rather than as a limitation to work around later. The reason differs per
composition and both are worth naming. For an in-memory model and a live-only push feed, nothing refills a wiped
read model at all, so the projection has no supported rebuild in the first place. For a durable-only model over
MongoDB, delivery continues from the checkpoint after a wipe, so the read model is partial and moving forward,
while the surviving membership records describe state the wipe destroyed.

The operator step in both cases is the store's own `clear(projectionId)`, which is public for this reason, and any
untrue answer in the meantime expires with the retention time in decision 11.

A third case belongs beside those two. A composition that can replay and can report its phase honestly, but is
wired so it is never asked to. `StartPosition.DEFAULT` with no global start position set and `NOW` both put an otherwise catch-up-capable
composition here, since neither ever asks it to replay. `isCatchingUp` answers false truthfully, not through a never-replays
fallback, because this projection's own configuration never invokes the replay the composition is capable of. The
consequence matches the other two regardless. Recording proceeds, no automatic clear ever engages, and the operator
step above is what closes it.

### 10. The identifier is recorded after each handled event

The recorder writes the membership record after the wrapped view's live update returns, once per handled event that
has the identifier. It does not wait until it has handled all of the append's events.

So a waiter can be told `true` when the projection has applied some but not all of the events it handles from that
append, and this is intended semantics rather than an accident, with a test asserting it. The delay before the rest
are applied has three different sizes, and the documentation states all three, because two of them are not what a
reader would assume. In the ordinary case it is the time the same node needs to work through the rest of that
append. If the node dies partway through, another node takes over when the lease expires, 20 seconds by default
(`MongoLeaseCompetingConsumerStrategySupport.java:53`). While the subscription is paused or stopped, it does not
end until someone starts it again.

Recording on the last event instead was considered and fails structurally. Occurrent pushes subscription filters
server-side, so a projection that does not handle the last event type of an append never sees that event, and the
wait would never finish even though the projection had already applied every event of the append it cares about.
Counting events towards a total fails the same way, since the count a filtered subscriber sees is not the count the
append wrote.

Skipping a repeated identifier inside the recorder is a free optimization, since one append usually delivers
several events and only the first needs a write.

### 11. Retention is storage housekeeping, and there is no event-time rule

A Mongo TTL index limits how much storage membership records take, and that is all it does. It is not a correctness
parameter. A wait for an identifier that has been evicted times out, which is the safe direction.

There is deliberately no rule based on the CloudEvent `time` attribute, which would be the obvious way to avoid
recording during a replay without asking about replays at all. Occurrent treats `time` as application-owned domain
time, it is optional, and an application that imports history legitimately writes events dated years ago. A design
that skipped old events would silently do nothing for those applications.

TTL sweeps run about once a minute, so eviction is late rather than early. That is harmless, since a late eviction
only means a true answer stays available for longer than the retention time promised.

### 12. Which compositions record

An asynchronous subscription with a catch-up layer records, with the replay check and the clear rule in full. A
push subscription with a catch-up layer behaves the same way.

`DomainEventFeed` and `CatchupProjectionFeed` record through the handover's `ReplayAware` lifecycle, and they
record whatever the application supplies through event metadata. An application that supplies no `appendid` gets
nothing recorded, and decision 6's skip rule makes that quiet rather than fatal.

A durable-only model, an in-memory model and a push model with `catchup = NONE` record with the check answering
live and no automatic clear, under decision 9.

The on-demand pull path has nothing to record, since it queries the store when asked rather than applying events as
they arrive. Synchronous mode is already read-your-writes because the projection updates inside the write, so
`mode = SYNCHRONOUS` and the opt-in in decision 13 are mutually exclusive and the registrar refuses the
combination.

### 13. `@Projection(recordAppliedAppends = true)` is the opt-in

Recording is opt-in through a new `@Projection` attribute, `recordAppliedAppends`, defaulting to `false`. The name
matches `AppliedAppendStore.recordApplied` and `hasApplied`, and it occupies the same position as
`recordAppliedPosition`, the attribute ADR 122 removed.

The registrar wraps the resolved view or update when the attribute is set, in the shape the withdrawn
`resolveStore` wrapper used, and refuses at startup when no `AppliedAppendStore` bean exists, since that
configuration cannot work and nothing at runtime would explain why.

The Spring Boot starters auto-configure a Mongo-backed store with `@ConditionalOnMissingBean`, so an application
that sets the attribute gets a working store without wiring one. Properties live under `occurrent.projection.*`,
covering the retention time, the wait's backoff, and the poll's schedule from decision 7.

Where a projection identifier and a subscription identifier can differ, the programmatic API takes both explicitly.
They are the same string by construction on the annotation path and on `ProjectionRunner.project(subscriptionId,
...)`, and taking both means the replay check is never asked about the wrong identifier when a caller chooses them
separately.

## Consequences

- Read-your-writes answers for appends a projection applied since its last reset. A restart with an intact
  checkpoint is not a reset, so ordinary restarts, failovers and scale-outs cost nothing. A reset only happens
  through a replay the projection's own configuration can actually perform. On Occurrent's own shipped composition
  the default start position with no global start position set never replays, so a projection left there never
  resets on a rebuild, and the operator
  step decision 9 prescribes is what a wiped or rebuilt read model needs instead. A custom composition's own default
  behavior is its own to declare, so a startup warning naming this only fires where that behavior is actually known.
- After a reset, waits for appends applied before it time out, because no replay writes a cleared record again and
  replay deliveries record nothing. The exception is a live redelivery of a pre-reset event, which records its
  identifier again like any other live event, so a broker that redelivers can bring one back. Any replay is a
  reset, including a deliberate `startAt`
  replay into a read model that would have tolerated one, because nothing observable tells that apart from a
  rebuild. This is a property of the design and the documentation states it beside the guarantee, not in a footnote.
- On a path where the projection can say whether it is replaying, and once any pending clear has finished, a `true`
  answer means the projection applied at least one event of that append after its last reset. That is weaker than
  having applied all of it, and decision 10 states the delay and why it is intended. An identifier that was never
  recorded, or was cleared, or has been evicted, all produce a timeout instead. A store that cannot be read keeps
  the wait polling until its timeout expires, which is true only because decision 5 limits each read to the time
  the wait has left.
- That guarantee depends on the clear having finished, so it is not true in the window before it does. A wait
  between a rebuild starting and its clear completing can be told `true` about an append whose read model is being
  discarded. Decision 7 states how long that window is in each case. This design narrows the untrue answer to that
  window instead of removing it, and the per-run key that would remove it was rejected there, with its cost.
- Two cases fall outside that, both from decision 9, and both expire with the retention time. A read model wiped by
  hand on a composition that never replays, and a read model wiped while the application keeps running with no
  restart and no replay anywhere.
- A projection behind a model that has to be polled rather than one that sends its catch-up signals keeps a residual
  that decision 7 states. A catch-up shorter than the poll interval is not seen at all, so the records it should
  have cleared survive it, and everything that catch-up delivered counts as history. Both stacks' shipped models
  send the signals.
- An append the projection applied during the second part of a catch-up is recorded, which closes the window
  [#890](https://github.com/johanhaleby/occurrent/issues/890) reported, where such an append could never be recorded
  by any path and a wait for it never finished. Two losses remain and are worth naming rather than leaving to be
  rediscovered. On the reactor stack a write whose position was reserved before the replay read the head, and that
  committed after it, can still be read by a history window, and the reactor replay puts every id it reads into the
  same cache its live delivery filters on, so no second delivery follows and that append is not recorded.
  [#891](https://github.com/johanhaleby/occurrent/issues/891) tracks it. The blocking stacks do not have it, because
  their history reads do not populate that cache and a second delivery follows. The other loss is the bound in
  decision 7, an append dropped from the wait while a clear keeps failing.
- Closing it needed no ordering rule and no position comparison, which is what made it closable at all. A delivery is
  classified by which read produced it, and every closure that classified by position ran into what ADR 122 refuted.
- Widening the two result records breaks external code two ways, and only one of them announces itself. An
  equality assertion against a whole record starts failing with no compile error, while a record deconstruction
  pattern stops compiling. The changelog entry and the migration-guide section are what tell the reader about the
  first.
- One membership write per handled live event per recording projection, reduced to one per append per projection by
  skipping a repeated identifier. This ADR orders no benchmark, and review reopens the question if it looks wrong
  in practice.
- The difference between the two stacks' capability lookups is worked around here rather than fixed.
  [#842](https://github.com/johanhaleby/occurrent/issues/842) tracks fixing it, and doing so would let the reactive
  registrar ask the model the way the blocking one does.
  [#746](https://github.com/johanhaleby/occurrent/issues/746) is the nearest relative and is not the same problem.
  It asked whether `ReplayAware` forwarding on materialized-view wrappers needed a typed replacement, and closed on
  2026-08-17 with a recorded no-change once the re-check found one forwarding wrapper left.
- SQL and broker stores are not built here, and the design ports to them unchanged.
  [#392](https://github.com/johanhaleby/occurrent/issues/392) owns building the JDBC store, and
  [#397](https://github.com/johanhaleby/occurrent/issues/397), which asks how a SQL event store assigns global
  positions and what a live subscription resumes from, is the one this ADR touches, since an append identifier
  works the same way whichever answer that question gets.
