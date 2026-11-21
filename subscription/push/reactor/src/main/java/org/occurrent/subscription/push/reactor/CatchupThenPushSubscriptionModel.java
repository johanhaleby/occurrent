/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.internal.ReactiveHandover;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.internal.CatchupMarkers;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.ReplayFilters;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The reactive counterpart of the blocking {@code CatchupThenPushSubscriptionModel}: a <strong>catch-up</strong> in
 * front of a reactive {@link PushSubscriptionModel}. Each time a subscription starts, it replays from the event store
 * the events after the stored catch-up marker, or the whole history when none is stored, then hands over to the live
 * push feed, so a projection is backfilled before it consumes the broker. After a failed delete of the marker, a
 * subscribe of the id in this process deletes it again and replays the whole history without reading it, until a
 * delete or a marker write of this model succeeds.
 * <p>
 * The catch-up applies the replayed events first, then the events a replayed handler wrote, and only then can store
 * the catch-up marker. A live event fed through {@code accept(..)} during the replay waits in a bounded unicast
 * sink until after that, unless the sink is full, and the overlap is de-duplicated by the CloudEvent id and source
 * together. A replay stopped before going live drops the events fed from outside that the sink held, and keeps the
 * events a replayed handler wrote, as described below.
 * <p>
 * Contract (see ADR 62 and the blocking model): catch-up is Occurrent's job and live resume is the broker's. An
 * optional {@link CheckpointStorage} catch-up marker lets a later catch-up for the id resume after a position an
 * earlier catch-up stored, and the broker delivers the live events. Correctness across a restart then depends on the
 * broker retaining the backlog for an offline consumer. With no marker stored, the catch-up replays from the
 * beginning, and a marker older than it should be costs a longer replay and no event.
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md">ADR 179</a>'s
 * "What a reactor catch-up start replays" says what a start replays.
 * The blocking model reads a stored marker as a finished catch-up and skips the replay instead. The marker is keyed
 * by the subscription id alone, so a subscribe that changes the filter or the handler uses a new subscription id.
 * Under a new id the subscription starts from the beginning of the history and repeats everything its handler does, the
 * records it writes, the commands it dispatches and the messages it sends. Stop every instance on the old id before the
 * first one on the new id starts. Stopping them doesn't prevent the repeats. With more than one instance, what else a
 * new id needs depends on where each instance keeps what it builds and which broker queue or consumer group it
 * consumes, as
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md#changing-a-checkpoint-or-marker-that-several-instances-share">the
 * upgrade guide</a> describes.
 * {@link #cancelSubscription(String)} deletes the marker, and says when subscribing the same id again resumes instead
 * of replaying from the beginning. No live position is persisted, so
 * resuming the live feed is the job of whatever feeds the {@link PushSubscriptionModel}. Only stream and
 * capability-agnostic subscription filters can be replayed.
 * <p>
 * Feeding {@link PushSubscriptionModel#accept(Iterable)} from an event store's write path is supported only for the
 * in-memory event store, and Occurrent ships no reactive {@link PositionOrderedReader} over that store for this model
 * to replay. A write that reaches this model through {@code accept(..)} while its replay runs fails at once when the
 * live sink is full, and otherwise waits until the event has been applied after the replay, or until the replay
 * fails or is stopped. A handler that writes an event the subscription's filter accepts does not wait for it. This
 * model hands the subscription's events to the handler one at a time, so that event cannot be applied before the
 * handler returns. The write returns once the event is queued, and the handler gets the events it wrote in the
 * order it wrote them, once it has returned. The model recognizes the write when the handler returns it as part of
 * the {@link Mono} it returns, or subscribes it, blocking or not, on the thread it was called on. A write made on
 * that thread and subscribed elsewhere counts too, while the event the handler was called for has not been answered,
 * or for a replayed event, until the catch-up has applied the events its replay wrote.
 * A handler that blocks on the write from a thread it switched to still waits for itself. A write the handler's code
 * makes on another thread after the handler has returned is a write from outside, answered to whatever subscribed
 * it. {@link PushSubscriptionModel#acceptOrDefer(CloudEvent)} called from the handler, recognized the same way, is
 * refused for that call only, live or not, and reported as
 * {@link org.occurrent.subscription.RoutingOutcome#NOT_DELIVERABLE}, since it could only report the event as queued
 * rather than applied. The event the handler was called for is not failed with it.
 * <p>
 * An event a handler writes and feeds back through {@code accept(..)} on its own thread is an early copy with no broker
 * copy behind it, and it needs a durable path as well. Either the application's forwarder publishes it to the broker
 * that feeds this model, or the handler writes and feeds it again whenever the event that triggered it is delivered
 * again, also when the write is already stored. Replicas of a subscription share its catch-up marker, and another
 * replica's catch-up can store a marker at or above an event this replica fed back and has not yet handled. Without
 * either path, a subscription that runs on more than one replica can lose such an event, and ADR 179's "Fed-back
 * events on the reactor stack" lists when. An event that reaches this model from the broker has that copy behind it.
 * <p>
 * The events a handler wrote, and those their handlers wrote in turn, are answered for by whatever waits for the
 * event the handler was called for.
 * <ul>
 *     <li>For an event fed through {@code accept(..)} or {@code acceptOrDefer(..)}, the returned {@link Mono}
 *     completes only once the written events have been applied. {@code accept(..)} errors with the first failure
 *     or refusal among them, and {@code acceptOrDefer(..)} reports it as
 *     {@link PushSubscriptionModel#acceptOrDefer(CloudEvent)} describes. An event written more than
 *     {@link CatchupThenLiveOptions#maxBufferedEvents()} levels below the event fed from outside, by a handler
 *     handling an event that was itself written, and so on, is refused.</li>
 *     <li>For a replayed event, the written events are applied after the rest of the history and before the
 *     catch-up marker. When one of them fails or is refused, even when the handler swallowed the error, the rest
 *     are still applied, and then the catch-up fails, so {@code waitUntilStarted()} errors and no marker is
 *     written. A replay that is stopped or fails before that hands the written events to the live feed. After a
 *     stop they are applied once the subscription is live, with no further replay when it already was, and after a
 *     failure before the subscription fails for good.</li>
 *     <li>For an event nothing waits for, such as one a stopped replay left to the live feed, a failure to apply
 *     it starts the subscription failing.</li>
 * </ul>
 * <p>
 * A failing subscription deletes its catch-up marker, refuses every later event that does not come from its handler,
 * applies the events it has already taken in and those its handler writes meanwhile, and then fails for good. A
 * failed catch-up starts it failing the same way, and also refuses each event from anywhere else that is still
 * waiting, and does not apply it. When deleting the marker still fails after 3 retries, the subscription logs an
 * error naming the subscription id. A cancel deletes the marker again, and when that delete fails too, subscribing
 * again in this process deletes it once more and replays from the beginning. After a restart a catch-up that reads a
 * marker the store still holds resumes after it and does not replay a failed event at or below it. Deleting the marker
 * by hand is best effort, as a cancel's delete is, so only a subscription under a new id is sure to replay from the
 * beginning, see the "Cancel" section of
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md">ADR 179</a>.
 * Under a new id the subscription starts from the beginning of the history and repeats everything its handler does, the
 * records it writes, the commands it dispatches and the messages it sends. Stop every instance on the old id before the
 * first one on the new id starts. Stopping them doesn't prevent the repeats. With more than one instance, what else a
 * new id needs depends on where each instance keeps what it builds and which broker queue or consumer group it
 * consumes, as
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md#changing-a-checkpoint-or-marker-that-several-instances-share">the
 * upgrade guide</a> describes.
 * A written event that no replay can bring back and nothing waits for is lost when applying it fails, or when the
 * process stops before it is applied.
 * <p>
 * Nothing records which live events the subscription has handled, and a crash before the handler has run
 * loses the event from the in-memory event store too, so after a crash the store never holds an event the
 * subscription missed. With a durable event store, such as MongoDB, use a durable subscription, or a broker as
 * described below. The amendment to ADR 133 records why.
 * <p>
 * Fed from a broker, call {@link PushSubscriptionModel#acceptOrDefer(CloudEvent)} and acknowledge the message
 * only when its {@link Mono} completes with {@link org.occurrent.subscription.RoutingOutcome#DELIVERED} or
 * {@link org.occurrent.subscription.RoutingOutcome#FILTERED}. It refuses an event arriving during the replay instead of
 * buffering it, so the listener has the broker deliver that event again, as
 * {@link org.occurrent.subscription.RoutingOutcome.Disposition#HOLD} describes, and a delivery after this model has
 * gone live applies it.
 * Once the catch-up has failed, the {@link Mono} completes with
 * {@link org.occurrent.subscription.RoutingOutcome#REFUSED} for every event, and the listener stops.
 * Delivery is at-least-once, so
 * a handler that sees the same event twice has to leave the same state as seeing it once. Do not acknowledge on {@code accept(..)} completing, since it also completes for
 * an event nothing applied, one fed before any subscription is registered say.
 * <p>
 * The catch-up-then-live coordination itself (the bounded live sink, the de-dup cache, and the
 * replay-then-marker-then-live pipeline shape) is delegated per-subscription to {@link ReactiveHandover}, shared with
 * {@code CatchupProjectionFeed}. The replay runs on {@code boundedElastic} rather than on the thread that called
 * {@link #subscribe}, so {@code waitUntilStarted()} is the only thing that joins it.
 * <p>
 * It is a {@link SubscriptionModel} but not a {@code FluxSubscriptionModel}: the latter is the bare
 * {@code Flux}-returning primitive, which a model fed by a push source rather than by reading a change stream cannot
 * honour. Most of the life cycle is a fan-out to the live feed. What this model adds is an answer for the window where
 * a replay is in flight, which the live feed cannot give because it is buffering rather than delivering.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, ReplayAwareSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);

    // Long enough that a replay noticing the shutdown at its next event always makes it, short enough that a parked
    // fold cannot hold a closing context open. Matches the blocking twin.
    private static final Duration SHUTDOWN_REPLAY_TIMEOUT = Duration.ofSeconds(5);

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;

    // Set by stop(), cleared by start(...). Read by the replay so stopping the model interrupts a replay in flight,
    // not just the live feed the replay has not handed over to yet.
    private volatile boolean stopped = false;
    // Whether the most recent start(..) asked for subscriptions to be resumed automatically. A replay that a stop
    // interrupted relaunches itself only when that answer is yes, since start(false) means the operator wants to
    // pick each subscription back up through resumeSubscription rather than have them all come back at once.
    private boolean startResumesSubscriptionsAutomatically = false;
    private volatile boolean shuttingDown = false;
    // Subscriptions whose replay is running, each mapped to the signal that fires when it finishes, is stopped, or
    // fails. The live feed cannot answer for them: it knows the id (this model registers there first) but it is
    // buffering rather than delivering, so it would report a subscription that is not yet folding anything as running.
    private final ConcurrentMap<String, Sinks.One<Boolean>> replayingSubscriptions = new ConcurrentHashMap<>();
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them. Kept
    // until this model shuts down, since the registration outlives any one catch-up: a stop and start, a resume, or
    // a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped being told
    // would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // The episode each id's listener heard catchupStarted for and not yet historyRead. The episode of a stopped or
    // failed attempt stays here, and the next attempt of the id ends it before it replays, whether it announces an
    // episode of its own or resumes after a marker and announces nothing. Otherwise the listener would stay reading
    // history for good, or hear a second start with no end for the first. A shutdown drops it unended, since the
    // model delivers nothing after it and a historyRead would claim a history was read that was not.
    private final ConcurrentMap<String, Sinks.One<Boolean>> episodesReadingHistory = new ConcurrentHashMap<>();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();
    // How to launch a subscription's replay again, kept only while there is a replay worth launching, matching the
    // blocking twin. Removed once one finishes, once one fails (it is refusing, not stopped), and on cancel or
    // shutdown. What is left is exactly the replays a stop interrupted, which start(true) and resumeSubscription
    // bring back (ADR 104).
    private final ConcurrentMap<String, Supplier<Sinks.One<Boolean>>> interruptibleReplays = new ConcurrentHashMap<>();
    // Which attempt owns each id's catch-up. Separate from replayingSubscriptions because that entry is released at
    // the drain, by design, so that isCatchingUp stays true while the payloads buffered during the history read are
    // delivered. Ownership has to outlive it: for an empty buffer the engine signals liveDrained before the
    // catch-up completes, and for a non-empty one after, so the completion below would be guarded by an entry that
    // is sometimes already gone. Written only under this model's monitor, or removed by identity.
    private final ConcurrentMap<String, Sinks.One<Boolean>> catchupOwners = new ConcurrentHashMap<>();
    // The last marker write or delete for each id, until it ends. The next one starts after it and a marker read
    // waits for it, so a cancel's delete comes after a write already running, and a subscribe after a cancel reads the
    // marker only once the cancel's delete has ended. Written only under this model's monitor, or removed by identity.
    private final ConcurrentMap<String, Mono<Void>> markerOperations = new ConcurrentHashMap<>();
    // Ids whose last marker operation is a delete that has not succeeded. Nothing vouches for such a marker any more,
    // so a catch-up replays the history instead of resuming after it, and deletes it again first. Replaying costs
    // only duplicates, and resuming can skip an event whose delivery failed. Cleared by a delete or a marker write that
    // succeeds.
    private final Set<String> markersNotYetDeleted = ConcurrentHashMap.newKeySet();
    // Runs at the start of a successful catch-up's completion, where the replaying entry is already released and
    // the launcher is not yet. Exists so a test can stand in that window, which nothing else can reach.
    private volatile Runnable beforeCompletingCatchup = () -> {
    };
    // Runs between the live feed being asked whether it is running and being told to pause. Exists so a test can
    // put a stop exactly there, which nothing outside this model can.
    private volatile Runnable betweenPauseCheckAndPause = () -> {
    };

    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!startAt.isDefault()) {
            throw new UnsupportedStartAtException(startAt, HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }
        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        ReactiveHandover<CloudEvent, CloudEventKey> handover = ReactiveHandover.create(action, CloudEventKey::of, options, "subscription");

        // Register on the live feed first, so events committing during the replay are buffered in the sink, not
        // lost. PushSubscriptionModel.accept(..), the write path, buffers through acceptReportingDelivery(..).
        // PushSubscriptionModel.acceptOrDefer(..), a broker path that can redeliver, uses acceptIfLive(..)
        // instead, which refuses a payload arriving before the handover is live rather than buffering it, reported
        // as RoutingOutcome.DEFERRED. Only the dedicated pre-dispatch exception is wrapped as a Refusal, so
        // routeReportingMatch reports NOT_DELIVERABLE or REFUSED for it, whichever handover.refusesPermanently()
        // decides, and DELIVERED for a handler's own exception, the same one-evaluation fix the blocking stack
        // already has.
        RegisteringSubscribable.RoutingAction routingAction = (cloudEvent, bufferIfNotLive) ->
                (bufferIfNotLive ? handover.acceptReportingDelivery(cloudEvent) : handover.acceptIfLive(cloudEvent))
                .onErrorMap(ReactiveHandover.PreDispatchRefusalException.class, e -> e.thrownBy(handover)
                        ? new RegisteringSubscribable.RoutingAction.Refusal(e, handover.refusesPermanently())
                        // A different handover refused, which this handler reached by calling into it. This
                        // registration ran, so its own outcome is DELIVERED and the error propagates as any other
                        // handler failure would.
                        : e);
        // Kept rather than launched once, so a replay a stop interrupts can be launched again over the same handover.
        // The handover has to be the same one: it holds the live sink and the de-dup cache, so a second one would
        // replay into a projection that had already seen part of the history.
        //
        // ownLaunch closes over itself through this reference so launchReplay can name its own launcher and remove
        // it by identity, which keeps a finishing replay from evicting the launcher a cancelSubscription(id) plus
        // subscribe(id, ..) already put there for a different attempt.
        AtomicReference<Supplier<Sinks.One<Boolean>>> ownLaunch = new AtomicReference<>();
        Supplier<Sinks.One<Boolean>> launch = () -> launchReplay(subscriptionId, handover, replayFilter, ownLaunch);
        ownLaunch.set(launch);
        Sinks.One<Boolean> replay;
        // Held across the live-feed registration and everything this subscribe installs, so a cancelSubscription
        // running at the same time sees either all of it or none of it. Without it a cancel landing in the middle
        // left the handover, the launcher and the replay itself behind for a subscription that is already gone.
        synchronized (this) {
            liveFeed.subscribeCatchupThenPush(subscriptionId, filter, StartAt.subscriptionModelDefault(), routingAction);
            interruptibleReplays.put(subscriptionId, launch);
            replay = launch.get();
        }
        return new CatchingUpSubscription(subscriptionId, replay);
    }

    // Starts one replay for subscriptionId and returns the signal that fires when it finishes, is stopped, or fails.
    // Called by subscribe, and again by start(true) or resumeSubscription for a replay that a stop interrupted.
    //
    // Relaunching is safe despite the handover's unicast live sink: it is subscribed only after the marker phase, and
    // a stop errors the pipeline before that, so an interrupted replay left it untouched. A replay that finished is
    // never relaunched, which is the case that would fail.
    private Sinks.One<Boolean> launchReplay(String subscriptionId, ReactiveHandover<CloudEvent, CloudEventKey> handover, Filter replayFilter,
                                            AtomicReference<Supplier<Sinks.One<Boolean>>> ownLaunch) {
        // Registered before the replay is handed to the handover, which subscribes it on boundedElastic straight away.
        // isRunning(id) and keepReplaying() therefore answer for this subscription from the moment subscribe returns,
        // rather than from whenever that pipeline happens to get scheduled.
        Sinks.One<Boolean> replayDone = Sinks.one();
        // Both maps in one step under the monitor cancelSubscription also takes, so a cancel and a fresh subscribe
        // cannot slip between them.
        synchronized (this) {
            replayingSubscriptions.put(subscriptionId, replayDone);
            catchupOwners.put(subscriptionId, replayDone);
        }

        // Set in isAlreadyCaughtUp(), which the handover calls before replay() and markCaughtUp() (ADR 179)
        AtomicReference<ReplayStart> attemptStart = new AtomicReference<>(ReplayStart.FROM_BEGINNING_UNMARKED);
        // Whether the listener hears this attempt, set in replayStarted() from whether it announced the attempt, which
        // follows the same read the replay uses. A listener registered after that hears nothing about the attempt,
        // since it never heard catchupStarted for it. The handover can read the marker again on a catch-up's turn, and
        // replayStarted() runs once after the last read, so catchupStarted goes out at most once and only for a replay
        // from the beginning. An attempt whose replay never starts is not heard. An attempt that resumes after a marker
        // is not heard either. Every attempt whose replay starts ends an episode an earlier attempt left reading
        // history.
        AtomicBoolean listenerHearsAttempt = new AtomicBoolean();
        // Whether this attempt's last marker read refused a checkpoint that is not a catch-up marker. Something else
        // stored it under the id, such as a durable subscription's resume token, so a failed catch-up does not delete
        // it. Kept per attempt, so a refusal that an earlier attempt or a cancel saw cannot spare a marker this
        // attempt wrote.
        AtomicBoolean storedIsNotAMarker = new AtomicBoolean();
        Mono<Boolean> catchupDone = handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                // Reads where the replay starts and the head markCaughtUp() would store, see ADR 179's "What a reactor
                // catch-up start replays". Every attempt replays, possibly nothing.
                return readReplayStart(subscriptionId, storedIsNotAMarker)
                        .doOnNext(attemptStart::set)
                        .thenReturn(false);
            }

            @Override
            public void replayStarted() {
                if (attemptStart.get().fromBeginning()) {
                    listenerHearsAttempt.set(announceCatchupStart(subscriptionId, replayDone));
                } else {
                    endEpisodeStillReadingHistory(subscriptionId, replayDone);
                }
            }

            @Override
            public Flux<CloudEvent> replay() {
                return reader.readInPositionOrder(replayFilter, attemptStart.get().range());
            }

            @Override
            public boolean keepReplaying() {
                return shouldKeepReplaying(subscriptionId, replayDone);
            }

            @Override
            public Mono<Void> markCaughtUp() {
                // Asked here rather than upstream, so an attempt that lost the id, or a model that was stopped,
                // between the last replayed event and this step writes nothing at all.
                return Mono.defer(() -> markCaughtUpIfStillOwned(subscriptionId, replayDone, attemptStart.get().head()));
            }

            @Override
            public Mono<Void> forgetCaughtUp() {
                // Names the id, since the handover logs this when every attempt failed, and an operator needs to know
                // which marker can still be stored.
                // Queued behind the last marker operation for the id, as a cancel's delete is, so a marker write
                // still running cannot put the marker back after it
                // Does not delete a checkpoint the last read refused, since it is not this model's to delete.
                CheckpointStorage marker = catchupMarker;
                return marker == null ? Mono.empty() : Mono.defer(() -> storedIsNotAMarker.get() ? Mono.<Void>empty() : deleteMarker(subscriptionId, marker))
                        .onErrorMap(error -> new IllegalStateException("Could not delete the catch-up marker of "
                                + "subscription " + subscriptionId + ".", error));
            }

            @Override
            public void liveDrained() {
                // Kept registered until here rather than dropped when the catch-up reports done, because the payloads
                // buffered while the history was read are delivered after that and each of them exactly once. A
                // recording projection has to see them as part of this catch-up, not as live delivery.
                forget(subscriptionId, replayDone);
            }

            @Override
            public void historyDone() {
                endEpisodeAtEndOfHistory(subscriptionId, replayDone);
            }

            @Override
            public Mono<Void> alreadyDeliveredByReplay(CloudEvent event) {
                // fromRunnable rather than a plain call, so a listener that throws errors this Mono instead of the
                // pipeline that was about to acknowledge the payload. On boundedElastic
                // because a recording listener writes to a store, and a payload emitted into the live sink is
                // delivered inline on whichever thread emitted it, which for a broker source is one that must not
                // block.
                return Mono.<Void>fromRunnable(() -> {
                    CatchupListener listener = catchupListeners.get(subscriptionId);
                    if (listener != null && listenerHearsAttempt.get()) {
                        listener.alreadyDeliveredByReplay(event);
                    }
                }).subscribeOn(Schedulers.boundedElastic());
            }
        });

        // Subscribed here rather than only handed back, so a caller that never waits still gets the bookkeeping below.
        // Logged because under startupMode = BACKGROUND nobody waits, and a failure would otherwise reach no one.
        catchupDone.subscribe(
                caughtUp -> {
                    if (caughtUp) {
                        // Package-private and a no-op in production. The window this opens, between the handover
                        // releasing the replaying entry at the drain and the launcher being dropped just below, is
                        // inline and microseconds long, so a test cannot reach it from the outside.
                        beforeCompletingCatchup.run();
                        // Guarded and by identity, so a completion arriving after a cancelSubscription(id) plus a
                        // fresh subscribe(id, ..) cannot evict the replacement's launcher or apply a pause meant
                        // for it. Not forgotten here, liveDrained does that once the payloads buffered during the
                        // history read have been delivered.
                        completeIfStillOwned(subscriptionId, replayDone, () -> {
                            interruptibleReplays.remove(subscriptionId, ownLaunch.get());
                            applyPendingPauseIfAny(subscriptionId);
                        });
                        catchupOwners.remove(subscriptionId, replayDone);
                    } else {
                        // Stopped rather than failed, so the handover is intact, nothing is marked, and both the
                        // registration and the launcher are kept for start(true), the answer
                        // CatchupProjectionFeed.stopCatchUp() already records (ADR 104). Forgetting the replay
                        // entry last is what makes "launcher present, nothing replaying" mean stopped.
                        // Under the monitor with the relaunch check, so a start(true) racing this either finds
                        // the entry still here and leaves it alone, or finds it gone and relaunches. Without that
                        // the start could see a replay still running, do nothing, and leave a launcher no one ever
                        // calls again.
                        synchronized (this) {
                            forget(subscriptionId, replayDone);
                            catchupOwners.remove(subscriptionId, replayDone);
                        }
                        // The lifecycle state is read where the replay is installed rather than here, because a
                        // stop() or a start(false) taking the monitor in between would make an answer read here
                        // stale by the time it was acted on.
                        relaunchInterruptedReplay(subscriptionId, true);
                    }
                    replayDone.tryEmitValue(caughtUp);
                },
                error -> {
                    // The registration stays, matching the blocking twin. The handover recorded this failure and is
                    // itself the registered action, so every later live event is refused rather than acknowledged and
                    // the broker keeps holding them (ADR 104). Recovery is cancelSubscription(id) plus a fresh
                    // subscribe. Dropped before the replay entry, so a start(true) racing this never sees a launcher
                    // with no replay running and relaunches a catch-up that failed.
                    // A cancel deletes a checkpoint that is not a marker, and another component may still use it, so
                    // this log asks for a cancel only when nothing else does
                    if (CatchupMarkers.refusedANonMarker(error)) {
                        log.error("Catch-up failed for subscription {}, because the checkpoint stored under its id is not "
                                + "a catch-up marker. Its registration on the live feed is kept and now refuses every "
                                + "event, so the source redelivers rather than losing them. Replace the subscription with "
                                + "one under a new id. Cancelling it deletes the checkpoint stored under its id, so cancel "
                                + "it only when nothing else uses that checkpoint. "
                                + SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT + " "
                                + SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT, subscriptionId, error);
                    } else {
                        log.error("Catch-up failed for subscription {}. Its registration on the live feed is kept and now "
                                + "refuses every event, so the source redelivers rather than losing them. Cancel the "
                                + "subscription and subscribe again once the cause is fixed. "
                                + SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT, subscriptionId, error);
                    }
                    interruptibleReplays.remove(subscriptionId, ownLaunch.get());
                    forget(subscriptionId, replayDone);
                    catchupOwners.remove(subscriptionId, replayDone);
                    replayDone.tryEmitError(error);
                });

        return replayDone;
    }

    // Relaunches the replay for subscriptionId if a stop interrupted it, and returns its signal, or null if there was
    // nothing to relaunch. A replay is restartable when its launcher survived and nothing is replaying under that id,
    // which is exactly the state a stop leaves behind.
    //
    // Synchronized because the check and the launch have to be one step. Two callers reaching this together (start and
    // resumeSubscription, or two starts) would otherwise both see nothing replaying and put two replays on one
    // handover, and the replay phase folds every event without consulting the de-dup cache, so the history would be
    // applied twice. Lifecycle calls are rare enough that the lock costs nothing.
    private Sinks.@Nullable One<Boolean> relaunchInterruptedReplay(String subscriptionId) {
        return relaunchInterruptedReplay(subscriptionId, false);
    }

    // onlyWhenStartResumesSubscriptionsAutomatically is for a replay relaunching itself after a stop. It asks the
    // lifecycle state here, where the replay is installed, so a stop() or a start(false) cannot slip in between
    // the question and the answer being acted on. An explicit resumeSubscription passes false and is unaffected.
    private synchronized Sinks.@Nullable One<Boolean> relaunchInterruptedReplay(String subscriptionId,
                                                                                boolean onlyWhenStartResumesSubscriptionsAutomatically) {
        if (onlyWhenStartResumesSubscriptionsAutomatically
                && (stopped || shuttingDown || !startResumesSubscriptionsAutomatically)) {
            return null;
        }
        Supplier<Sinks.One<Boolean>> launch = interruptibleReplays.get(subscriptionId);
        // catchupOwners, not replayingSubscriptions. The latter is released at the drain, before this model drops
        // the launcher, so a resume landing in that window would read "launcher present, nothing replaying", take
        // it to mean stopped, and replay again over a handover that has already gone live.
        if (launch == null || catchupOwners.containsKey(subscriptionId)) {
            return null;
        }
        // Unpaused here rather than by the caller, so a caller that loses the race above does not leave the
        // subscription unpaused and then try to resume it a second time, which the live feed refuses. A no-op under
        // start(true), which cleared every pause before calling this.
        if (liveFeed.isPaused(subscriptionId)) {
            liveFeed.resumeSubscription(subscriptionId);
        }
        return launch.get();
    }

    private void forget(String subscriptionId, Sinks.One<Boolean> replay) {
        replayingSubscriptions.remove(subscriptionId, replay);
    }

    /**
     * Whether the replay for {@code subscriptionId} should keep going: the model is neither shutting down nor stopped,
     * and the subscription has not been cancelled out from under it.
     */
    private boolean shouldKeepReplaying(String subscriptionId, Sinks.One<Boolean> replay) {
        return !shuttingDown && !stopped && catchupOwners.get(subscriptionId) == replay;
    }

    // The ownership check and whatever it guards have to be one step, or a stale attempt's own late completion can
    // still act on an id a cancelSubscription(id) plus subscribe(id, ..) already gave to a newer one. Synchronized
    // on the same monitor launchReplay's puts and cancelSubscription's removes use.
    private synchronized void completeIfStillOwned(String subscriptionId, Sinks.One<Boolean> replay, Runnable completion) {
        if (catchupOwners.get(subscriptionId) == replay) {
            completion.run();
        }
    }

    // Whether this attempt may still write the marker. Asked once more here even though the replay loop asks
    // before every event, because that check never runs after the last one, so a stop or a cancel arriving while
    // the final event was being handled would otherwise reach the write unnoticed. The blocking twin re-asks in
    // the same place and for the same reason, in BlockingHandover.
    //
    // A sample, not a hold. The monitor is released when this returns, so a cancel can still take the id away
    // while the write this answer allowed is running, and that is fine, since the attempt had replayed the events up
    // to the head it stores and held the id when the write began, which is all a marker claims.
    //
    // Taken on the same monitor as the puts in launchReplay, the removes in cancelSubscription and the flag stop()
    // sets, so the answer is never read part way through one of those. The write itself stays outside the monitor,
    // because a checkpoint store is someone else's code and can take as long as it likes while every lifecycle
    // call on this model takes the same monitor.
    private synchronized boolean mayStillMarkCaughtUp(String subscriptionId, Sinks.One<Boolean> replay) {
        return !shuttingDown && !stopped && catchupOwners.get(subscriptionId) == replay;
    }

    // The ownership check and queueing the write as one step under the monitor cancelSubscription takes, so a cancel
    // either comes first and nothing is written, or comes after and its delete runs once the write ends
    private synchronized Mono<Void> markCaughtUpIfStillOwned(String subscriptionId, Sinks.One<Boolean> replay, long head) {
        return mayStillMarkCaughtUp(subscriptionId, replay) ? afterTheLastMarkerOperation(subscriptionId, markCaughtUp(subscriptionId, head)) : Mono.empty();
    }

    private synchronized Mono<Void> afterTheLastMarkerOperation(String subscriptionId, Mono<Void> operation) {
        Mono<Void> previous = markerOperations.getOrDefault(subscriptionId, Mono.empty());
        AtomicReference<Mono<Void>> self = new AtomicReference<>();
        Mono<Void> next = previous.onErrorResume(__ -> Mono.empty())
                .then(operation)
                .doFinally(__ -> markerOperations.remove(subscriptionId, self.get()))
                .cache();
        self.set(next);
        markerOperations.put(subscriptionId, next);
        return next;
    }

    // Checked against the live feed rather than applied blindly. A stop landing between the last replayed event and
    // here already paused everything, and pausing again throws, which would report a catch-up that actually
    // finished as a failure.
    private void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null && liveFeed.isRunning(subscriptionId)) {
            // Stands between the check and the call it guards, which is the only place a stop could get between
            // them. A no-op in production.
            betweenPauseCheckAndPause.run();
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    // --- Life cycle. Mirrors the blocking twin: the live feed owns delivery, so most of this is a fan-out, and what
    // this model adds is an answer for the window where a replay is in flight. ---

    /**
     * Stops the live feed and any catch-up replay still in flight. Reversible: a stopped replay keeps its registration
     * on the live feed and is replayed again by {@link #start(boolean)}, because a stop is not a failure.
     * That is the decision {@code CatchupProjectionFeed.stopCatchUp()} already records, ported here rather than
     * re-derived (ADR 104).
     * <p>
     * A replay this interrupts marks nothing, and neither does one that had already read its last event, since the
     * marker step asks whether this model is stopped before it writes. What a stop cannot do is call off a write
     * that has already begun, because that would mean waiting for a checkpoint store here. That write can still store
     * its marker, which is safe since the attempt that wrote it had replayed the events up to the head it stores and
     * held the id when the write began.
     * <p>
     * The marker is written before the events buffered during the replay are delivered, so a stop arriving while
     * they are being delivered finds it already written. The blocking twin writes it after that delivery instead
     * and refuses a stop arriving there, which is a difference between the two rather than a promise either makes.
     * <p>
     * Live events fed while stopped are dropped rather than refused, the dropped-not-deferred contract every stopped
     * subscription model has (ADR 85). That is bounded here only because the stop is reversible: the window closes at
     * {@code start(..)}.
     */
    @Override
    public synchronized void stop() {
        stopped = true;
        liveFeed.stop();
    }

    /**
     * Starts the live feed and, when {@code resumeSubscriptionsAutomatically}, replays the history again for every
     * subscription whose catch-up {@link #stop()} interrupted.
     * <p>
     * A stop is not a failure, so each interrupted replay runs again. ADR 179's "What a reactor catch-up start
     * replays" says where it starts. Under {@code start(false)} the interrupted replays are left for
     * {@link #resumeSubscription(String)} to pick up one at a time.
     */
    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        startResumesSubscriptionsAutomatically = resumeSubscriptionsAutomatically;
        // Before the replays, so the registrations they hand over to are unpaused by the time one finishes.
        liveFeed.start(resumeSubscriptionsAutomatically);
        if (resumeSubscriptionsAutomatically) {
            // Skips an id already replaying, so a start() while a replay is in flight does not put a second replay on
            // the same handover.
            interruptibleReplays.keySet().forEach(this::relaunchInterruptedReplay);
        }
    }

    @Override
    public boolean isRunning() {
        return !replayingSubscriptions.isEmpty() || liveFeed.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return replayingSubscriptions.containsKey(subscriptionId) || liveFeed.isRunning(subscriptionId);
    }

    /**
     * The listener hears a catch-up only when it replays from the beginning. A catch-up that resumes after a stored
     * marker delivers its events the way the live feed does and sends nothing about itself, so a recording projection
     * keeps what it recorded before the restart (ADR 179). Before the next catch-up of the id replays, whether from the
     * beginning or after a marker, the listener hears {@code historyRead} for a catch-up it heard start and that a
     * stop, a cancel or a failure cut short. A catch-up a shutdown cuts short gets no {@code historyRead}. The
     * listener hears nothing about a catch-up that started before it was registered. The blocking push model still
     * tells its listener about every start.
     * <p>
     * This model calls {@code catchupStarted} and {@code historyRead} while it holds its own lock, so a listener that
     * blocks holds up {@code subscribe}, {@code cancelSubscription}, {@code stop}, {@code start},
     * {@code pauseSubscription}, {@code resumeSubscription} and the catch-up of every other id on this model, and a
     * listener that waits for another thread calling one of those methods never returns.
     */
    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    /**
     * Whether {@code subscriptionId} is still replaying history and has not yet handed over to the live feed. Here
     * {@link #isRunning(String)} is {@code true} throughout the replay, matching what an event-store catch-up model
     * reports, which is why the handover needs an answer of its own.
     */
    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return replayingSubscriptions.containsKey(subscriptionId);
    }

    /**
     * Whether {@code subscriptionId} is paused, counting a pause asked for while its replay was still running and
     * not yet applied to the live feed.
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringReplay.containsKey(subscriptionId) || liveFeed.isPaused(subscriptionId);
    }

    @Override
    public Set<String> subscriptionIds() {
        // A subscription is registered on the live feed before its replay is recorded, and the replay is only forgotten
        // when the live feed either keeps the registration or loses it too, so the live feed knows every id this model
        // knows. That is why this does not also read replayingSubscriptions, unlike isRunning.
        return liveFeed.subscriptionIds();
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (replayingSubscriptions.containsKey(subscriptionId)) {
            // The live feed would accept the pause, but the replay does not go through it, so pausing there now would
            // report the subscription paused while its history keeps folding. Record it and apply it at the handover.
            pauseRequestedDuringReplay.put(subscriptionId, true);
        } else {
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Sinks.One<Boolean> relaunched = relaunchInterruptedReplay(subscriptionId);
        if (relaunched != null) {
            // Its catch-up was interrupted by a stop, so resuming it means replaying the history again, since this
            // model keeps no replay cursor to resume from.
            pauseRequestedDuringReplay.remove(subscriptionId);
            return new CatchingUpSubscription(subscriptionId, relaunched);
        }
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
            // Paused and resumed while its replay was still running, so the live feed was never told and has nothing
            // to resume. Dropping the request is the whole of it, but hand back a handle that still tracks the replay
            // rather than one that claims to be started.
            Sinks.One<Boolean> replay = replayingSubscriptions.get(subscriptionId);
            if (replay != null) {
                return new CatchingUpSubscription(subscriptionId, replay);
            }
            // The replay finished between dropping the request and looking it up, so whether the handover managed to
            // apply the pause first is a race. Resume only if it actually landed, since the live feed refuses to
            // resume a subscription it never paused.
            return liveFeed.isPaused(subscriptionId)
                    ? liveFeed.resumeSubscription(subscriptionId)
                    : new CatchingUpSubscription(subscriptionId, finished());
        }
        return liveFeed.resumeSubscription(subscriptionId);
    }

    /**
     * Cancels the subscription and deletes its catch-up marker from the {@link CheckpointStorage}, the same clean-up a
     * cancel does for a durable subscription's checkpoint. The returned {@link Mono} completes once the marker is
     * deleted and fails when deleting it fails.
     * <p>
     * Subscribing the same id again replays from the beginning when the delete reached the store and no marker write
     * reached it later. A delete the store loses in a failover, or a marker write the store applies after the delete,
     * makes that subscribe resume after the position stored then instead. A delete that fails makes a subscribe in
     * this process delete the marker again and replay from the beginning, and a subscribe after a restart resume after
     * the old marker if the store still holds it. To rebuild the projection for certain, subscribe under a new
     * subscription id.
     * Under a new id the subscription starts from the beginning of the history and repeats everything its handler does,
     * the records it writes, the commands it dispatches and the messages it sends. Stop every instance on the old id
     * before the first one on the new id starts. Stopping them doesn't prevent the repeats. With more than one
     * instance, what else a new id needs depends on where each instance keeps what it builds and which broker queue or
     * consumer group it consumes, as
     * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md#changing-a-checkpoint-or-marker-that-several-instances-share">the
     * upgrade guide</a> describes.
     * <p>
     * The cancel happens when you call this, whether or not anything subscribes to the returned {@code Mono}. When the
     * delete fails, the subscription stays cancelled and the marker stays stored. It deletes the marker for an id this
     * model has never subscribed to.
     * <p>
     * Without a {@link CheckpointStorage} there is no marker, and the {@code Mono} completes at once.
     *
     * @param subscriptionId The id of the subscription to cancel.
     * @return A {@code Mono} that completes once the catch-up marker is deleted.
     */
    @Override
    public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // Dropping the ownership entry is what stops a replay in flight, since shouldKeepReplaying reads it. All of
        // it under the monitor, so a subscribe running at the same time installs everything or nothing.
        catchupOwners.remove(subscriptionId);
        replayingSubscriptions.remove(subscriptionId);
        pauseRequestedDuringReplay.remove(subscriptionId);
        // A cancel is not a stop, so nothing is kept to launch again. This is also the recovery from a failed
        // catch-up, freeing the id and releasing the registration that was refusing (ADR 104).
        interruptibleReplays.remove(subscriptionId);
        Mono<Void> liveFeedCancelled = liveFeed.cancelSubscription(subscriptionId);
        if (catchupMarker == null) {
            return liveFeedCancelled;
        }
        // Started here rather than by whoever subscribes, so the delete runs even when nobody waits for it. Cached,
        // so a caller that waits does not delete a second time. A subscribe after the cancel reads the marker only
        // once the delete ends.
        Mono<Void> delete = deleteMarker(subscriptionId, catchupMarker);
        delete.subscribe(unused -> {
        }, error -> log.warn("Could not delete the catch-up marker of cancelled subscription {}. Subscribing it again in this process replays its history, but after a restart it resumes after the marker if the store still holds it, unless the subscription is cancelled again first.", subscriptionId, error));
        return Mono.when(liveFeedCancelled, delete).cache();
    }

    /**
     * Stops every replay still in flight and waits for them to unwind before shutting the live feed down.
     * <p>
     * The waiting is the point. A replay runs on a scheduler thread, so without it a context that is closing would
     * leave one folding into a store that is closing with it, surfacing as an error from a thread nobody owns. A
     * replay notices the shutdown at its next event, so the wait is normally brief. It gives up after five seconds
     * anyway, because the fold is application code and may never return.
     */
    @Override
    public void shutdown() {
        shuttingDown = true;
        awaitReplays(SHUTDOWN_REPLAY_TIMEOUT);
        replayingSubscriptions.clear();
        catchupOwners.clear();
        catchupListeners.clear();
        episodesReadingHistory.clear();
        pauseRequestedDuringReplay.clear();
        // Unlike stop(), a shutdown keeps nothing to launch again: it drops the registrations too.
        interruptibleReplays.clear();
        liveFeed.shutdown();
    }

    private void awaitReplays(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Sinks.One<Boolean> replay : replayingSubscriptions.values()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                // toFuture().get rather than block(), which Reactor rejects outright on a non-blocking scheduler
                // thread. That rejection is a RuntimeException, so it would be swallowed below and skip the wait
                // entirely, which is the one thing this method exists to do.
                replay.asMono().toFuture().get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                // A failed replay was already reported to whoever waited on this subscription, and a shutdown has
                // nowhere useful to put it, so keep waiting for the rest. A timeout lands here too, and the deadline
                // check above ends the loop on the next iteration rather than this one.
            }
        }
    }

    // Package-private for the test that stands in the window described on the field. Not public, and not part of
    // this model's contract.
    void runBeforeCompletingCatchup(Runnable hook) {
        this.beforeCompletingCatchup = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes.
    void runBetweenPauseCheckAndPause(Runnable hook) {
        this.betweenPauseCheckAndPause = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Tells the listener that a replay from the beginning starts, which a recorder takes as the view being rebuilt.
    // A replay after a stored position is plain delivery, so it sends nothing, or the recorder would drop the records
    // of appends the view still holds (ADR 179). Under the monitor cancelSubscription also takes, so a cancel and a
    // fresh subscribe cannot slip between the ownership check and the signal. An attempt that no longer owns the id
    // sends nothing, or its start would arrive after its replacement's and the recorder would adopt a catch-up that is
    // already over. The replay itself is the episode, so a later attempt starts its own.
    // Answers whether a listener heard the start.
    private synchronized boolean announceCatchupStart(String subscriptionId, Sinks.One<Boolean> replayDone) {
        if (catchupOwners.get(subscriptionId) != replayDone) {
            return false;
        }
        endUnfinishedEpisode(subscriptionId);
        CatchupListener startListener = catchupListeners.get(subscriptionId);
        if (startListener == null) {
            return false;
        }
        episodesReadingHistory.put(subscriptionId, replayDone);
        startListener.catchupStarted(replayDone);
        return true;
    }

    // Under the same monitor and owner check as announceCatchupStart, so an attempt that has lost the id cannot end the
    // episode of the attempt that replaced it.
    private synchronized void endEpisodeStillReadingHistory(String subscriptionId, Sinks.One<Boolean> replayDone) {
        if (catchupOwners.get(subscriptionId) != replayDone) {
            return;
        }
        endUnfinishedEpisode(subscriptionId);
    }

    // Sent only by whoever removes the episode, so a replay whose episode a later attempt ended first sends no
    // historyRead of its own. Under the monitor announceCatchupStart and endEpisodeStillReadingHistory take, so the
    // next attempt of the id cannot announce or start its replay between the remove and the historyRead. No owner
    // check, since a cancelled replay that still reaches the end of its history ends the episode it opened.
    private synchronized void endEpisodeAtEndOfHistory(String subscriptionId, Sinks.One<Boolean> replayDone) {
        if (episodesReadingHistory.remove(subscriptionId, replayDone)) {
            CatchupListener listener = catchupListeners.get(subscriptionId);
            if (listener != null) {
                listener.historyRead(replayDone);
            }
        }
    }

    // Called only under this model's monitor, by an attempt that owns the id
    private void endUnfinishedEpisode(String subscriptionId) {
        Sinks.One<Boolean> unfinished = episodesReadingHistory.remove(subscriptionId);
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (unfinished != null && listener != null) {
            listener.historyRead(unfinished);
        }
    }

    // Waits for the last marker operation for the id, so a subscribe after a cancel reads the marker after the
    // cancel's delete. The head is read after the live feed was registered, which ADR 179 relies on. A marker whose
    // delete has not succeeded reads as absent, after the delete is tried again. One that fails once more keeps the
    // marker untrusted, and the history is replayed anyway.
    private Mono<ReplayStart> readReplayStart(String subscriptionId, AtomicBoolean storedIsNotAMarker) {
        CheckpointStorage marker = catchupMarker;
        if (marker == null) {
            return Mono.just(ReplayStart.FROM_BEGINNING_UNMARKED);
        }
        return Mono.defer(() -> markerOperations.getOrDefault(subscriptionId, Mono.empty()))
                .onErrorResume(__ -> Mono.empty())
                .then(Mono.defer(() -> markersNotYetDeleted.contains(subscriptionId)
                        ? deleteMarker(subscriptionId, marker)
                        .onErrorResume(error -> {
                            log.warn("Could not delete the catch-up marker of subscription {} again, so its history is replayed without deleting it", subscriptionId, error);
                            return Mono.empty();
                        })
                        .then(Mono.just(Optional.<Long>empty()))
                        : marker.read(subscriptionId)
                        .map(stored -> Optional.of(storedPosition(subscriptionId, stored, storedIsNotAMarker)))
                        .defaultIfEmpty(Optional.empty())))
                .flatMap(storedPosition -> reader.currentPosition()
                        .map(head -> new ReplayStart(storedPosition.orElse(null), head)));
    }

    private static long storedPosition(String subscriptionId, Checkpoint stored, AtomicBoolean storedIsNotAMarker) {
        try {
            return CatchupMarkers.positionOf(subscriptionId, stored);
        } catch (CatchupMarkers.NotACatchupMarkerException notAMarker) {
            storedIsNotAMarker.set(true);
            throw notAMarker;
        }
    }

    // Queued behind the last marker operation for the id. The marker counts as not deleted from the moment the delete
    // starts until it succeeds.
    private Mono<Void> deleteMarker(String subscriptionId, CheckpointStorage marker) {
        return afterTheLastMarkerOperation(subscriptionId, Mono.defer(() -> {
            markersNotYetDeleted.add(subscriptionId);
            return marker.delete(subscriptionId).then(Mono.fromRunnable(() -> markersNotYetDeleted.remove(subscriptionId)));
        }));
    }

    // Stores the head read before this attempt's replay, not the head now (ADR 179)
    private Mono<Void> markCaughtUp(String subscriptionId, long head) {
        CheckpointStorage marker = catchupMarker;
        return marker == null ? Mono.empty() : marker.save(subscriptionId, GlobalCheckpoint.of(head))
                .then(Mono.fromRunnable(() -> markersNotYetDeleted.remove(subscriptionId)));
    }

    // The stored position a catch-up resumes after, null when there is none, and the head read before its replay
    private record ReplayStart(@Nullable Long storedPosition, long head) {
        static final ReplayStart FROM_BEGINNING_UNMARKED = new ReplayStart(null, -1);

        PositionRange range() {
            return storedPosition == null ? PositionRange.fromBeginning() : PositionRange.afterPosition(storedPosition);
        }

        boolean fromBeginning() {
            return storedPosition == null;
        }
    }

    /**
     * A subscription whose catch-up is running on a scheduler thread. {@code waitUntilStarted} is the only thing that
     * joins it, which is what lets a caller choose to keep the replay off the startup path. It completes when the
     * replay finished, and the events its handler wrote during the replay have been applied, or when it was stopped.
     * It errors when it failed, and when one of those written events failed or was refused: a projection's runner
     * discards the value, so swallowing a replay failure would start an application whose read model is silently
     * empty.
     * <p>
     * It tracks the one replay it was created for. A replay that {@link #stop()} interrupted completes here and stays
     * completed after {@link #start(boolean)} launches a fresh one, since this handle cannot see it. Ask
     * {@link #isRunning(String)} about a restarted replay, or take the handle {@link #resumeSubscription(String)}
     * hands back.
     */
    private record CatchingUpSubscription(String id, Sinks.One<Boolean> replay) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return replay.asMono().then();
        }
    }

    // A signal that is already done, for a handle handed back when there is no longer a replay to track. The blocking
    // twin does the same with CompletableFuture.completedFuture(true) rather than a second Subscription type.
    private static Sinks.One<Boolean> finished() {
        Sinks.One<Boolean> done = Sinks.one();
        done.tryEmitValue(true);
        return done;
    }

    // A CloudEvent is identified by its id and source together, so the same id from two producers is two events.
    // Null for an event with no id, which the handover refuses.
    private record CloudEventKey(String id, URI source) {
        private static @Nullable CloudEventKey of(CloudEvent event) {
            String id = event.getId();
            return id == null ? null : new CloudEventKey(id, event.getSource());
        }
    }
}
