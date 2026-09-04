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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.ReplayFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A one-time <strong>catch-up</strong> in front of a {@link PushSubscriptionModel}: on first subscribe it
 * replays a projection's history from the event store, then hands over to the live push feed, so a brand-new or rebuilt
 * projection is backfilled before it starts consuming the broker. It exists because a broker is not a log, so the push
 * feed alone cannot backfill a projection that started after events were already written.
 * <p>
 * Contract (the "broker owns live-resume" model, see ADR 62):
 * <ul>
 *   <li><strong>Catch-up</strong> is Occurrent's job and runs once per subscription id. On subscribe this model
 *       registers on the live feed first and buffers, replays the store {@code position}-ordered up to the head at read
 *       time via {@link PositionOrderedReader}, then drains the buffer and goes live. An event that commits during the
 *       replay is delivered either by the replay or by the buffered feed, and the overlap is de-duplicated by event id
 *       (not by a position watermark: Occurrent positions can commit late and have permanent gaps, so a watermark would
 *       drop a late-committing low-position event, see ADR 62). Because buffering starts before the head is read, no
 *       reconcile pass is needed.</li>
 *   <li><strong>Live resume</strong> is the broker's job, not Occurrent's. After catch-up, the listener consumes the
 *       broker and acknowledges each message only once {@code accept(...)} returns, so an unprocessed event is
 *       redelivered by the broker. This model persists no live position watermark. Delivery is therefore at-least-once,
 *       so the projection fold must be idempotent, the same contract as the change-stream path. The "acknowledge after
 *       processing" guarantee holds for the live phase. During the catch-up window {@code accept(...)} buffers the event
 *       and returns before it is folded (the calling thread is not blocked for the whole replay), so a message may be
 *       acknowledged before it is applied. That is safe because the catch-up-complete marker is written only after the
 *       drain, so a crash mid-catch-up re-replays the whole history from the store, which is the backstop for any
 *       event acknowledged but not yet folded.</li>
 *   <li>A one-shot <strong>catch-up-complete marker</strong> (an optional {@link CheckpointStorage}) records that the
 *       replay finished, so a restart skips it and lets the broker resume. The stored value marks completion, it is not
 *       a live resume position. Correctness across a restart then depends on the broker retaining the backlog for an
 *       offline consumer (a durable queue with a preserved offset). If the marker is lost or absent, the projection is
 *       caught up again.</li>
 * </ul>
 * Only stream and capability-agnostic subscription filters can be replayed (their plain {@link Filter} drives the
 * position-ordered read). A DCB subscription filter is rejected, since a DCB boundary needs a different replay read.
 * <p>
 * The catch-up-then-live coordination itself (the buffer, the de-dup cache, and the drain-then-mark ordering) is
 * delegated per-subscription to {@link BlockingHandover}, shared with {@code CatchupProjectionFeed}.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, ReplayAwareSubscriptions, HistoryRetainingSubscriptions {

    /**
     * Whether the store this model replays from holds {@code event}, asked of the store rather than assumed from the
     * fact that this model replays one. The reader and the live feed are independent: a bridge consuming another
     * service's events delivers what this store never had, while a feed carrying this application's own writes
     * delivers what it did, and nothing about this model's construction says which of the two it was given.
     * <p>
     * Reads a single position rather than scanning, when the event carries one. An event with no position is looked
     * up by id alone, which a store may answer with a scan, and the caller is expected to ask this rarely.
     * <p>
     * Fails closed. An event missing either half of its identity, and a read that throws, both answer {@code false},
     * because a caller uses this to decide whether it may stop retrying an event and a question that cannot be
     * answered has to cost the event nothing. A reader with no position needs no branch here, since the constructor
     * already refuses one.
     */
    @Override
    public boolean retains(CloudEvent event) {
        String id = event.getId();
        URI source = event.getSource();
        if (id == null || source == null) {
            return false;
        }
        long position = OccurrentCloudEventExtension.getPosition(event);
        PositionRange range = position > 0 ? PositionRange.between(position - 1, position) : PositionRange.fromBeginning();
        // Matched on source as well as id, since a CloudEvent is identified by the pair. An id alone would let a local
        // event from another source stand in for the one that arrived over the feed, which is the reading that loses it.
        try (Stream<CloudEvent> stored = reader.readInPositionOrder(Filter.cloudEvent(id, source), range)) {
            return stored.findAny().isPresent();
        } catch (RuntimeException e) {
            log.warn("Could not check whether the event '{}' is still in the event store, so it is treated as gone. The caller keeps retrying it rather than dropping it.", id, e);
            return false;
        }
    }

    // Named so subscribe(..) can build the action before taking the monitor and register it inside, rather than
    // spelling a multi-line lambda out in the middle of a synchronized block.
    private interface RoutingSubscribeAction extends RegisteringSubscribable.RoutingAction {
    }

    private static final Logger log = LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);

    // Long enough that a replay noticing the shutdown at its next event always makes it, short enough that a parked
    // fold cannot hold a closing context open. Matches how SagaSubscription bounds its own poller shutdown.
    private static final Duration SHUTDOWN_REPLAY_TIMEOUT = Duration.ofSeconds(5);

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;
    private final @Nullable CheckpointWriteVersionSource writeVersionSource;

    // Set by stop(), cleared by start(...). Read by the replay so stopping the model interrupts a replay in flight, not
    // just the live feed the replay has not handed over to yet.
    private volatile boolean stopped = false;
    // Whether the most recent start(..) asked for subscriptions to be resumed automatically. A replay that a stop
    // interrupted relaunches itself only when that answer is yes, since start(false) means the operator wants to
    // pick each subscription back up through resumeSubscription rather than have them all come back at once.
    private boolean startResumesSubscriptionsAutomatically = false;
    private volatile boolean shuttingDown = false;
    // Subscriptions whose replay is running. The live feed cannot answer for them: it knows the id (this model
    // registers there first) but it is buffering rather than delivering, so it would report a subscription that is
    // not yet folding anything as running.
    private final ConcurrentMap<String, Future<Boolean>> replayingSubscriptions = new ConcurrentHashMap<>();
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them. Kept
    // until this model shuts down, since the registration outlives any one catch-up: a stop and start, a resume, or
    // a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped being told
    // would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();
    // How to launch a subscription's replay again, kept only while there is a replay worth launching. Removed once one
    // finishes (nothing left to replay), once one fails (it is refusing, not stopped, and restarting it would turn a
    // loud refusal into a restart loop), and on cancel or shutdown. What is left is exactly the replays a stop
    // interrupted, which start(true) and resumeSubscription bring back. Without this a stop during a replay was
    // permanent, because the replay is the only thing that reaches the handover (ADR 104).
    private final ConcurrentMap<String, Supplier<Future<Boolean>>> interruptibleReplays = new ConcurrentHashMap<>();
    // The handover backing each subscription id currently registered here, so isReadyForLiveDelivery(String) can ask
    // the one component that actually owns the buffer rather than track readiness separately. Populated once, in
    // subscribe(), and kept for the id's whole lifetime (a stop-then-relaunch reuses the same handover), removed only
    // by cancelSubscription and shutdown.
    private final ConcurrentMap<String, BlockingHandover<CloudEvent>> handoversBySubscriptionId = new ConcurrentHashMap<>();
    // One lock per registered subscription id, so the marker write and the lifecycle calls that move the id are
    // one step against each other without the model monitor being held across a checkpoint store call.
    // Entries are created only by a registration and outlive the subscription, which is the same trade ADR 131
    // made, since a subscription id is application-defined and low-cardinality, so slow growth over a model's
    // lifetime is cheaper than reference counting a key space that does not need it.
    private final ConcurrentMap<String, ReentrantLock> markerLocks = new ConcurrentHashMap<>();
    // Runs just before a successful catch-up's guarded completion step. Exists so a test can stand there, which
    // nothing outside this model can.
    private volatile Runnable beforeCompletingCatchup = () -> {
    };
    // Runs between the live feed being asked whether it is running and being told to pause. Exists so a test can
    // put a stop exactly there, which nothing outside this model can.
    private volatile Runnable betweenPauseCheckAndPause = () -> {
    };
    // Runs inside a lifecycle call that found no marker lock for its id, before it moves the id. Exists so a test
    // can stand there, which nothing outside this model can.
    private volatile Runnable betweenMarkerLockLookupAndAction = () -> {
    };

    /**
     * @param reader          Reads the projection's history in position order for the catch-up replay.
     * @param liveFeed        The live push feed the listener drives with {@code accept(...)}.
     * @param catchupMarker Records that the one-time catch-up finished so a restart skips it, or {@code null} to
     *                        catch up on every subscribe.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * @param options De-dup cache size and live-buffer cap for the catch-up-to-live handover.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        this(reader, liveFeed, catchupMarker, options, null);
    }

    /**
     * @param catchupMarker      Records that the one-time catch-up finished so a restart skips it, or {@code null} to
     *                           catch up on every subscribe.
     * @param writeVersionSource Asked for a version before the one-shot marker write and every checkpoint write this
     *                           model makes. A version stamps the write {@code notOlderThan} it, an empty answer or
     *                           no source at all stamps it {@code any()}.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker,
                                            @Nullable CheckpointWriteVersionSource writeVersionSource) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults(), writeVersionSource);
    }

    /**
     * @param options            De-dup cache size and live-buffer cap for the catch-up-to-live handover.
     * @param writeVersionSource Asked for a version before the one-shot marker write and every checkpoint write this
     *                           model makes. A version stamps the write {@code notOlderThan} it, an empty answer or
     *                           no source at all stamps it {@code any()}.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker,
                                            CatchupThenLiveOptions options, @Nullable CheckpointWriteVersionSource writeVersionSource) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.writeVersionSource = writeVersionSource;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!startAt.isDefault()) {
            throw new UnsupportedStartAtException(startAt, HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }

        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        BlockingHandover<CloudEvent> handover = BlockingHandover.create(action, CloudEvent::getId, options, "subscription");
        // Register on the live feed first, so any event that commits during the replay is captured (buffered) and not
        // lost in the gap between the replay head and going live. Registers a delivery-reporting action rather than
        // a plain Consumer, so PushSubscriptionModel.accept(..) (the write path, bufferIfNotLive true) still buffers
        // exactly as before, while PushSubscriptionModel.acceptRedeliverable(..) (a broker path that can redeliver,
        // bufferIfNotLive false) refuses instead of buffering, reported as RoutingOutcome.DEFERRED. Both branches
        // wrap only a BlockingHandover.PreDispatchRefusalException as RoutingAction.Refusal, never every
        // IllegalStateException the call could throw, since the handler itself can throw one too (deliverOutsideLock
        // runs it inside this same try), and a handler that genuinely ran must report DELIVERED, not
        // NOT_DELIVERABLE, whatever it threw. Route (unobserved) and routeReportingMatch (observed) both unwrap
        // Refusal back to the original cause before it ever reaches a caller, so accept(..) and
        // acceptRedeliverable(..) both still throw the plain IllegalStateException a catch-up failure always has,
        // whether or not a PushObserver is configured.
        RoutingSubscribeAction routingAction = (cloudEvent, bufferIfNotLive) -> {
                    try {
                        return bufferIfNotLive ? handover.acceptReportingDelivery(cloudEvent) : handover.acceptIfLive(cloudEvent);
                    } catch (BlockingHandover.PreDispatchRefusalException e) {
                        if (!e.thrownBy(handover)) {
                            // A different handover refused, which this handler reached by calling into it. This
                            // registration ran, so its own outcome is DELIVERED and the exception propagates as
                            // any other handler failure would.
                            throw e;
                        }
                        throw new RegisteringSubscribable.RoutingAction.Refusal(e, handover.refusesPermanently());
                    }
                };

        // Kept rather than launched once, so a replay a stop interrupts can be launched again over the same handover.
        // The handover has to be the same one: it holds the live buffer and the de-dup cache, so a second one would
        // replay into a projection that had already seen part of the history.
        //
        // ownLaunch closes over itself through this reference for the same reason self does for the Future below:
        // launchReplay needs to name its own launcher to remove it from interruptibleReplays by identity, so a
        // cancelSubscription(id) immediately followed by a subscribe(id, ...) never lets the old replay's completion
        // evict the new subscription's launcher out from under it, silently leaving that subscription un-relaunchable
        // by a later stop() plus start(true).
        AtomicReference<Supplier<Future<Boolean>>> ownLaunch = new AtomicReference<>();
        Supplier<Future<Boolean>> launch = () -> launchReplay(subscriptionId, handover, replayFilter, ownLaunch);
        ownLaunch.set(launch);
        Future<Boolean> replay;
        // Held across the live-feed registration and everything this subscribe installs, so a cancelSubscription
        // running at the same time sees either all of it or none of it. Without it a cancel landing in the middle
        // left the handover, the launcher and the replay itself behind for a subscription that is already gone.
        // The replay thread this starts needs the monitor only at its own boundaries, and nothing here waits for
        // it, so starting it under the monitor cannot deadlock.
        synchronized (this) {
            liveFeed.subscribeCatchupThenPush(subscriptionId, filter, StartAt.subscriptionModelDefault(), routingAction);
            handoversBySubscriptionId.put(subscriptionId, handover);
            interruptibleReplays.put(subscriptionId, launch);
            replay = launch.get();
        }
        return new CatchingUpSubscription(subscriptionId, replay);
    }

    /**
     * Whether a live event fed to {@code subscriptionId}'s registration right now would actually reach the
     * projection, rather than only being buffered against a replay still in flight or not yet started. Delegates
     * straight to the {@link BlockingHandover} this subscription's catch-up owns. See
     * {@link BlockingHandover#isReadyForLiveDelivery()} for exactly what that answers and why. In short, {@code true}
     * only once the catch-up has reached live, {@code false} while replaying or buffering ahead of its own drain, and
     * {@code false} forever after a catch-up failure.
     * <p>
     * A CloudEvent-level broker bridge that feeds the live {@link PushSubscriptionModel} this model wraps is safe to
     * acknowledge from {@link org.occurrent.subscription.RoutingOutcome#DELIVERED} alone, with no call to this method
     * at all: {@link PushSubscriptionModel#acceptRedeliverable(io.cloudevents.CloudEvent)} already refuses, rather
     * than buffers, a message this catch-up would only have buffered, reported
     * {@link org.occurrent.subscription.RoutingOutcome#DEFERRED} so the bridge redelivers it instead of acknowledging.
     * This method exists only for a bridge that wants to pace itself, skipping a fetch it can predict would come
     * back {@code DEFERRED} rather than pulling the message off the broker and immediately handing it back. An
     * optional throughput optimization, never a correctness dependency: a bridge that never calls this still
     * acknowledges only on genuine delivery, just after a few more refuse-and-redeliver round trips than one that
     * does. {@code false} for a {@code subscriptionId} this model never subscribed, or already cancelled, the safe
     * answer for an id nothing here is tracking.
     *
     * @param subscriptionId The subscription to ask about.
     */
    public boolean isReadyForLiveDelivery(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        BlockingHandover<CloudEvent> handover = handoversBySubscriptionId.get(subscriptionId);
        return handover != null && handover.isReadyForLiveDelivery();
    }

    // Starts one replay for subscriptionId and returns its handle. Called by subscribe, and again by start(true) or
    // resumeSubscription for a replay that a stop interrupted.
    private Future<Boolean> launchReplay(String subscriptionId, BlockingHandover<CloudEvent> handover, Filter replayFilter,
                                          AtomicReference<Supplier<Future<Boolean>>> ownLaunch) {
        // The task needs to name itself to forget(), so the entry it removes is its own rather than whatever holds
        // the id by then. Without that, a cancel followed by a re-subscribe of the same id lets this replay keep
        // going against the new subscription's entry and then delete it, silently killing the new subscription.
        AtomicReference<Future<Boolean>> self = new AtomicReference<>();
        BlockingHandover.Source<CloudEvent> source = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.isAlreadyCaughtUp(subscriptionId);
            }

            @Override
            public Stream<CloudEvent> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning());
            }

            @Override
            public boolean keepReplaying() {
                return shouldKeepReplaying(subscriptionId, self.get());
            }

            @Override
            public void markCaughtUp() {
                markIfStillOwned(subscriptionId, self.get());
            }

            @Override
            public void historyDone() {
                // The replay itself is the episode, so a listener a later attempt for this id has since started
                // ignores this and no lock is needed to keep this attempt from speaking for that one.
                CatchupListener listener = catchupListeners.get(subscriptionId);
                if (listener != null) {
                    listener.historyRead(self.get());
                }
            }
        };

        FutureTask<Boolean> replay = new FutureTask<>(() -> {
            final boolean caughtUp;
            try {
                caughtUp = handover.catchUp(source);
            } catch (RuntimeException | Error e) {
                // The registration stays. The handover was registered before the replay and recorded this failure, so
                // every later live event is refused rather than acknowledged, and the broker keeps holding them
                // (ADR 104). Releasing it here used to be the point, and it was the wrong trade: it freed the id at
                // the cost of acknowledging every later event into a subscription that handled nothing. Recovery is
                // cancelSubscription(id), which frees both the id and the slot, followed by a fresh subscribe.
                // Only the replay entry is forgotten, so isCatchingUp(id) stops answering true for a replay that
                // ended, while isRunning(id) answers true for the registration that is now refusing.
                // Logged because under startupMode = BACKGROUND nobody waits, and the failure would otherwise reach
                // no one.
                log.error("Catch-up failed for subscription {}. Its registration on the live feed is kept and now "
                        + "refuses every event, so the source redelivers rather than losing them. Cancel the "
                        + "subscription and subscribe again once the cause is fixed.", subscriptionId, e);
                // Dropped before the replay entry, so a start(true) racing this never sees a launcher with no replay
                // running and relaunches a catch-up that failed. By identity, never by key alone, so this replay's
                // own completion can never evict a newer launcher a cancelSubscription(id) plus subscribe(id, ...)
                // already put there for a different attempt.
                interruptibleReplays.remove(subscriptionId, ownLaunch.get());
                forget(subscriptionId, self.get());
                throw e;
            }
            if (!caughtUp) {
                // Stopped rather than failed, so the handover is intact, nothing is marked, and the registration is
                // kept. The launcher is kept too, so start(true) replays the whole history again, which is the answer
                // CatchupProjectionFeed.stopCatchUp() already records (ADR 104). Live events in the meantime are
                // dropped rather than refused, per ADR 85: the operator stopped this, and the window closes at
                // start(). Forgetting the replay entry last is what makes "launcher present, nothing replaying" mean
                // stopped.
                // Under the monitor with the relaunch check, so a start(true) racing this either finds the entry
                // still here and leaves it alone, or finds it gone and relaunches. Without that the start could
                // see a replay still running, do nothing, and leave a launcher no one ever calls again.
                synchronized (CatchupThenPushSubscriptionModel.this) {
                    forget(subscriptionId, self.get());
                }
                // The lifecycle state is read where the replay is installed rather than here, because a stop() or a
                // start(false) taking the monitor in between would make an answer read here stale by the time it
                // was acted on.
                relaunchInterruptedReplay(subscriptionId, true);
                return false;
            }
            // By identity, for the same reason the failure path above is: this replay's own completion must never
            // evict a newer launcher a cancelSubscription(id) plus subscribe(id, ...) already put there. Forgetting
            // the entry and applying the pending pause are one guarded step with markCaughtUp's own check, so a
            // cancelSubscription(id) plus subscribe(id, ...) landing between the post-loop keepReplaying() check
            // inside catchUp(..) and here still finds this replay's ownership already gone and does neither.
            interruptibleReplays.remove(subscriptionId, ownLaunch.get());
            // Package-private and a no-op in production. Lets a test stand immediately before the guarded
            // completion step, which is where a lifecycle call racing it has to be ordered against it.
            beforeCompletingCatchup.run();
            completeIfStillOwned(subscriptionId, self.get(), () -> {
                // Through forget, so this replay's catch-up state goes with its registration. Removing only the
                // registration leaves the reconciliation marker behind, and a later replay for the same id would
                // then read its own history as if it were past it.
                forget(subscriptionId, self.get());
                applyPendingPauseIfAny(subscriptionId);
            });
            return true;
        });
        self.set(replay);
        // Registered before the thread starts, so isRunning(id) answers for it the moment subscribe returns rather than
        // whenever the replay thread happens to get scheduled. Synchronized with completeIfStillOwned's own check, so
        // a new registration here can never land in the middle of an old replay's late, id-scoped completion. The
        // per-attempt catch-up state is set inside the same guarded step, so this attempt starts in the history part
        // of its catch-up whatever the previous attempt for the same id left behind.
        synchronized (this) {
            // Created before the id is published and under this monitor, so every later lookup for this id finds
            // it and no write can be the thing that brings it into existence (ADR 131 does the same).
            markerLocks.computeIfAbsent(subscriptionId, id -> new ReentrantLock());
            // On this id's marker lock too, so a fresh attempt cannot take the id while the previous attempt's
            // marker write is still running. Monitor first and the lock second, matching cancelSubscription.
            whileHoldingMarkerLock(subscriptionId, () -> replayingSubscriptions.put(subscriptionId, replay));
            // Sent where the id is taken and before the replay below runs, so it always precedes anything this
            // attempt delivers. The replay itself is the episode, so a later attempt for the same id starts its own.
            CatchupListener startListener = catchupListeners.get(subscriptionId);
            if (startListener != null) {
                startListener.catchupStarted(replay);
            }
        }
        Thread.ofVirtual().name("occurrent-push-catchup-" + subscriptionId).start(replay);
        return replay;
    }

    // Relaunches the replay for subscriptionId if a stop interrupted it, and returns its handle, or null if there was
    // nothing to relaunch. A replay is restartable when its launcher survived and nothing is replaying under that id,
    // which is exactly the state a stop leaves behind.
    //
    // Synchronized because the check and the launch have to be one step. Two callers reaching this together (start and
    // resumeSubscription, or two starts) would otherwise both see nothing replaying and put two replays on one
    // handover, and the replay path folds every event without consulting the de-dup cache, so the history would be
    // applied twice. Lifecycle calls are rare enough that the lock costs nothing.
    private @Nullable Future<Boolean> relaunchInterruptedReplay(String subscriptionId) {
        return relaunchInterruptedReplay(subscriptionId, false);
    }

    // onlyWhenStartResumesSubscriptionsAutomatically is for a replay relaunching itself after a stop. It asks the
    // lifecycle state here, where the replay is installed, so a stop() or a start(false) cannot slip in between
    // the question and the answer being acted on. An explicit resumeSubscription passes false and is unaffected.
    private synchronized @Nullable Future<Boolean> relaunchInterruptedReplay(String subscriptionId,
                                                                             boolean onlyWhenStartResumesSubscriptionsAutomatically) {
        if (onlyWhenStartResumesSubscriptionsAutomatically
                && (stopped || shuttingDown || !startResumesSubscriptionsAutomatically)) {
            return null;
        }
        Supplier<Future<Boolean>> launch = interruptibleReplays.get(subscriptionId);
        if (launch == null || replayingSubscriptions.containsKey(subscriptionId)) {
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

    // Removes this replay's own entry, never one a later subscribe put there under the same id.
    private void forget(String subscriptionId, @Nullable Future<Boolean> replay) {
        if (replay != null) {
            replayingSubscriptions.remove(subscriptionId, replay);
        }
    }

    // The ownership check and the id-scoped write or side effect it guards have to be one step, or a stale replay's
    // own late completion can still act on an id a cancelSubscription(id) plus subscribe(id, ...) already gave to a
    // newer one, even though the per-payload and post-loop keepReplaying() checks inside catchUp(..) both passed.
    // Synchronized on the same monitor launchReplay's own replayingSubscriptions.put(..) and cancelSubscription's
    // replayingSubscriptions.remove(..) use, for the same reason relaunchInterruptedReplay already is: lifecycle
    // calls are rare enough that the lock costs nothing.
    private synchronized void completeIfStillOwned(String subscriptionId, @Nullable Future<Boolean> replay, Runnable completion) {
        if (replay != null && replayingSubscriptions.get(subscriptionId) == replay) {
            completion.run();
        }
    }

    // The ownership check and the marker write as one step, on this id's lock rather than on the model monitor.
    // Same atomicity, since cancelSubscription and subscribe take the same lock before they move the id. What the
    // model monitor no longer does is hold every other lifecycle call for as long as a checkpoint store takes to
    // answer, so stop, start, pause and resume get through while a write is in flight. A cancelSubscription for
    // the same id is the exception, since it waits for the write and holds the monitor while waiting, so anything
    // arriving behind such a cancel waits with it.
    //
    // A ReentrantLock rather than the monitor, because this runs on the replay's virtual thread and the write can
    // block on storage. Blocking inside synchronized holds the platform thread underneath for that whole span
    // (ADR 131). The lock is taken without the model monitor held, and every lifecycle call takes the monitor
    // first and this second, so the two are always acquired in that order.
    private void markIfStillOwned(String subscriptionId, @Nullable Future<Boolean> replay) {
        ReentrantLock lock = markerLocks.get(subscriptionId);
        if (lock == null) {
            // Only a registration creates one, and a replay cannot reach this without having been registered, so
            // there is nothing this attempt could be entitled to mark.
            return;
        }
        lock.lock();
        try {
            if (replay != null && !shuttingDown && !stopped && replayingSubscriptions.get(subscriptionId) == replay) {
                markCaughtUp(subscriptionId);
            }
        } finally {
            lock.unlock();
        }
    }

    // Held across a lifecycle call that moves an id, so it waits for a marker write already running for that id
    // rather than taking the id from under it. Takes a lock only when one exists and never creates one, so an id
    // this model has never registered adds nothing to the registry. A registration creates the lock before it
    // publishes the id, both under this monitor, so a missing lock here means the id was never registered and
    // there is nothing to wait for. Creating it lazily at the write instead would not be safe: a get can return
    // null while a computeIfAbsent for the same key is still in flight, and the two would then run unserialized.
    private void whileHoldingMarkerLock(String subscriptionId, Runnable action) {
        ReentrantLock lock = markerLocks.get(subscriptionId);
        if (lock == null) {
            // Package-private and a no-op in production. Lets a test stand where a lock does not exist yet, which
            // is the only moment a write could start behind a lifecycle call's back if the write did not take the
            // monitor to start.
            betweenMarkerLockLookupAndAction.run();
            action.run();
            return;
        }
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this
    // model's contract.
    void runBeforeCompletingCatchup(Runnable hook) {
        this.beforeCompletingCatchup = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes.
    void runBetweenMarkerLockLookupAndAction(Runnable hook) {
        this.betweenMarkerLockLookupAndAction = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes.
    void runBetweenPauseCheckAndPause(Runnable hook) {
        this.betweenPauseCheckAndPause = Objects.requireNonNull(hook, "hook cannot be null");
    }

    private boolean isAlreadyCaughtUp(String subscriptionId) {
        return catchupMarker != null && catchupMarker.exists(subscriptionId);
    }

    private void markCaughtUp(String subscriptionId) {
        if (catchupMarker != null) {
            // The stored position marks that the catch-up replay completed at this head, not a live resume watermark.
            catchupMarker.save(subscriptionId, GlobalCheckpoint.of(reader.currentPosition()), writeConditionFor(subscriptionId));
        }
    }

    // A version from writeVersionSource stamps notOlderThan. An empty answer or no source stamps any(). Always the
    // 3-arg save, never a choice between two.
    private CheckpointWriteCondition writeConditionFor(String subscriptionId) {
        if (writeVersionSource == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = writeVersionSource.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
    }

    /**
     * Whether the replay for {@code subscriptionId} should keep going: the model is neither shutting down nor stopped,
     * and the subscription has not been cancelled out from under it.
     */
    private boolean shouldKeepReplaying(String subscriptionId, @Nullable Future<Boolean> replay) {
        return !shuttingDown && !stopped && replay != null && replayingSubscriptions.get(subscriptionId) == replay;
    }

    // Checked against the live feed rather than applied blindly. A stop landing between the last replayed event and
    // here already paused everything, and pausing again throws, which would report a catch-up that actually finished
    // as a failure.
    private void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null && liveFeed.isRunning(subscriptionId)) {
            // Stands between the check and the call it guards, which is the only place a stop could get between
            // them. A no-op in production.
            betweenPauseCheckAndPause.run();
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    // --- Life cycle. The live feed owns delivery, so most of this is a fan-out; what this model adds is an answer for
    // the window where a replay is in flight, which the live feed cannot give because it is buffering rather than
    // delivering. ---

    /**
     * Stops the live feed and any catch-up replay still in flight. Reversible: a stopped replay keeps its registration
     * on the live feed and is replayed from the beginning by {@link #start(boolean)}, because a stop is not a failure.
     * That is the decision {@code CatchupProjectionFeed.stopCatchUp()} already records, ported here rather than
     * re-derived (ADR 104).
     * <p>
     * A replay this interrupts marks nothing, and neither does one that had already read its last event, since the
     * marker step asks whether this model is stopped before it writes. What a stop cannot do is call off a write
     * that has already begun, because that would mean waiting for a checkpoint store here. Such a marker stands,
     * and the subscription it belongs to is caught up rather than replayed again, since the attempt that wrote it
     * had read the whole history and held the id when the write began.
     * <p>
     * The events buffered during the replay are delivered before the marker is written, so a stop arriving while
     * they are being delivered is still early enough to be refused.
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
     * A stop is not a failure, so nothing was marked and the replay starts from the beginning rather than from a
     * cursor this model does not keep. Under {@code start(false)} the interrupted replays are left for
     * {@link #resumeSubscription(String)} to pick up one at a time, which is what "do not resume subscriptions
     * automatically" has to mean for a subscription whose catch-up is the thing that was stopped.
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

    /**
     * Whether {@code subscriptionId} is still replaying history and has not yet handed over to the live feed. Here
     * {@link #isRunning(String)} is {@code true} throughout the replay, matching what an event-store catch-up model
     * reports, which is why the handover needs an answer of its own.
     */
    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return replayingSubscriptions.containsKey(subscriptionId);
    }

    /**
     * Pauses {@code subscriptionId}, or records that a pause was asked for when its replay is still running, since
     * a replay does not go through the live feed and pausing there would report a subscription paused while its
     * history keeps being handled. A recorded pause is applied at the handover.
     */
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
        Future<Boolean> relaunched = relaunchInterruptedReplay(subscriptionId);
        if (relaunched != null) {
            // Its catch-up was interrupted by a stop, so resuming it means replaying the history again, since this
            // model keeps no replay cursor to resume from.
            pauseRequestedDuringReplay.remove(subscriptionId);
            return new CatchingUpSubscription(subscriptionId, relaunched);
        }
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
            // Paused and resumed while its replay was still running, so the live feed was never told and has nothing to
            // resume. Dropping the request is the whole of it, but hand back a handle that still tracks the replay
            // rather than one that claims to be started.
            Future<Boolean> replay = replayingSubscriptions.get(subscriptionId);
            if (replay != null) {
                return new CatchingUpSubscription(subscriptionId, replay);
            }
            // The replay finished between dropping the request and looking it up, so whether the handover managed to
            // apply the pause first is a race. Resume only if it actually landed, since the live feed refuses to
            // resume a subscription it never paused.
            return liveFeed.isPaused(subscriptionId)
                    ? liveFeed.resumeSubscription(subscriptionId)
                    : new CatchingUpSubscription(subscriptionId, CompletableFuture.completedFuture(true));
        }
        return liveFeed.resumeSubscription(subscriptionId);
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // On this id's marker lock as well as the monitor, so a cancel arriving while that id's marker write is
        // running waits for the write rather than taking the id from under it. Monitor first and the lock second,
        // the one order every caller uses.
        whileHoldingMarkerLock(subscriptionId, () -> {
            // Removing the replay entry is what stops a replay in flight, since shouldKeepReplaying reads this map.
            // All of it under the monitor, so a subscribe running at the same time installs everything or nothing.
            replayingSubscriptions.remove(subscriptionId);
            pauseRequestedDuringReplay.remove(subscriptionId);
            // A cancel is not a stop, so nothing is kept to launch again. This is also the recovery from a failed
            // catch-up, freeing the id and releasing the registration that was refusing (ADR 104).
            interruptibleReplays.remove(subscriptionId);
            handoversBySubscriptionId.remove(subscriptionId);
            liveFeed.cancelSubscription(subscriptionId);
        });
    }

    /**
     * Stops every replay still in flight and waits for them to unwind before shutting the live feed down.
     * <p>
     * The waiting is the point. A replay runs on its own thread, so without it a context that is closing would leave
     * one folding into a store that is closing with it, surfacing as an error from a thread nobody owns. A replay
     * notices the shutdown at its next event, so the wait is normally brief. It gives up after five seconds anyway,
     * because the fold is application code and may never return.
     */
    @Override
    public void shutdown() {
        shuttingDown = true;
        awaitReplays(SHUTDOWN_REPLAY_TIMEOUT);
        replayingSubscriptions.clear();
        catchupListeners.clear();
        pauseRequestedDuringReplay.clear();
        // Unlike stop(), a shutdown keeps nothing to launch again: it drops the registrations too.
        interruptibleReplays.clear();
        handoversBySubscriptionId.clear();
        // markerLocks is deliberately not cleared. A replay that outlived the wait above may still hold one, and
        // dropping the map would not release it, it would only hand the next caller for that id a different lock
        // and lose the exclusion the write is relying on.
        liveFeed.shutdown();
    }

    private void awaitReplays(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Future<Boolean> replay : replayingSubscriptions.values()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                replay.get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (TimeoutException e) {
                return;
            } catch (ExecutionException e) {
                // Already reported to whoever waited on this subscription, and a shutdown has nowhere useful to put it.
            }
        }
    }

    /**
     * A subscription whose catch-up is running on its own thread. {@code waitUntilStarted} is the only thing that joins
     * it, which is what lets a caller choose to keep the replay off the startup path.
     * <p>
     * It tracks the one replay it was created for. A replay that {@link #stop()} interrupted answers {@code false}
     * here and keeps answering {@code false} after {@link #start(boolean)} launches a fresh one, since this handle
     * cannot see it. Ask {@link #isCatchingUp(String)} or {@link #isRunning(String)} about a restarted replay, or take
     * the handle {@link #resumeSubscription(String)} hands back.
     */
    private record CatchingUpSubscription(String id, Future<Boolean> replay) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
            try {
                // false when the replay was stopped rather than finished: not started, but not a failure either.
                return replay.get(safeTimeout.timeout(), safeTimeout.timeUnit());
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                // Rethrown rather than reported as false, unlike the event-store catch-up's handle. A projection's
                // runner discards this return value, so swallowing a replay failure would start an application whose
                // read model is silently empty.
                switch (e.getCause()) {
                    case RuntimeException cause -> throw cause;
                    case Error cause -> throw cause;
                    case null, default -> throw new IllegalStateException("The catch-up for subscription '" + id + "' failed", e.getCause());
                }
            }
        }
    }
}
