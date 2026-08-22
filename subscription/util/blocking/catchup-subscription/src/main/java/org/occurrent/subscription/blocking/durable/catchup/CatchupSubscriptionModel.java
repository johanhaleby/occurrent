/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A {@link SubscriptionModel} that can read historic cloud events from the all event streams (see {@link EventStoreQueries#all()}) until caught up with the
 * {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} of the {@code subscription} (you probably want to narrow the historic set events of events
 * by using a {@link org.occurrent.filter.Filter} when subscribing). It'll automatically switch over to the wrapped {@code subscription model} when all history events are read and the subscription has caught-up.
 * <br><b>Important:</b>&nbsp;The subscription model will only stream historic events if started with a {@link TimeBasedCheckpoint}, by default (i.e. if {@code StartAt.subscriptionModelDefault() is used}),
 * it'll NOT replay historic events, but instead delegate to the wrapped subscription model. Thus, to start the {@link CatchupSubscriptionModel} and make it replay historic events you can start it like this:
 * <pre>
 * var subscriptionModel = new CatchupSubscriptionModel(..);
 * // All examples below are equivalent:
 * subscriptionModel.subscribeFromBeginningOfTime("subscriptionId", e -> System.out.println("Event: " + e);
 * subscriptionModel.subscribe("subscriptionId", StartAtTime.beginningOfTime(), e -> System.out.println("Event: " + e);
 * subscriptionModel.subscribe("subscriptionId", StartAt.checkpoint(TimeBasedSubscription.beginningOfTime()), e -> System.out.println("Event: " + e);
 * </pre>
 * <p>
 * If you're using Kotlin you can import the extension functions from {@code org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelExtensions.kt} and do:
 * <pre>
 * subscriptionModel.subscribe("subscriptionId", StartAt.beginningOfTime()) { e ->
 *      println("Event: $e")
 * }
 * </pre>
 *
 * <p>
 * Note that the implementation uses an in-memory cache (default size is {@value #DEFAULT_CACHE_SIZE} but this can be configured using a {@link CatchupSubscriptionModelConfig})
 * to reduce the number of duplicate event when switching from historic events to the current cloud event position. It's highly recommended that the application logic is idempotent if the
 * cache size doesn't cover all duplicate events.
 * </p>
 * <br>
 * <p>
 * Delivery is at-least-once. Events written while the catch-up phase runs are reconciled and delivered, including events whose {@code time} is clock-skewed earlier than the replay cursor.
 * This reconciliation assumes the set of events matching the filter only grows while catching up, which holds for append-only stores. If events are deleted from the store while a catch-up
 * replay is running, the reconciliation can under-count and miss some events written during that replay. Avoid deleting events that match a running catch-up subscription's filter until it
 * has caught up.
 * </p>
 * <br>
 * <p>
 * Also note that the if a the subscription crashes during catch-up mode it'll continue where it left-off on restart, given the no specific `StartAt` position is supplied (i.e. if {@code StartAt.subscriptionModelDefault() is used}).
 * For this to work, the subscription must store the checkpoint in a {@link org.occurrent.subscription.api.blocking.CheckpointStorage} implementation periodically. It's possible to configure
 * how often this should happen in the {@link CatchupSubscriptionModelConfig}.
 * </p>
 * <br>
 * <p>
 * During catch-up the live resume token is captured before the bulk replay, so an event committed during the replay
 * is still delivered live. A replay that runs longer than the database change stream history (the MongoDB oplog
 * window) ages the token out, and the handover then fails loudly rather than dropping events. Size the oplog for very
 * large rebuilds.
 * </p>
 * <br>
 * <p>
 * The catch-up phase reads historic events by {@code position} when the backing store writes one. A DCB store always
 * does. A stream store does when {@code writesPosition()} is true, otherwise the catch-up falls back to the older
 * time-ordered replay. If a stream store starts writing position after previously running the time-based catch-up, a
 * stored time token is re-resolved to the model default instead of being read as a position.
 * </p>
 * <p>
 * This is the general catch-up entry point. It routes each subscription to stream or DCB replay by the subscription
 * filter and start position, dispatching over {@link StreamCatchupSubscriptionModel} (stream catch-up) and
 * {@code DcbCatchupSubscriptionModel} (DCB catch-up, ADR 20) so a single model can serve stream, DCB, or dual-mode
 * stores. A single-mode instance only depends on the module for that mode's store; a stream-only application does
 * not need {@code eventstore-api-dcb} on its classpath, since {@link StreamCatchupSubscriptionModel} lives in the
 * separate {@code stream-catchup-subscription} module this class depends on. A stream-only store that wants to avoid
 * the DCB dependency altogether can use {@link StreamCatchupSubscriptionModel} directly as the DCB-free variant.
 * </p>
 */
@NullMarked
public class CatchupSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, ReplayAwareSubscriptions, RepositionableSubscriptions {

    private static final int DEFAULT_CACHE_SIZE = CatchupSubscriptionModelConfig.DEFAULT_HANDOVER_CACHE_SIZE;

    private final CheckpointAwareSubscriptionModel subscriptionModel;
    private final @Nullable StreamCatchupSubscriptionModel streamCatchupSubscriptionModel;
    private final @Nullable DcbCatchupSubscriptionModel dcbCatchupSubscriptionModel;
    // The capability-agnostic position catch-up: the same position/time catch-up as the stream model but with no
    // capability scope, so it replays and delivers events of every capability, filtered only by the caller's plain
    // Filter. Present whenever a position/time-capable store (EventStoreQueries) is wired, i.e. in the stream-only and
    // dual-mode configurations. Null in the DCB-only configuration, where an AgnosticSubscriptionFilter routes to the
    // DCB model instead (a DCB-only store has only DCB events).
    private final @Nullable StreamCatchupSubscriptionModel agnosticCatchupSubscriptionModel;

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} the uses a default {@link CatchupSubscriptionModelConfig} with a cache size of
     * {@value #DEFAULT_CACHE_SIZE} but store the checkpoint during the <i>catch-up</i> phase (i.e. if the application crashes or is shutdown during the
     * catch-up phase then the subscription will start from the beginning on application restart). After the catch-up phase has completed, the {@link CheckpointAwareSubscriptionModel}
     * will dictate how often the checkpoint is stored.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     */
    public CatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries) {
        this(subscriptionModel, eventStoreQueries, new CatchupSubscriptionModelConfig(DEFAULT_CACHE_SIZE));
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} the uses the supplied {@link CatchupSubscriptionModelConfig}.
     * After catch-up mode has completed, the {@link CheckpointAwareSubscriptionModel} will dictate how often the checkpoint is stored.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     * @param config            The configuration to use
     */
    public CatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        // Shared so a same id routed to the stream child on one call and the agnostic child on another (an
        // AgnosticSubscriptionFilter versus a StreamSubscriptionFilter) still serializes through one handover, and
        // both still see the same current owner for that id, even though both children share this same delegate
        // and checkpoint storage.
        AbstractCatchupSubscriptionModel.SharedCatchupState sharedState = new AbstractCatchupSubscriptionModel.SharedCatchupState();
        this.streamCatchupSubscriptionModel = new StreamCatchupSubscriptionModel(subscriptionModel, eventStoreQueries, config, CatchupSubscriptionModel.class, sharedState);
        this.dcbCatchupSubscriptionModel = null;
        this.agnosticCatchupSubscriptionModel = new StreamCatchupSubscriptionModel(subscriptionModel, eventStoreQueries, config, CatchupSubscriptionModel.class, null, sharedState);
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} in DCB mode using a default {@link CatchupSubscriptionModelConfig}.
     * In DCB mode the catch-up phase replays historic DCB events ordered by their {@code position} (rather than stream
     * events ordered by time), and the subscription resumes by {@code position}. Only events matching {@code dcbQuery}
     * are delivered, in both the replay and the live phase.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param dcbEventStore     The DCB event store that will be used for the DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events this subscription delivers.
     */
    public CatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbCriteria dcbQuery) {
        this(subscriptionModel, dcbEventStore, dcbQuery, new CatchupSubscriptionModelConfig(DEFAULT_CACHE_SIZE));
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} in DCB mode using the supplied {@link CatchupSubscriptionModelConfig}.
     * In DCB mode the catch-up phase replays historic DCB events ordered by their {@code position} (rather than stream
     * events ordered by time), and the subscription resumes by {@code position}. Only events matching {@code dcbQuery}
     * are delivered, in both the replay and the live phase.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param dcbEventStore     The DCB event store that will be used for the DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events this subscription delivers.
     * @param config            The configuration to use.
     */
    public CatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbCriteria dcbQuery, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.streamCatchupSubscriptionModel = null;
        // No sharing needed here. This configuration constructs exactly one child, so there is no other route the
        // same id could take.
        this.dcbCatchupSubscriptionModel = new DcbCatchupSubscriptionModel(subscriptionModel, dcbEventStore, dcbQuery, config, CatchupSubscriptionModel.class);
        this.agnosticCatchupSubscriptionModel = null;
    }

    /**
     * Create a dual-mode instance that catches up both stream subscriptions (by {@code position} when the stream store
     * writes one, otherwise by time) and DCB subscriptions (by {@code position}). Each subscription is routed by its
     * filter and start position, so a single model serves an application that uses both streams and DCB.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for stream catch-up.
     * @param dcbEventStore     The DCB event store that will be used for DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events a DCB subscription delivers.
     * @param config            The configuration to use.
     */
    public CatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, DcbEventStore dcbEventStore, DcbCriteria dcbQuery, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        // Shared across all three children. A subscriptionId can route to any one of them on a given call
        // (routesToDcb, or an AgnosticSubscriptionFilter), and a later call for the same id can route to a
        // different one, so one handover and one current-owner record has to cover all three, not one each.
        AbstractCatchupSubscriptionModel.SharedCatchupState sharedState = new AbstractCatchupSubscriptionModel.SharedCatchupState();
        this.streamCatchupSubscriptionModel = new StreamCatchupSubscriptionModel(subscriptionModel, eventStoreQueries, config, CatchupSubscriptionModel.class, sharedState);
        this.dcbCatchupSubscriptionModel = new DcbCatchupSubscriptionModel(subscriptionModel, dcbEventStore, dcbQuery, config, CatchupSubscriptionModel.class, sharedState);
        this.agnosticCatchupSubscriptionModel = new StreamCatchupSubscriptionModel(subscriptionModel, eventStoreQueries, config, CatchupSubscriptionModel.class, null, sharedState);
    }

    /**
     * Shortcut to start subscribing to events matching the supplied filter from begging of time. Same as doing:
     *
     * <pre>
     * subscriptionModel.subscribe(&lt;subscriptionId&gt;, &lt;filter&gt;, StartAtTime.beginningOfTime(), &lt;action&gt;);
     * </pre>
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, StartAtTime.beginningOfTime(), action);
    }

    /**
     * Shortcut to start subscribing to <i>all</i> events from begging of time. Same as doing:
     *
     * <pre>
     * subscriptionModel.subscribe(&lt;subscriptionId&gt;, StartAtTime.beginningOfTime(), &lt;action&gt;);
     * </pre>
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
    }

    // The child gets the caller's own StartAt, but it resolves that under this class rather than its own, since
    // every child is built with CatchupSubscriptionModel.class as its context type. This layer's answer is
    // therefore the one acted on, which is why decidesWhereTheSubscriptionStarts() stays true here.
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        return route(filter, startAt).subscribe(subscriptionId, filter, startAt, action);
    }

    // Route to the DCB, stream, or capability-agnostic catch-up model. A single-mode model has only one inner model and
    // always routes there. A dual-mode model routes by filter type first, since a global position start is ambiguous
    // between the position-ordered replays. An AgnosticSubscriptionFilter routes to the unscoped agnostic model so both
    // stream and DCB events are delivered; if there is no agnostic model (a DCB-only store) it falls back to the DCB
    // model, whose store has only DCB events anyway. Only an already-resolved start is inspected for the fallback
    // heuristic, so routing reads no position storage.
    private SubscriptionModel route(@Nullable SubscriptionFilter filter, StartAt startAt) {
        if (filter instanceof AgnosticSubscriptionFilter) {
            if (agnosticCatchupSubscriptionModel != null) {
                return agnosticCatchupSubscriptionModel;
            }
            return Objects.requireNonNull(dcbCatchupSubscriptionModel);
        }
        return routesToDcb(filter, startAt)
                ? Objects.requireNonNull(dcbCatchupSubscriptionModel)
                : Objects.requireNonNull(streamCatchupSubscriptionModel);
    }

    private boolean routesToDcb(@Nullable SubscriptionFilter filter, StartAt startAt) {
        if (dcbCatchupSubscriptionModel == null) {
            return false;
        }
        if (streamCatchupSubscriptionModel == null) {
            return true;
        }
        if (filter instanceof DcbSubscriptionFilter) {
            return true;
        }
        if (filter instanceof StreamSubscriptionFilter) {
            return false;
        }
        return StreamCatchupSubscriptionModel.startsAtExplicitGlobalPosition(startAt, CatchupSubscriptionModel.class);
    }

    // Whichever catch-up children this configuration wired. Only the live delegate is always present.
    private Stream<AbstractCatchupSubscriptionModel> presentCatchupModels() {
        return Stream.of(streamCatchupSubscriptionModel, dcbCatchupSubscriptionModel, agnosticCatchupSubscriptionModel)
                .filter(Objects::nonNull);
    }

    // stopReplay() rather than a child's own stop(), which would reach the shared live delegate once per child. The
    // children have to be told so a replay already in flight stops delivering.
    @Override
    public void stop() {
        presentCatchupModels().forEach(AbstractCatchupSubscriptionModel::stopReplay);
        getWrappedSubscriptionModel().stop();
    }

    // resumeReplay() for the same reason stop() uses stopReplay().
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        presentCatchupModels().forEach(AbstractCatchupSubscriptionModel::resumeReplay);
        getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    // Asks the catch-up children too, because a replay is running before the live delegate has registered the
    // subscription. Repeating the delegate's answer per child is harmless here, unlike in stop() and start().
    @Override
    public boolean isRunning() {
        return presentCatchupModels().anyMatch(AbstractCatchupSubscriptionModel::isRunning)
                || getWrappedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return presentCatchupModels().anyMatch(model -> model.isRunning(subscriptionId))
                || getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    /**
     * A subscription lives in exactly one of the inner catch-up models, so asking all of them is the same as asking
     * the one that owns it, and the delegate is not asked at all: it only ever sees a subscription that has already
     * handed over.
     */
    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return presentCatchupModels().anyMatch(model -> model.isCatchingUp(subscriptionId));
    }

    /**
     * Registered on every mode-specific model, since which one ends up running this id is not known until it
     * subscribes. Answers true only when every present one accepts, so a model that cannot report its boundaries
     * makes the whole registration false and the caller falls back to polling, rather than leaving the id able to
     * land on a model that says nothing.
     */
    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        return presentCatchupModels().map(model -> model.listenForCatchup(subscriptionId, listener))
                .reduce(Boolean.TRUE, (a1, b1) -> a1 && b1);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return presentCatchupModels().anyMatch(model -> model.isPaused(subscriptionId))
                || getWrappedSubscriptionModel().isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    /**
     * A plain forward to whichever {@link RepositionableSubscriptions} the wrapped model resolves to, exactly
     * as the one-argument {@link #resumeSubscription(String)} above already forwards unconditionally rather than
     * routing through a catch-up child. Catch-up is therefore never re-triggered by a resume at an explicit
     * position either. It stays what it already was, a one-time replay driven from {@code subscribe}, not something
     * a lease regain can turn back on.
     *
     * @throws UnsupportedOperationException if the wrapped model is not itself repositionable.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        return RepositionableSubscriptions.findIn(getWrappedSubscriptionModel())
                .orElseThrow(() -> new UnsupportedOperationException(getWrappedSubscriptionModel().getClass().getSimpleName() + " is not repositionable"))
                .resumeSubscription(subscriptionId, startAt);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        // Neither mode is known ahead of time for a bare subscriptionId, so tell both inner models to drop their own
        // bookkeeping for it (a no-op on whichever one wasn't running it) and cancel the shared live delegate once.
        if (streamCatchupSubscriptionModel != null) {
            streamCatchupSubscriptionModel.cancelRunningCatchup(subscriptionId);
        }
        if (dcbCatchupSubscriptionModel != null) {
            dcbCatchupSubscriptionModel.cancelRunningCatchup(subscriptionId);
        }
        if (agnosticCatchupSubscriptionModel != null) {
            agnosticCatchupSubscriptionModel.cancelRunningCatchup(subscriptionId);
        }
        subscriptionModel.cancelSubscription(subscriptionId);
        // Position storage is shared config, not per-mode state; either inner model's config deletes the same
        // storage entry, so delegate to whichever is present instead of deleting twice.
        if (streamCatchupSubscriptionModel != null) {
            streamCatchupSubscriptionModel.deletePositionFromStorage(subscriptionId);
        } else if (dcbCatchupSubscriptionModel != null) {
            dcbCatchupSubscriptionModel.deletePositionFromStorage(subscriptionId);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        if (streamCatchupSubscriptionModel != null) {
            streamCatchupSubscriptionModel.markShuttingDown();
        }
        if (dcbCatchupSubscriptionModel != null) {
            dcbCatchupSubscriptionModel.markShuttingDown();
        }
        if (agnosticCatchupSubscriptionModel != null) {
            agnosticCatchupSubscriptionModel.markShuttingDown();
        }
        subscriptionModel.shutdown();
    }

    static boolean isTimeBasedCheckpoint(StartAt startAt) {
        return StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(startAt, CatchupSubscriptionModel.class);
    }

    static boolean isTimeBasedCheckpoint(Checkpoint checkpoint) {
        return StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(checkpoint);
    }

    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CatchupSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("streamCatchupSubscriptionModel=" + streamCatchupSubscriptionModel)
                .add("dcbCatchupSubscriptionModel=" + dcbCatchupSubscriptionModel)
                .toString();
    }
}
