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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Reactive DCB catch-up: replays the DCB history matching a {@link DcbCriteria} by {@code position}, then hands over to
 * a live subscription, as a single {@link Flux}. It lets a reactive read model rebuild from the beginning of the DCB
 * sequence and then keep up with new events.
 * <p>
 * The handover preserves the central invariant of the blocking catch-up. The live change-stream resume token is captured
 * before the bulk replay, not after, so a DCB event that commits during the replay is still delivered by the live
 * subscription. The replay pages the sequence in {@code position} windows (no count and no time sort, because
 * {@code position} is monotonic and server-assigned), then a reconciliation pass keeps paging until the head stops
 * advancing to deliver events written during the replay in order. The handover seam is deduplicated with a bounded id
 * cache so a reconciliation event the live subscription also sees is delivered once.
 * <p>
 * Trade-off: if the replay runs longer than the change stream history (the MongoDB oplog window), the captured token
 * ages out and the live resume fails loudly rather than silently dropping an event. Size the oplog for very large
 * rebuilds. If the model cannot report a resume token at all (for example an empty oplog or a restricted cluster), the
 * subscription fails loudly for the same reason, rather than replaying without a guaranteed handover to live.
 * <p>
 * This is the DCB path only. Stream time-based catch-up is not provided here, and this model does not persist
 * subscription positions, so layer a durable model on top (for example {@code ReactorDurableSubscriptionModel}) if
 * resume across restarts is needed.
 * <p>
 * It implements {@link CheckpointAwareSubscriptionModel}, so it can sit as a plain (cold) subscription model underneath a
 * durable model or be handed to the reactive DCB subscription DSL. Its generic {@link #subscribe(SubscriptionFilter, StartAt)}
 * only understands a {@link DcbSubscriptionFilter} (or no filter, in which case a default {@link DcbCriteria} supplied to the
 * constructor is used), since catch-up is DCB-specific.
 */
@NullMarked
class ReactorDcbCatchupSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, ReplayAwareSubscriptions {

    /**
     * Default number of DCB positions read per replay window.
     */
    public static final long DEFAULT_POSITION_WINDOW_SIZE = 1000;
    /**
     * Default ceiling on the number of event ids kept to dedupe the replay-to-live handover. Grows to cover the
     * replay-to-live overlap (bounded by write volume during replay, not total history) and evicts oldest-first
     * past this ceiling. Exceeding it causes extra duplicate deliveries, never loss (at-least-once); raise it to
     * cut duplicates on a large rebuild or lower it to cap memory (each id is a short string). Well above the
     * previous {@code 1000} so a rebuild under heavy concurrent writes no longer evicts the overlap before live
     * re-delivers it.
     */
    public static final int DEFAULT_HANDOVER_CACHE_SIZE = 100_000;

    private final CheckpointAwareSubscriptionModel subscriptionModel;
    private final NamedCatchupSupport namedSubscriptions;
    private final DcbEventStore dcbEventStore;
    private final @Nullable DcbCriteria defaultCriteria;
    private final long windowSize;
    private final int handoverCacheSize;
    // The class a caller's StartAt.dynamic sees. This model's own class when it is used directly, and the dispatcher's
    // class when ReactorCatchupSubscriptionModel wraps it, so a caller matching on the type it holds keeps working.
    private final Class<?> subscriptionModelContextType;

    public ReactorDcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore) {
        this(subscriptionModel, dcbEventStore, null, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorDcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, dcbEventStore, null, windowSize, handoverCacheSize);
    }

    /**
     * Create a catch-up model with a default {@link DcbCriteria} used by {@link #subscribe(SubscriptionFilter, StartAt)}
     * when it is called without a filter. This mirrors the blocking {@code CatchupSubscriptionModel} constructor that
     * takes a shared {@code DcbCriteria.all()}, so the reactive starter can wire one model that every DCB subscription
     * narrows with its own query in the consumer.
     */
    public ReactorDcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultCriteria) {
        this(subscriptionModel, dcbEventStore, defaultCriteria, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorDcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultCriteria, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, dcbEventStore, defaultCriteria, windowSize, handoverCacheSize, ReactorDcbCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                     {@code SubscriptionModelContext#subscriptionModelType()}. The
     *                                     {@link ReactorCatchupSubscriptionModel} dispatcher passes its own class here
     *                                     so a caller that pattern-matches on the public dispatcher type keeps working
     *                                     regardless of which mode-specific model runs the catch-up. Mirrors the
     *                                     blocking {@code DcbCatchupSubscriptionModel}.
     */
    ReactorDcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultCriteria, long windowSize, int handoverCacheSize, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.subscriptionModelContextType = requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
        this.namedSubscriptions = new NamedCatchupSupport(subscriptionModel, subscriptionModelContextType);
        this.dcbEventStore = requireNonNull(dcbEventStore, DcbEventStore.class.getSimpleName() + " cannot be null");
        this.defaultCriteria = defaultCriteria;
        if (windowSize <= 0) {
            throw new IllegalArgumentException("Window size must be greater than zero");
        }
        if (handoverCacheSize <= 0) {
            throw new IllegalArgumentException("Handover cache size must be greater than zero");
        }
        this.windowSize = windowSize;
        this.handoverCacheSize = handoverCacheSize;
    }

    /**
     * The generic (cold) subscription-model entry point. The {@code filter} must be a {@link DcbSubscriptionFilter}, or
     * {@code null} to use the default {@link DcbCriteria} supplied to the constructor. A {@code startAt} that resolves to a
     * {@code position} replays history from that position and then goes live, anything else goes straight to live.
     * This is how a durable model wrapping this catch-up model, and the reactive DCB subscription DSL, drive it.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        final DcbCriteria criteria;
        try {
            criteria = resolveCriteria(filter);
        } catch (IllegalArgumentException e) {
            // The cold primitive reports an unsupported filter through the publisher, per its contract.
            return Flux.error(e);
        }
        return subscribe(criteria, startAt);
    }

    // Resolves the caller's SubscriptionFilter to the DcbCriteria this model replays and delivers, throwing for a
    // filter this model does not understand. The named path throws this synchronously from subscribe(..); the cold
    // primitive converts it to an error publisher.
    private DcbCriteria resolveCriteria(@Nullable SubscriptionFilter filter) {
        if (filter == null) {
            if (defaultCriteria == null) {
                throw new IllegalArgumentException("A " + DcbSubscriptionFilter.class.getSimpleName() + " is required unless a default " + DcbCriteria.class.getSimpleName() + " was supplied to the constructor.");
            }
            return defaultCriteria;
        } else if (filter instanceof DcbSubscriptionFilter dcbSubscriptionFilter) {
            return dcbSubscriptionFilter.criteria();
        }
        throw new UnsupportedSubscriptionFilterException(filter.getClass(), ReactorDcbCatchupSubscriptionModel.class.getSimpleName() + " only supports a " + DcbSubscriptionFilter.class.getSimpleName() + ", but got " + filter.getClass().getName());
    }

    /**
     * The named subscription entry point (issues #547 and #550). An unsupported {@code filter} is refused here,
     * synchronously. A {@code startAt} that resolves to a {@code position} replays history from that position without
     * retrying a failing action, then hands the live half to the wrapped model's own named {@code subscribe(..)}, so
     * live delivery inherits the wrapped model's handler retry. Any other start delegates straight to the wrapped
     * model. Requires the wrapped model to manage named subscriptions itself.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        DcbCriteria criteria = resolveCriteria(filter);
        Predicate<CloudEvent> livePredicate = cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, criteria);
        SubscriptionFilter liveFilter = DcbSubscriptionFilter.filter(criteria);

        StartAt resolved = startAt.get(new SubscriptionModelContext(subscriptionModelContextType));
        if (!(resolved instanceof StartAt.StartAtCheckpoint position) || !GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint)) {
            return namedSubscriptions.subscribeStraightToLive(subscriptionId, liveFilter, livePredicate, resolved == null ? startAt : resolved, action);
        }
        long startPosition = GlobalCheckpoint.positionOf(position.checkpoint);
        CatchupReader reader = new DcbCatchupReader(dcbEventStore, criteria);
        return namedSubscriptions.subscribeWithCatchup(subscriptionId, liveFilter, livePredicate, reader, windowSize, handoverCacheSize, startPosition, action);
    }

    // --- The life cycle forwards to the wrapped model, with bookkeeping for subscriptions still replaying.

    @Override
    public void stop() {
        namedSubscriptions.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        namedSubscriptions.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return namedSubscriptions.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return namedSubscriptions.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return namedSubscriptions.isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return namedSubscriptions.resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        namedSubscriptions.pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        namedSubscriptions.cancelSubscription(subscriptionId);
    }

    // Whether a replay for this id is in flight here, so this model is the only one that can answer for it. Lets a
    // dispatcher over several catch-up models find the one that owns an id instead of picking one of them.
    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return namedSubscriptions.isCatchingUp(subscriptionId);
    }

    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        return namedSubscriptions.listenForCatchup(subscriptionId, listener);
    }




    @Override
    public void shutdown() {
        namedSubscriptions.shutdown();
    }

    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        return subscriptionModel.globalCheckpoint();
    }

    /**
     * Subscribe to DCB events matching {@code criteria}. A {@link DcbStartAt} that carries a {@code position} (for
     * example {@link DcbStartAt#beginning()} or {@link DcbStartAt#afterPosition(long)}) replays history from that
     * position and then goes live. Any other start (now or the subscription model default) goes straight to live.
     */
    public Flux<CloudEvent> subscribe(DcbCriteria criteria, DcbStartAt startAt) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        return subscribe(criteria, startAt.toStartAt());
    }

    private Flux<CloudEvent> subscribe(DcbCriteria criteria, StartAt startAt) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        StartAt resolved = startAt.get(new SubscriptionModelContext(subscriptionModelContextType));
        if (!(resolved instanceof StartAt.StartAtCheckpoint position) || !GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint)) {
            // Not a DCB catch-up position, so go straight to live. Apply the same in-process DCB floor the replay-to-live
            // path and the DcbSubscriptionModel adapter apply, so a backend that does not honor the filter server-side
            // still only delivers events matching the criteria.
            return subscriptionModel.subscribe(DcbSubscriptionFilter.filter(criteria), resolved == null ? startAt : resolved)
                    .filter(cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, criteria));
        }

        long startPosition = GlobalCheckpoint.positionOf(position.checkpoint);
        CatchupReader reader = new DcbCatchupReader(dcbEventStore, criteria);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, windowSize, handoverCacheSize);
        Predicate<CloudEvent> livePredicate = cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, criteria);
        return pipeline.catchup(subscriptionModel, DcbSubscriptionFilter.filter(criteria), livePredicate, startPosition);
    }

    // Reads DCB events in position order through the DcbEventStore, wrapping each with its position so a durable
    // model layered on top can persist replay progress.
    private record DcbCatchupReader(DcbEventStore dcbEventStore, DcbCriteria criteria) implements CatchupReader {
        @Override
        public Flux<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
            return dcbEventStore.read(criteria, DcbReadOptions.between(fromExclusive, toInclusive))
                    .flatMapMany(stream -> Flux.fromIterable(stream.events())
                            .map(event -> (CloudEvent) new CheckpointAwareCloudEvent(event, GlobalCheckpoint.of(OccurrentCloudEventExtension.getPosition(event)))));
        }

        @Override
        public Mono<Long> currentHead() {
            // lastSequencePosition is the global head at read time regardless of whether the criteria matched anything.
            return dcbEventStore.read(criteria, DcbReadOptions.between(0, 0)).map(stream -> stream.lastSequencePosition());
        }
    }
}
