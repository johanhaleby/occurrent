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
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.FilterMatcher;
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
 * Reactive stream catch-up: replays stream history matching a {@link Filter} in global {@code position} order via
 * {@link PositionOrderedReader}, then hands over to a live subscription, all as a single {@link Flux}. It lets a
 * reactive read model rebuild from the start of the stream sequence and then keep up with new events.
 * <p>
 * This is the stream counterpart of {@code ReactorDcbCatchupSubscriptionModel}. It replays through
 * {@link PositionOrderedReader#readInPositionOrder(Filter, PositionRange)} and matches a stream {@link Filter}
 * in-process, where the DCB model uses a {@code DcbCriteria}. Otherwise the two are the same, because both read the
 * same global {@code position} sequence.
 * <p>
 * This model only ever replays and delivers stream-capability events. On a store that has both the {@code STREAM} and
 * {@code DCB} capabilities enabled at once, that promise is enforced, not merely descriptive: a
 * {@link Filter#capability(EventStoreCapability) STREAM-capability filter} is ANDed into both the position-ordered
 * replay reads and the filter handed to (and matched against) the live subscription, so a DCB-tagged event never
 * reaches a subscriber of this model in either phase (see ADR 50). A caller filter is still honored; the capability
 * guard is composed on top of it.
 * <p>
 * Only meaningful for a store that writes a {@code position} on stream events. This model cannot check that itself
 * (it depends only on {@link PositionOrderedReader}), so do not wire it up against a store that does not write
 * position. If you do, {@link PositionOrderedReader#readInPositionOrder(Filter, PositionRange)} throws
 * {@link UnsupportedOperationException}.
 * <p>
 * The live resume token is captured before the bulk replay, not after, so an event that commits during the replay is
 * still delivered by the live subscription. The replay pages the sequence in {@code position} windows, then a
 * reconciliation pass keeps paging until the head stops advancing so events written during the replay are delivered
 * in order. A bounded id cache dedupes events that both the replay and the live subscription see.
 * <p>
 * If the replay runs longer than the change stream history (the MongoDB oplog window), the captured token ages out
 * and the live resume fails loudly rather than silently dropping an event. Size the oplog for very large rebuilds.
 * If the model reports no resume token at all (for example an empty oplog or a restricted cluster), the subscription
 * fails loudly for the same reason.
 * <p>
 * This model does not persist subscription positions, so layer a durable model on top (for example
 * {@code ReactorDurableSubscriptionModel}) if resume across restarts is needed.
 * <p>
 * It implements {@link CheckpointAwareSubscriptionModel}, so it can sit as a plain (cold) subscription model underneath
 * a durable model. Its generic {@link #subscribe(SubscriptionFilter, StartAt)} only accepts an
 * {@link StreamSubscriptionFilter}, or no filter, in which case the default {@link Filter} passed to the
 * constructor is used.
 * <p>
 * It also implements the reactor {@link SubscriptionModel} (issues #547 and #550): named subscriptions replay the same
 * way and then hand the live half to the wrapped model's own named {@code subscribe(..)}, so a named catch-up
 * subscription inherits everything the wrapped model does for one, its handler retry and its synchronous refusal of an
 * unsupported filter included. A failing action during the replay itself is not retried, matching the blocking
 * catch-up models, and the failure reaches whoever waits on the returned subscription. The named path therefore
 * requires the wrapped model to manage named subscriptions itself; over a cold-only wrapped model it refuses loudly.
 * The life cycle forwards to the wrapped model, so give each catch-up model its own wrapped model rather than sharing
 * one between compositions.
 */
@NullMarked
public class ReactorStreamCatchupSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, ReplayAwareSubscriptions {

    // Guards every replay read and the live subscription this model performs so a DCB-tagged event is never delivered
    // to a stream subscriber, even on a store that has both capabilities enabled (see ADR 50).
    // Package-private rather than private so the ReactorCatchupSubscriptionModel dispatcher, which builds both the
    // stream-scoped and the capability-agnostic instance, can name the scope it wants.
    static final Filter STREAM_CAPABILITY_FILTER = Filter.capability(EventStoreCapability.STREAM);

    /**
     * Default number of positions read per replay window.
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
    private final PositionOrderedReader positionOrderedReader;
    private final @Nullable Filter defaultFilter;
    private final long windowSize;
    private final int handoverCacheSize;
    // The capability guard ANDed into every replay read and the live subscription. It is {@link #STREAM_CAPABILITY_FILTER}
    // for a stream subscription (so a DCB-tagged event never reaches a stream subscriber, see ADR 50), and {@code null}
    // for a capability-agnostic subscription, which then filters only by the caller's plain Filter and so delivers
    // events of every capability.
    private final @Nullable Filter capabilityScope;
    // The class a caller's StartAt.dynamic sees. This model's own class when it is used directly, and the dispatcher's
    // class when ReactorCatchupSubscriptionModel wraps it, so a caller matching on the type it holds keeps working.
    private final Class<?> subscriptionModelContextType;

    public ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader) {
        this(subscriptionModel, positionOrderedReader, null, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, positionOrderedReader, null, windowSize, handoverCacheSize);
    }

    /**
     * Create a catch-up model with a default {@link Filter} used by {@link #subscribe(SubscriptionFilter, StartAt)}
     * when it is called without a filter. Lets one model serve every stream subscription, each narrowing with its own
     * filter.
     */
    public ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, STREAM_CAPABILITY_FILTER);
    }

    /**
     * @param capabilityScope The capability {@link Filter} ANDed into every replay read and the live subscription.
     *                        Pass {@link #STREAM_CAPABILITY_FILTER} for a stream subscription, or {@code null} for a
     *                        capability-agnostic subscription that delivers events of every capability, filtered only by
     *                        the caller's plain {@link Filter}.
     */
    public ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize, @Nullable Filter capabilityScope) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, capabilityScope, ReactorStreamCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                     {@code SubscriptionModelContext#subscriptionModelType()}. The
     *                                     {@code ReactorCatchupSubscriptionModel} dispatcher passes its own class here
     *                                     so a caller that pattern-matches on the public dispatcher type keeps working
     *                                     regardless of which mode-specific model runs the catch-up. Mirrors the
     *                                     blocking {@code StreamCatchupSubscriptionModel}.
     */
    ReactorStreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize, @Nullable Filter capabilityScope, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.subscriptionModelContextType = requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
        this.namedSubscriptions = new NamedCatchupSupport(subscriptionModel, subscriptionModelContextType);
        this.positionOrderedReader = requireNonNull(positionOrderedReader, PositionOrderedReader.class.getSimpleName() + " cannot be null");
        this.defaultFilter = defaultFilter;
        this.capabilityScope = capabilityScope;
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
     * The generic (cold) subscription-model entry point. The {@code filter} must be an
     * {@link StreamSubscriptionFilter}, or {@code null} to use the default {@link Filter} supplied to the
     * constructor. A {@code startAt} that resolves to a {@code position} replays history from that position and then
     * goes live, anything else goes straight to live.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        final Filter resolvedFilter;
        try {
            resolvedFilter = resolveFilter(filter);
        } catch (IllegalArgumentException e) {
            // The cold primitive reports an unsupported filter through the publisher, per its contract.
            return Flux.error(e);
        }
        return subscribe(resolvedFilter, startAt);
    }

    // Resolves the caller's SubscriptionFilter to the stream Filter this model replays and delivers, throwing for a
    // filter this model does not understand. The named path throws this synchronously from subscribe(..); the cold
    // primitive converts it to an error publisher.
    private Filter resolveFilter(@Nullable SubscriptionFilter filter) {
        if (filter == null) {
            if (defaultFilter == null) {
                throw new IllegalArgumentException("A " + StreamSubscriptionFilter.class.getSimpleName() + " is required unless a default " + Filter.class.getSimpleName() + " was supplied to the constructor.");
            }
            return defaultFilter;
        } else if (filter instanceof StreamSubscriptionFilter streamSubscriptionFilter) {
            return streamSubscriptionFilter.filter();
        } else if (filter instanceof AgnosticSubscriptionFilter agnosticSubscriptionFilter) {
            return agnosticSubscriptionFilter.filter();
        }
        throw new UnsupportedSubscriptionFilterException(filter.getClass(), ReactorStreamCatchupSubscriptionModel.class.getSimpleName() + " only supports an " + StreamSubscriptionFilter.class.getSimpleName() + " or " + AgnosticSubscriptionFilter.class.getSimpleName() + ", but got " + filter.getClass().getName());
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
        Filter scoped = withCapabilityScope(resolveFilter(filter));
        Predicate<CloudEvent> matchesLocally = FilterMatcher.matcherIgnoringPayloadConditions(scoped);
        Predicate<CloudEvent> livePredicate = cloudEvent -> OccurrentCloudEventExtension.getPosition(cloudEvent) > 0 && matchesLocally.test(cloudEvent);
        SubscriptionFilter liveFilter = StreamSubscriptionFilter.filter(scoped);

        StartAt resolved = startAt.get(new SubscriptionModelContext(subscriptionModelContextType));
        if (!(resolved instanceof StartAt.StartAtCheckpoint position) || !GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint)) {
            return namedSubscriptions.subscribeStraightToLive(subscriptionId, liveFilter, livePredicate, resolved == null ? startAt : resolved, action);
        }
        long startPosition = GlobalCheckpoint.positionOf(position.checkpoint);
        CatchupReader reader = new StreamCatchupReader(positionOrderedReader, scoped);
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
     * Subscribe to stream events matching {@code filter}, starting from a {@code position}-based
     * {@link StartAt#checkpoint(Checkpoint)} built from {@link GlobalCheckpoint} (for
     * example {@code GlobalCheckpoint.of(0)} to replay from the beginning) to replay history then go live.
     * Any other start (now or the subscription model default) goes straight to live.
     */
    public Flux<CloudEvent> subscribe(Filter callerFilter, StartAt startAt) {
        requireNonNull(callerFilter, "Filter cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        // AND the capability scope onto the caller's filter. For a stream subscription this keeps a DCB-tagged event
        // out of the replay reads, the live subscription filter, and the in-process live predicate below (see ADR 50).
        // For a capability-agnostic subscription the scope is null, so the caller's filter is used unchanged and events
        // of every capability are delivered.
        Filter filter = withCapabilityScope(callerFilter);
        // What the in-process predicates below can decide for themselves. A condition on the data payload is treated as
        // already satisfied, because reading a payload needs a DataFieldReader this model wraps no store to obtain, and
        // the store applied the real condition to have delivered the event (ADR 92).
        Predicate<CloudEvent> matchesLocally = FilterMatcher.matcherIgnoringPayloadConditions(filter);

        StartAt resolved = startAt.get(new SubscriptionModelContext(subscriptionModelContextType));
        if (!(resolved instanceof StartAt.StartAtCheckpoint position) || !GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint)) {
            // Not a catch-up position, so go straight to live. Filter in-process too, so a backend that does not
            // honor the filter server-side still only delivers matching events, and skip events without a position.
            return subscriptionModel.subscribe(StreamSubscriptionFilter.filter(filter), resolved == null ? startAt : resolved)
                    .filter(cloudEvent -> OccurrentCloudEventExtension.getPosition(cloudEvent) > 0 && matchesLocally.test(cloudEvent));
        }

        long startPosition = GlobalCheckpoint.positionOf(position.checkpoint);
        CatchupReader reader = new StreamCatchupReader(positionOrderedReader, filter);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, windowSize, handoverCacheSize);
        Predicate<CloudEvent> livePredicate = cloudEvent -> OccurrentCloudEventExtension.getPosition(cloudEvent) > 0 && matchesLocally.test(cloudEvent);
        return pipeline.catchup(subscriptionModel, StreamSubscriptionFilter.filter(filter), livePredicate, startPosition);
    }

    // ANDs the capability scope onto the caller's filter. When the scope is null (a capability-agnostic subscription)
    // the caller's filter is returned unchanged, so events of every capability are delivered. Since Filter.all() means
    // "no constraint", ANDing the scope onto it is exactly the scope filter alone.
    private Filter withCapabilityScope(Filter filter) {
        if (capabilityScope == null) {
            return filter;
        }
        return filter instanceof Filter.All ? capabilityScope : filter.and(capabilityScope);
    }

    // Reads stream events in position order through the PositionOrderedReader, wrapping each with its position so a
    // durable model layered on top can persist replay progress.
    private record StreamCatchupReader(PositionOrderedReader positionOrderedReader, Filter filter) implements CatchupReader {
        @Override
        public Flux<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
            return positionOrderedReader.readInPositionOrder(filter, PositionRange.between(fromExclusive, toInclusive))
                    .map(event -> (CloudEvent) new CheckpointAwareCloudEvent(event, GlobalCheckpoint.of(OccurrentCloudEventExtension.getPosition(event))));
        }

        @Override
        public Mono<Long> currentHead() {
            return positionOrderedReader.currentPosition();
        }
    }
}
