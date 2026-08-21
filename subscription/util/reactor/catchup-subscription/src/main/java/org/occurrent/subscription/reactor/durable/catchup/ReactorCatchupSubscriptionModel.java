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
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The general reactive catch-up entry point. It routes each subscription to stream or DCB replay by the subscription
 * filter and start position, dispatching over {@link ReactorStreamCatchupSubscriptionModel} (stream catch-up) and
 * {@code ReactorDcbCatchupSubscriptionModel} (DCB catch-up), so a single model serves an application that uses streams,
 * DCB, or both. Mirrors the routing of the blocking {@code CatchupSubscriptionModel}. A stream-only store that wants to
 * avoid the DCB dependency can use {@link ReactorStreamCatchupSubscriptionModel} directly as the DCB-free variant.
 * <p>
 * It also implements the reactor {@link SubscriptionModel} (issues #547 and #550): a named subscription is routed the
 * same way and handled by the routed inner model, which replays and then hands the live half to the wrapped model's
 * own named {@code subscribe(..)}, so retry and synchronous filter refusal are inherited. A durable model wrapping
 * this one therefore delegates to it rather than driving the cold primitive itself. The life cycle forwards to the
 * wrapped model, so give each composition its own wrapped model rather than sharing one.
 * <p>
 * A subscription routed through this model reports <em>this</em> class to a caller's {@link StartAt#dynamic(Function)},
 * on both the cold and the named path, so a start position that branches on the subscription model type matches the
 * type the caller holds rather than the mode-specific model that happens to run the catch-up.
 */
@NullMarked
public class ReactorCatchupSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, ReplayAwareSubscriptions {

    private final @Nullable ReactorStreamCatchupSubscriptionModel streamCatchupSubscriptionModel;
    private final @Nullable ReactorDcbCatchupSubscriptionModel dcbCatchupSubscriptionModel;
    // The capability-agnostic position catch-up: the same position catch-up as the stream model but with no capability
    // scope, so it replays and delivers events of every capability, filtered only by the caller's plain Filter. Present
    // whenever a PositionOrderedReader is wired (stream-only and dual-mode). Null in the DCB-only configuration, where
    // an AgnosticSubscriptionFilter routes to the DCB model instead (a DCB-only store has only DCB events).
    private final @Nullable ReactorStreamCatchupSubscriptionModel agnosticCatchupSubscriptionModel;

    /**
     * Create a stream-only instance. Every subscription routes to the stream catch-up model.
     */
    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, ReactorStreamCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorStreamCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, ReactorStreamCatchupSubscriptionModel.STREAM_CAPABILITY_FILTER, ReactorCatchupSubscriptionModel.class);
        this.dcbCatchupSubscriptionModel = null;
        this.agnosticCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, null, ReactorCatchupSubscriptionModel.class);
    }

    /**
     * Create a DCB-only instance. Every subscription routes to the DCB catch-up model.
     */
    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery) {
        this(subscriptionModel, dcbEventStore, defaultQuery, ReactorDcbCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorDcbCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery, long windowSize, int handoverCacheSize) {
        this.streamCatchupSubscriptionModel = null;
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), dcbEventStore, defaultQuery, windowSize, handoverCacheSize, ReactorCatchupSubscriptionModel.class);
        this.agnosticCatchupSubscriptionModel = null;
    }

    /**
     * Create a dual-mode instance that catches up both stream subscriptions (by {@code position}, over
     * {@code positionOrderedReader}) and DCB subscriptions (by {@code position}, over {@code dcbEventStore}). Each
     * subscription is routed by its filter and start position, so a single model serves an application that uses
     * both streams and DCB.
     */
    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, ReactorStreamCatchupSubscriptionModel.STREAM_CAPABILITY_FILTER, ReactorCatchupSubscriptionModel.class);
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, dcbEventStore, defaultQuery, windowSize, handoverCacheSize, ReactorCatchupSubscriptionModel.class);
        this.agnosticCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, null, ReactorCatchupSubscriptionModel.class);
    }

    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, dcbEventStore, defaultQuery, defaultFilter, ReactorStreamCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorStreamCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        return route(filter, startAt).subscribe(filter, startAt);
    }

    // Route to the DCB, stream, or capability-agnostic catch-up model. An AgnosticSubscriptionFilter routes to the
    // unscoped agnostic model so both stream and DCB events are delivered; if there is no agnostic model (a DCB-only
    // store) it falls back to the DCB model, whose store has only DCB events anyway.
    private CheckpointAwareSubscriptionModel route(@Nullable SubscriptionFilter filter, StartAt startAt) {
        if (filter instanceof AgnosticSubscriptionFilter) {
            return agnosticCatchupSubscriptionModel != null
                    ? agnosticCatchupSubscriptionModel
                    : requireNonNull(dcbCatchupSubscriptionModel);
        }
        return routesToDcb(filter, startAt)
                ? requireNonNull(dcbCatchupSubscriptionModel)
                : requireNonNull(streamCatchupSubscriptionModel);
    }

    // Route to the DCB path or the stream path. A single-mode model has only one inner model and always routes there.
    // A dual-mode model routes by filter type first, since a global position start is ambiguous. Stream and DCB replay
    // both use a GlobalCheckpoint, so only the filter tells them apart. A null filter falls back to the
    // position heuristic and is narrowed by the default query or filter.
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
        return startsAtExplicitDcbPosition(startAt);
    }

    private static boolean startsAtExplicitDcbPosition(StartAt startAt) {
        StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(ReactorCatchupSubscriptionModel.class));
        return resolved instanceof StartAtCheckpoint position
                && GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint);
    }

    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        // Both inner models delegate globalCheckpoint() to the same wrapped subscriptionModel, so either
        // one reports the identical position. In dual mode, ask whichever is present; there is no dcb-vs-stream
        // ambiguity here because this is the wrapped live model's position, not a catch-up cursor.
        return dcbCatchupSubscriptionModel != null
                ? dcbCatchupSubscriptionModel.globalCheckpoint()
                : requireNonNull(streamCatchupSubscriptionModel).globalCheckpoint();
    }

    /**
     * The named subscription entry point (issues #547 and #550): routes exactly like the cold
     * {@link #subscribe(SubscriptionFilter, StartAt)} and hands the subscription to the routed inner model, which
     * replays and then delegates the live half to the wrapped model's own named {@code subscribe(..)}. Everything the
     * wrapped model does for a named subscription is therefore inherited, its handler retry and its synchronous
     * refusal of an unsupported filter included.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        SubscriptionModel routed = (SubscriptionModel) route(filter, startAt);
        // Claim the owner slot before subscribing, so a life-cycle call racing the subscribe reaches the model that
        // owns the replay before that model has registered the replay itself; roll the claim back if the subscribe
        // refuses, so the id stays free.
        if (subscriptionOwners.putIfAbsent(subscriptionId, routed) != null) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        final Subscription subscription;
        try {
            subscription = routed.subscribe(subscriptionId, filter, startAt, action);
        } catch (RuntimeException e) {
            subscriptionOwners.remove(subscriptionId, routed);
            throw e;
        }
        return subscription;
    }

    // Which inner model a named subscription was routed to, so a per-subscription life-cycle call reaches the model
    // that may still be replaying it. It is also the duplicate-id guard across the inner models, which cannot see each
    // other's replays because an id stays invisible to the wrapped model until the handover.
    private final ConcurrentMap<String, SubscriptionModel> subscriptionOwners = new ConcurrentHashMap<>();

    // Routes a per-subscription life-cycle call. The record above answers for every id this dispatcher created, and a
    // replay still in flight is the one state the wrapped model cannot answer for, so the inner models are asked next.
    // Anything left belongs to the wrapped model, and every inner model forwards there, so one of them gives its answer.
    private SubscriptionModel ownerOf(String subscriptionId) {
        SubscriptionModel owner = subscriptionOwners.get(subscriptionId);
        if (owner != null) {
            return owner;
        }
        SubscriptionModel replaying = innerModelCatchingUp(subscriptionId);
        return replaying != null ? replaying : anyInnerModel();
    }

    /**
     * A subscription lives in exactly one of the inner catch-up models, so asking all of them is the same as asking
     * the one that owns it.
     */
    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return innerModelCatchingUp(subscriptionId) != null;
    }

    /**
     * Asked of whichever inner model owns this id, since only it knows which part of its catch-up it has reached.
     * Left to the default here and a projection fed through this model would record nothing for the events its
     * reconciliation delivered.
     */
    @Override
    public boolean isReplayingHistory(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        SubscriptionModel owner = innerModelCatchingUp(subscriptionId);
        return owner instanceof ReplayAwareSubscriptions replayAware && replayAware.isReplayingHistory(subscriptionId);
    }

    /**
     * Asked of whichever inner model owns this id, for the same reason {@link #isReplayingHistory(String)} is.
     */
    @Override
    public long catchupGeneration(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        SubscriptionModel owner = innerModelCatchingUp(subscriptionId);
        return owner instanceof ReplayAwareSubscriptions replayAware ? replayAware.catchupGeneration(subscriptionId) : 0L;
    }

    /**
     * Asked of whichever inner model owns this id, for the same reason {@link #isReplayingHistory(String)} is.
     */
    @Override
    public org.occurrent.subscription.CatchupSnapshot catchupSnapshot(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        SubscriptionModel owner = innerModelCatchingUp(subscriptionId);
        return owner instanceof ReplayAwareSubscriptions replayAware ? replayAware.catchupSnapshot(subscriptionId) : org.occurrent.subscription.CatchupSnapshot.LIVE;
    }

    private @Nullable SubscriptionModel innerModelCatchingUp(String subscriptionId) {
        if (streamCatchupSubscriptionModel != null && streamCatchupSubscriptionModel.isCatchingUp(subscriptionId)) {
            return streamCatchupSubscriptionModel;
        }
        if (dcbCatchupSubscriptionModel != null && dcbCatchupSubscriptionModel.isCatchingUp(subscriptionId)) {
            return dcbCatchupSubscriptionModel;
        }
        if (agnosticCatchupSubscriptionModel != null && agnosticCatchupSubscriptionModel.isCatchingUp(subscriptionId)) {
            return agnosticCatchupSubscriptionModel;
        }
        return null;
    }

    // Any inner model, which is not an arbitrary choice for a life-cycle call, since they all forward to the same
    // wrapped model. Going through one of them rather than to that model directly keeps the documented behaviour over
    // a cold-only wrapped model, which has no life cycle to forward to.
    private SubscriptionModel anyInnerModel() {
        return streamCatchupSubscriptionModel != null ? streamCatchupSubscriptionModel : requireNonNull(dcbCatchupSubscriptionModel);
    }

    // The distinct inner models. Model-wide calls go to each, and each forwards to the same wrapped model, whose
    // model-wide life-cycle operations are idempotent, so the repeated forward is harmless while the per-inner replay
    // bookkeeping is what genuinely needs every inner to see the call.
    private Stream<SubscriptionModel> innerModels() {
        return Stream.of(streamCatchupSubscriptionModel, dcbCatchupSubscriptionModel, agnosticCatchupSubscriptionModel)
                .filter(Objects::nonNull)
                .map(SubscriptionModel.class::cast);
    }

    @Override
    public void stop() {
        innerModels().forEach(SubscriptionModel::stop);
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        innerModels().forEach(inner -> inner.start(resumeSubscriptionsAutomatically));
    }

    @Override
    public boolean isRunning() {
        return anyInnerModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return ownerOf(subscriptionId).isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return ownerOf(subscriptionId).isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return ownerOf(subscriptionId).resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        ownerOf(subscriptionId).pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        SubscriptionModel owner = ownerOf(subscriptionId);
        subscriptionOwners.remove(subscriptionId);
        owner.cancelSubscription(subscriptionId);
    }

    @Override
    public void shutdown() {
        subscriptionOwners.clear();
        innerModels().forEach(SubscriptionModel::shutdown);
    }
}
