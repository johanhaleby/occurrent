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
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

/**
 * The general reactive catch-up entry point. It routes each subscription to stream or DCB replay by the subscription
 * filter and start position, dispatching over {@link ReactorStreamCatchupSubscriptionModel} (stream catch-up) and
 * {@code ReactorDcbCatchupSubscriptionModel} (DCB catch-up), so a single model serves an application that uses streams,
 * DCB, or both. Mirrors the routing of the blocking {@code CatchupSubscriptionModel}. A stream-only store that wants to
 * avoid the DCB dependency can use {@link ReactorStreamCatchupSubscriptionModel} directly as the DCB-free variant.
 */
@NullMarked
public class ReactorCatchupSubscriptionModel implements CheckpointAwareSubscriptionModel {

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
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), positionOrderedReader, defaultFilter, windowSize, handoverCacheSize);
        this.dcbCatchupSubscriptionModel = null;
        this.agnosticCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, null);
    }

    /**
     * Create a DCB-only instance. Every subscription routes to the DCB catch-up model.
     */
    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery) {
        this(subscriptionModel, dcbEventStore, defaultQuery, ReactorDcbCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorDcbCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbCriteria defaultQuery, long windowSize, int handoverCacheSize) {
        this.streamCatchupSubscriptionModel = null;
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(requireNonNull(subscriptionModel, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), dcbEventStore, defaultQuery, windowSize, handoverCacheSize);
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
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize);
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, dcbEventStore, defaultQuery, windowSize, handoverCacheSize);
        this.agnosticCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize, null);
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
}
