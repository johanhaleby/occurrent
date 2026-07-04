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
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
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
public class ReactorCatchupSubscriptionModel implements PositionAwareSubscriptionModel {

    private final @Nullable ReactorStreamCatchupSubscriptionModel streamCatchupSubscriptionModel;
    private final @Nullable ReactorDcbCatchupSubscriptionModel dcbCatchupSubscriptionModel;

    /**
     * Create a stream-only instance. Every subscription routes to the stream catch-up model.
     */
    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, ReactorStreamCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorStreamCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), positionOrderedReader, defaultFilter, windowSize, handoverCacheSize);
        this.dcbCatchupSubscriptionModel = null;
    }

    /**
     * Create a DCB-only instance. Every subscription routes to the DCB catch-up model.
     */
    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery) {
        this(subscriptionModel, dcbEventStore, defaultQuery, ReactorDcbCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorDcbCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery, long windowSize, int handoverCacheSize) {
        this.streamCatchupSubscriptionModel = null;
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null"), dcbEventStore, defaultQuery, windowSize, handoverCacheSize);
    }

    /**
     * Create a dual-mode instance that catches up both stream subscriptions (by {@code position}, over
     * {@code positionOrderedReader}) and DCB subscriptions (by {@code position}, over {@code dcbEventStore}). Each
     * subscription is routed by its filter and start position, so a single model serves an application that uses
     * both streams and DCB.
     */
    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.streamCatchupSubscriptionModel = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, positionOrderedReader, defaultFilter, windowSize, handoverCacheSize);
        this.dcbCatchupSubscriptionModel = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, dcbEventStore, defaultQuery, windowSize, handoverCacheSize);
    }

    public ReactorCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, dcbEventStore, defaultQuery, defaultFilter, ReactorStreamCatchupSubscriptionModel.DEFAULT_POSITION_WINDOW_SIZE, ReactorStreamCatchupSubscriptionModel.DEFAULT_HANDOVER_CACHE_SIZE);
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        return routesToDcb(filter, startAt)
                ? requireNonNull(dcbCatchupSubscriptionModel).subscribe(filter, startAt)
                : requireNonNull(streamCatchupSubscriptionModel).subscribe(filter, startAt);
    }

    // Route to the DCB path or the stream path. A single-mode model has only one inner model and always routes there.
    // A dual-mode model routes by filter type first, since a global position start is ambiguous. Stream and DCB replay
    // both use a GlobalSubscriptionPosition, so only the filter tells them apart. A null filter falls back to the
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
        if (filter instanceof OccurrentSubscriptionFilter) {
            return false;
        }
        return startsAtExplicitDcbPosition(startAt);
    }

    private static boolean startsAtExplicitDcbPosition(StartAt startAt) {
        StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(ReactorCatchupSubscriptionModel.class));
        return resolved instanceof StartAtSubscriptionPosition position
                && GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position.subscriptionPosition);
    }

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        // Both inner models delegate globalSubscriptionPosition() to the same wrapped subscriptionModel, so either
        // one reports the identical position. In dual mode, ask whichever is present; there is no dcb-vs-stream
        // ambiguity here because this is the wrapped live model's position, not a catch-up cursor.
        return dcbCatchupSubscriptionModel != null
                ? dcbCatchupSubscriptionModel.globalSubscriptionPosition()
                : requireNonNull(streamCatchupSubscriptionModel).globalSubscriptionPosition();
    }
}
