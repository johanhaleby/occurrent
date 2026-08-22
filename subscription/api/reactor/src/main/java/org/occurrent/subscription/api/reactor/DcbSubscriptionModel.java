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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.DcbStartAt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * A typed reactive view over a {@link FluxSubscriptionModel} that subscribes to DCB events selected by a {@link DcbCriteria}.
 * <p>
 * It is the DCB counterpart to the reactive {@link FluxSubscriptionModel}, accepting a {@link DcbCriteria} and a
 * {@link DcbStartAt} rather than a stream filter and a generic start position. The {@link DcbStartAt} is passed through
 * to the underlying {@link FluxSubscriptionModel}, so whether a replay-oriented start such as {@link DcbStartAt#beginning()}
 * or {@link DcbStartAt#afterPosition(long)} replays history depends on that model. A plain model such as
 * {@code ReactorMongoSubscriptionModel} has no DCB catch-up and treats such a start as live, whereas a model composed
 * with {@code ReactorDcbCatchupSubscriptionModel} replays history from that position before going live.
 * <p>
 * The {@code subscribe(criteria)}/{@code subscribe(criteria, startAt)} methods above return a bare {@link Flux}: the
 * subscription lives as long as something is subscribed to the {@code Flux} and is cancelled by disposing that
 * subscription. The named {@link #subscribe(String, DcbCriteria, DcbStartAt, Function)} methods below are the
 * lifecycle-managed counterpart, mirroring {@link Subscribable}: they track the subscription by id so it can be
 * cancelled through {@link #cancelSubscription(String)}, which is what a durable, catch-up-capable DCB subscription
 * (such as the {@code @DcbSubscription} annotation) needs.
 */
@NullMarked
public interface DcbSubscriptionModel {

    /**
     * Subscribe to DCB events matching {@code criteria}, starting at {@code startAt}.
     *
     * @return a {@link Flux} of the matching cloud events.
     */
    Flux<CloudEvent> subscribe(DcbCriteria criteria, DcbStartAt startAt);

    /**
     * Subscribe to DCB events matching {@code criteria} at the subscription model default position.
     */
    default Flux<CloudEvent> subscribe(DcbCriteria criteria) {
        return subscribe(criteria, DcbStartAt.subscriptionModelDefault());
    }

    /**
     * Subscribe to every DCB event at the subscription model default position.
     */
    default Flux<CloudEvent> subscribe() {
        return subscribe(DcbCriteria.all());
    }

    /**
     * Subscribe to DCB events matching {@code criteria}, starting at {@code startAt}, tracked by {@code subscriptionId}
     * so it can be cancelled with {@link #cancelSubscription(String)}. Unlike the {@link Flux}-returning
     * {@code subscribe} methods above, this requires the underlying {@link FluxSubscriptionModel} to also support named,
     * lifecycle-managed subscriptions.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event matching {@code criteria}. The next event
     *                       is not processed until the returned {@link Mono} completes.
     */
    SubscriptionHandle subscribe(String subscriptionId, DcbCriteria criteria, DcbStartAt startAt, Function<CloudEvent, Mono<Void>> action);

    /**
     * Subscribe to DCB events matching {@code criteria} at the subscription model default position, tracked by
     * {@code subscriptionId}.
     *
     * @see #subscribe(String, DcbCriteria, DcbStartAt, Function)
     */
    default SubscriptionHandle subscribe(String subscriptionId, DcbCriteria criteria, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, criteria, DcbStartAt.subscriptionModelDefault(), action);
    }

    /**
     * Cancel a named DCB subscription started with {@link #subscribe(String, DcbCriteria, DcbStartAt, Function)} and
     * forget it. Cancelling a subscription id that is unknown or already cancelled is a no-op.
     */
    void cancelSubscription(String subscriptionId);

    /**
     * Create a DCB view over an existing reactive {@link FluxSubscriptionModel}. The named
     * {@link #subscribe(String, DcbCriteria, DcbStartAt, Function)} and {@link #cancelSubscription(String)} methods
     * additionally require {@code delegate} to implement {@link Subscribable} and {@link SubscriptionModelLifeCycle}
     * respectively; this is only checked when one of those methods is called, not eagerly here, since a delegate
     * that only ever uses the {@link Flux}-returning {@code subscribe} methods need not support named subscriptions.
     */
    static DcbSubscriptionModel from(FluxSubscriptionModel delegate) {
        return new DcbSubscriptionModelAdapter(delegate);
    }
}
