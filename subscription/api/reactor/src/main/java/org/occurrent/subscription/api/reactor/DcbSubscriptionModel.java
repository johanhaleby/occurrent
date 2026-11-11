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
 * to the underlying {@link FluxSubscriptionModel}, so what a start does depends on that model.
 * <p>
 * A model composed with {@code ReactorDcbCatchupSubscriptionModel} replays history from {@link DcbStartAt#beginning()},
 * or from a checkpoint a replayed event delivered, before going live.
 * <p>
 * {@code ReactorMongoSubscriptionModel} has no DCB catch-up. It refuses a start at a DCB position, such as
 * {@link DcbStartAt#beginning()}, and a checkpoint a replayed event delivered, with an
 * {@link org.occurrent.subscription.UnsupportedStartAtException}, because neither is a position in its change stream.
 * The {@code Flux}-returning {@code subscribe} methods deliver the exception as an error on the {@code Flux}, and the
 * named {@code subscribe} methods throw it.
 * <p>
 * A {@link DcbStartAt#dynamic(Function) dynamic} start is refused too when its function returns one of these before
 * the change stream of the subscription has opened, which the {@code Flux}-returning methods count from the
 * {@code Flux} being subscribed to. The model calls the function as the change stream opens, so the
 * {@code Flux}-returning methods fail their {@code Flux} with the exception. A named {@code subscribe} returns normally
 * and then stops the subscription with it, and {@link Subscription#waitUntilStarted()} fails with it, except on the
 * handle a {@code subscribe} returns while the model is stopped, which never completes. A handle that has answered that
 * it started keeps that answer. It answers once the change stream is subscribed to, so a refusal on a retry after a
 * failed opening stops the subscription and only logs. A start the function returns after the change stream has opened,
 * before the first event, is not refused.
 * <p>
 * A checkpoint taken from an event that model delivered live works, and so do {@link DcbStartAt#now()} and
 * {@link DcbStartAt#subscriptionModelDefault()}. A checkpoint string the model does not recognise is not refused, and
 * it opens at the present.
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
    Subscription subscribe(String subscriptionId, DcbCriteria criteria, DcbStartAt startAt, Function<CloudEvent, Mono<Void>> action);

    /**
     * Subscribe to DCB events matching {@code criteria} at the subscription model default position, tracked by
     * {@code subscriptionId}.
     *
     * @see #subscribe(String, DcbCriteria, DcbStartAt, Function)
     */
    default Subscription subscribe(String subscriptionId, DcbCriteria criteria, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, criteria, DcbStartAt.subscriptionModelDefault(), action);
    }

    /**
     * Cancel a named DCB subscription started with {@link #subscribe(String, DcbCriteria, DcbStartAt, Function)} and
     * forget it. Cancelling a subscription id that is unknown or already cancelled stops nothing, and still deletes
     * what a store holds for that id.
     * <p>
     * Behaves as {@link CancellableSubscriptions#cancelSubscription(String)} does. The cancel takes effect when this
     * method is called, and the returned {@code Mono} completes once the underlying model has deleted what it stored
     * for this id.
     */
    Mono<Void> cancelSubscription(String subscriptionId);

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
