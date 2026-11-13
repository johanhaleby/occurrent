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

package org.occurrent.subscription.api.reactor.internal;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscription;
import reactor.core.publisher.Mono;

import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Internal, not part of the public API. Implemented by a reactor subscription model that manages named subscriptions,
 * so a subscribe that a reactor catch-up cancelled before this model registered it registers nothing. The catch-up
 * calls this model's {@code subscribe(..)} with none of its own locks held, so its cancel of the id can reach this
 * model first, when there is nothing here to cancel yet.
 * <p>
 * A catch-up over a model that doesn't implement this interface lets the cancelled subscribe register, and cancels the
 * id in that model again once the subscribe has returned. So does a catch-up over a subclass of a model that
 * implements this interface, or over a proxy of one, see {@link #standsInForSubscribe(Object)}.
 */
@NullMarked
public interface CancellableRegistration {

    /**
     * Subscribes as {@code SubscriptionModel.subscribe(subscriptionId, filter, startAt, action)} does, unless
     * {@code cancelled} answers {@code true}. The model asks {@code cancelled} while it holds the lock it registers the
     * subscription with, and its {@code cancelSubscription(subscriptionId)} takes that lock too. So a caller that makes
     * {@code cancelled} answer {@code true} before it calls {@code cancelSubscription(subscriptionId)} has that cancel
     * find the subscription, or has nothing registered. Where {@code cancelled} answers {@code true} on a model that is
     * shut down by then, the model throws {@code SubscriptionModelShutdownException} instead of returning {@code null}.
     *
     * @param cancelled Whether the caller cancelled this subscribe.
     * @return The subscription, or {@code null} when {@code cancelled} answered {@code true} on a model that isn't shut
     * down and nothing was registered.
     */
    @Nullable Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                     Function<CloudEvent, Mono<Void>> action, BooleanSupplier cancelled);

    /**
     * Whether a caller can call the {@code subscribe(..)} of this interface in place of
     * {@code subscribe(String, SubscriptionFilter, StartAt, Function)} on the reference it holds. The caller passes that
     * reference as {@code asSeenByCaller}. The model answers {@code true} only when {@code asSeenByCaller} is this
     * instance and this instance is of the model's own class.
     * <p>
     * A subclass can override {@code subscribe(String, SubscriptionFilter, StartAt, Function)}, and a proxy can advise
     * it. Calling the {@code subscribe(..)} of this interface would skip that override or that advice. So a subclass
     * that doesn't override this method answers {@code false}, since its class isn't the model's own class. A proxy that
     * forwards this call to the model it proxies gets {@code false} from that model, since {@code asSeenByCaller} is the
     * proxy. A Mockito spy made with Mockito's default inline mock maker answers {@code true}. That mock maker changes
     * the model's own class instead of making a subclass, and the spy is a new instance of that class with the fields of
     * the model it spies on copied into it. So a stub on the spy's
     * {@code subscribe(String, SubscriptionFilter, StartAt, Function)} doesn't run for a caller that holds the spy. A
     * caller that gets {@code false} calls {@code subscribe(String, SubscriptionFilter, StartAt, Function)}, and the
     * model gets the cleanup a model that doesn't implement this interface gets.
     *
     * @param asSeenByCaller The reference the caller calls this model's {@code subscribe(..)} through.
     * @return {@code true} when the {@code subscribe(..)} of this interface can be called on {@code asSeenByCaller} in
     * place of the one of {@code SubscriptionModel}, {@code false} unless the model overrides this method.
     */
    default boolean standsInForSubscribe(Object asSeenByCaller) {
        return false;
    }
}
