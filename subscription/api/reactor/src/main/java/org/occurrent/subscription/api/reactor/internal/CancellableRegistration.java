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
 * id in that model again once the subscribe has returned. So does a catch-up over a subclass that overrides
 * {@code subscribe(String, SubscriptionFilter, StartAt, Function)}, see
 * {@link #standsInForSubscribe(CancellableRegistration)}.
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
     * Whether no subclass of the topmost class of {@code model} that implements this interface overrides
     * {@code subscribe(String, SubscriptionFilter, StartAt, Function)}, so calling the {@code subscribe(..)} of this
     * interface skips no override. That topmost class subscribes in both {@code subscribe(..)} methods the same way, as
     * this interface asks. It answers {@code false} when a subclass overrides that {@code subscribe(..)}, also when the
     * subclass declares this interface again or overrides the {@code subscribe(..)} of this interface too. A caller then
     * calls the override, and the subclass keeps the cleanup a model that doesn't implement this interface gets. A
     * subclass that overrides only the {@code subscribe(..)} of this interface answers {@code true}.
     *
     * @param model The model to call.
     * @return {@code true} when the {@code subscribe(..)} of this interface can be called in place of the one of
     * {@code SubscriptionModel}.
     */
    static boolean standsInForSubscribe(CancellableRegistration model) {
        Class<?> declaringSubscribe;
        try {
            declaringSubscribe = model.getClass().getMethod("subscribe", String.class, SubscriptionFilter.class, StartAt.class, Function.class).getDeclaringClass();
        } catch (NoSuchMethodException e) {
            return false;
        }
        Class<?> topmostImplementing = model.getClass();
        while (topmostImplementing.getSuperclass() != null && CancellableRegistration.class.isAssignableFrom(topmostImplementing.getSuperclass())) {
            topmostImplementing = topmostImplementing.getSuperclass();
        }
        return declaringSubscribe.isAssignableFrom(topmostImplementing);
    }
}
