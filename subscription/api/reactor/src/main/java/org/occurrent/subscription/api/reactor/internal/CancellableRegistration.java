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
 * id in that model again once the subscribe has returned.
 */
@NullMarked
public interface CancellableRegistration {

    /**
     * Subscribes as {@code SubscriptionModel.subscribe(subscriptionId, filter, startAt, action)} does, unless
     * {@code cancelled} answers {@code true}. The model asks {@code cancelled} while it holds the lock it registers the
     * subscription with, and its {@code cancelSubscription(subscriptionId)} takes that lock too. So a caller that makes
     * {@code cancelled} answer {@code true} before it calls {@code cancelSubscription(subscriptionId)} has that cancel
     * find the subscription, or has nothing registered.
     *
     * @param cancelled Whether the caller cancelled this subscribe.
     * @return The subscription, or {@code null} when {@code cancelled} answered {@code true} and nothing was registered.
     */
    @Nullable Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                     Function<CloudEvent, Mono<Void>> action, BooleanSupplier cancelled);
}
