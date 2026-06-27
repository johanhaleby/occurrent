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

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * Shared base for the typed subscription-model facades. It holds the wrapped {@link SubscriptionModel} and forwards
 * every life-cycle call to it, so each facade adapter only has to implement its own typed {@code subscribe}.
 */
@NullMarked
abstract class AbstractDelegatingSubscriptionModelAdapter implements SubscriptionModelLifeCycle {

    final SubscriptionModel delegate;

    AbstractDelegatingSubscriptionModelAdapter(SubscriptionModel delegate) {
        this.delegate = requireNonNull(delegate, SubscriptionModel.class.getSimpleName() + " cannot be null");
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        delegate.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return delegate.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return delegate.isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return delegate.resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        delegate.pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        delegate.cancelSubscription(subscriptionId);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
