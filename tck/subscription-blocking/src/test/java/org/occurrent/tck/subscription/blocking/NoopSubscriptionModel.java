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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.Set;
import java.util.function.Consumer;

/**
 * A subscription model that honours none of the contract. Run a suite against it and every single test must fail.
 * <p>
 * Every method throws rather than answering emptily, and that matters more than it looks. A model whose
 * {@code subscribe} handed back a started-looking subscription and then delivered nothing would pass the lifecycle
 * assertions that only ask about state, and a model whose {@code isRunning} answered {@code false} would pass the two
 * tests about a subscription nobody started.
 */
class NoopSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, CheckpointAwareSubscriptionModel {

    static final NoopSubscriptionModel INSTANCE = new NoopSubscriptionModel();

    private NoopSubscriptionModel() {
    }

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        // Throws rather than answering null. Null is a documented answer, so a null here would let the suite take its
        // "cannot seed a catch-up handover" branch and pass, which is the opposite of what this model is for.
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public boolean isRunning() {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }

    @Override
    public Set<String> subscriptionIds() {
        throw new UnsupportedOperationException("NoopSubscriptionModel implements nothing on purpose");
    }
}
