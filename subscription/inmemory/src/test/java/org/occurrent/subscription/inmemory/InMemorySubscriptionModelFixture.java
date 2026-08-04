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

package org.occurrent.subscription.inmemory;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.util.List;

/**
 * Shared by {@link InMemorySubscriptionModelConformanceTest} and
 * {@link InMemorySubscriptionModelIntrospectionConformanceTest}.
 */
class InMemorySubscriptionModelFixture implements SubscriptionModelFixture {

    private final InMemorySubscriptionModel model = new InMemorySubscriptionModel();

    @Override
    public SubscriptionModel subscriptionModel() {
        return model;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        model.accept(events);
    }

    /**
     * {@code accept(...)} skips a subscription that is not running (see {@link InMemorySubscriptionModel#accept(List)}),
     * so an event fed in while a subscription is paused is dropped rather than queued for later.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return false;
    }

    /**
     * Delivery is asynchronous: {@code accept(...)} queues on the caller's thread, and each subscription's own pool
     * thread runs the handler behind a {@link org.occurrent.retry.RetryStrategy}, so a throwing handler is retried
     * rather than propagated back out of {@link #publish(List)}.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    @Override
    public void close() {
        model.shutdown();
    }
}
