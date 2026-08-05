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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.reactor.BlockingSubscriptionOverReactive;

import java.util.List;

/**
 * Shared by {@link SynchronousSubscriptionModelConformanceTest}, {@link SynchronousSubscriptionModelIntrospectionConformanceTest}
 * and {@link SynchronousSubscriptionModelInProcessConformanceTest}, mirroring the blocking
 * {@code SynchronousSubscriptionModelFixture}.
 */
class SynchronousSubscriptionModelFixture implements SubscriptionModelFixture {

    private final SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();

    @Override
    public SubscriptionModel subscriptionModel() {
        return BlockingSubscriptionOverReactive.of(model);
    }

    @Override
    public void publish(List<CloudEvent> events) {
        model.dispatch(events).block();
    }

    /**
     * {@code RegisteringSubscribable.route} skips a paused subscription (dropped, not deferred), so an event
     * published while paused never reaches that handler at all.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return false;
    }

    /**
     * {@code dispatch(List)} routes through {@code route(..)}, whose unguarded {@code concatMap} propagates the
     * first handler error straight back out of the returned {@code Mono}, which {@link #publish(List)} blocks on, so
     * the exception reaches the caller rather than being retried.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return false;
    }
}
