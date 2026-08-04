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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.util.List;

/**
 * Shared by {@link PushSubscriptionModelConformanceTest}, {@link PushSubscriptionModelIntrospectionConformanceTest}
 * and {@link PushSubscriptionModelInProcessConformanceTest}.
 */
class PushSubscriptionModelFixture implements SubscriptionModelFixture {

    private final PushSubscriptionModel model = new PushSubscriptionModel();

    @Override
    public SubscriptionModel subscriptionModel() {
        return model;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        model.accept((Iterable<CloudEvent>) events);
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
     * Dispatch happens on the calling thread, so a throwing handler's exception propagates straight back out of
     * {@link #publish(List)} rather than being retried.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return false;
    }

    /**
     * {@link PushSubscriptionModel} passes {@code Consumers.ONE}: a push sink has one broker acknowledgement per
     * message, so a second subscription is refused rather than sharing that acknowledgement between two handlers.
     */
    @Override
    public boolean acceptsSeveralSubscriptions() {
        return false;
    }
}
