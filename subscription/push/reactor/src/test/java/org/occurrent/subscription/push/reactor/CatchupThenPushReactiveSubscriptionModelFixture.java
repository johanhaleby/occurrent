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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.tck.subscription.reactor.ReactiveSubscriptionModelConformance;
import org.occurrent.tck.subscription.reactor.ReactiveSubscriptionModelFixture;

import java.util.List;

/**
 * The reactive-only counterpart of {@link CatchupThenPushSubscriptionModelFixture}, feeding
 * {@link ReactiveSubscriptionModelConformance} the reactor {@link CatchupThenPushSubscriptionModel} directly rather
 * than through the blocking bridge.
 */
class CatchupThenPushReactiveSubscriptionModelFixture implements ReactiveSubscriptionModelFixture {

    private final InMemoryReactivePositionOrderedReader reader = new InMemoryReactivePositionOrderedReader();
    private final PushSubscriptionModel liveFeed = new PushSubscriptionModel();
    private final CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, liveFeed, null);

    @Override
    public SubscriptionModel subscriptionModel() {
        return model;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        reader.append(events);
        liveFeed.accept(events).block();
    }
}
