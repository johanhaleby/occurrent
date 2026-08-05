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
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.reactor.BlockingSubscriptionOverReactive;

import java.util.List;

/**
 * Feeds {@link CatchupThenPushSubscriptionModel} through {@link InMemoryReactivePositionOrderedReader}, the way the
 * blocking {@code CatchupThenPushSubscriptionModelTest} feeds its twin through an {@code InMemoryEventStore} whose
 * write listener forwards to the live feed: {@link #publish(List)} does both halves itself since the reader and the
 * live feed are separate constructor arguments here.
 * <p>
 * No blocking wiring of this model exists to mirror (phase 6 did not wire it: see the ORCHESTRATOR TCK notes), so
 * every declaration below is derived directly from this model's own source, not copied from a sibling.
 * <p>
 * This fixture backs the introspection and reactive-only suites ONLY. The general
 * {@code SubscriptionModelConformance} is deliberately NOT wired here yet: this model's documented contract replays
 * the whole history to every NEW subscription id, so the suite's fresh-subscription-starts-now assumption fails on a
 * cancelled-then-recreated subscription for a reason that is the model's declared {@code StartAt} behaviour, not a
 * bug. Wiring it needs the declared {@code StartAt}-restriction mechanism the phase 8 wrapper suites add (#395), the
 * same reason phase 6 left the blocking wrapper models to phase 8.
 */
class CatchupThenPushSubscriptionModelFixture implements SubscriptionModelFixture {

    private final InMemoryReactivePositionOrderedReader reader = new InMemoryReactivePositionOrderedReader();
    private final PushSubscriptionModel liveFeed = new PushSubscriptionModel();
    private final CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, liveFeed, null);

    @Override
    public SubscriptionModel subscriptionModel() {
        return BlockingSubscriptionOverReactive.of(model);
    }

    @Override
    public void publish(List<CloudEvent> events) {
        reader.append(events);
        liveFeed.accept(events).block();
    }

    /**
     * By the time {@code subscribeAndWait} returns, catch-up has already finished (its replay-then-marker phase is
     * exactly what {@code waitUntilStarted()} joins) and the subscription id has been forgotten from the replaying
     * map, so a later {@code pauseSubscription} always takes the {@code liveFeed.pauseSubscription(..)} branch. That
     * delegates to the same {@code RegisteringSubscribable.route}, dropped-not-deferred, as the raw push model.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return false;
    }

    /**
     * A live event's handler runs through {@code liveFeed}'s unguarded {@code route(..)}, the same as the raw push
     * model, so a failing handler propagates straight back out of {@link #publish(List)} rather than being retried.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return false;
    }

    /**
     * The live feed is a {@link PushSubscriptionModel}, {@code Consumers.ONE}: a second {@code subscribe} is refused
     * there, and this model registers on the live feed before anything else, so it inherits that refusal.
     */
    @Override
    public boolean acceptsSeveralSubscriptions() {
        return false;
    }
}
