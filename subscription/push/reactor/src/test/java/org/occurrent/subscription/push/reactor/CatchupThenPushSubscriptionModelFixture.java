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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.StartAtVariant;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.reactor.BlockingSubscriptionOverReactive;

import java.util.List;
import java.util.Set;

/**
 * Feeds {@link CatchupThenPushSubscriptionModel} through {@link InMemoryReactivePositionOrderedReader}, the way the
 * blocking {@code CatchupThenPushSubscriptionModelTest} feeds its twin through an {@code InMemoryEventStore} whose
 * write listener forwards to the live feed: {@link #publish(List)} does both halves itself since the reader and the
 * live feed are separate constructor arguments here.
 * <p>
 * Every declaration below is derived from this model's own source rather than copied from a sibling, and the blocking
 * twin's fixture is derived the same way from the blocking model.
 * <p>
 * This fixture now backs the general {@code SubscriptionModelConformance} as well as the introspection and
 * reactive-only suites. Phase 7 held that wiring back because the suite assumed a fresh subscription starts at the
 * present, which this model contradicts by contract, and there was no way to say so. The two declarations that say it
 * are below.
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
     * Only reached for the refusal below, since {@code CHECKPOINT} is not an accepted variant here, and any checkpoint
     * is as good as another for a model that rejects the whole variant before looking at the value.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return GlobalCheckpoint.of(0);
    }

    /**
     * The one model in Occurrent that refuses a start position, and it refuses three of the four variants. It replays a
     * whole history and then hands over to a live feed, so there is nothing for a caller's position to apply to, and
     * {@code subscribe} rejects anything but the default rather than accepting a position it would then ignore. The
     * refusal is on the variant rather than on what the variant resolves to, which is why a {@code Dynamic} resolving
     * to the default is refused as well.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return Set.of(StartAtVariant.SUBSCRIPTION_MODEL_DEFAULT);
    }

    /**
     * The other half of the same contract, and the reason phase 7 could not wire the general suite here. A subscription
     * id this model has not seen before is replayed the whole history from the reader before it goes live, which is
     * what the model is for: a read model built by an application that was not running when the events were written.
     */
    @Override
    public boolean replaysHistoryToANewSubscription() {
        return true;
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
