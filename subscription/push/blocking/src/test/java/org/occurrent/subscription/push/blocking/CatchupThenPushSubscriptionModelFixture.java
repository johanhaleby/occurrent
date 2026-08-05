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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.StartAtVariant;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Feeds {@link CatchupThenPushSubscriptionModel} the way an application would: an {@link InMemoryEventStore} is both
 * the history the replay reads and, through its write listener, the source of the live feed. That is one call rather
 * than the reactor fixture's two, because the blocking store takes the listener on its constructor.
 * <p>
 * The declarations are read off this model's own source rather than copied from the reactor twin, and they agree with
 * it, which is worth something: the two are separate implementations of one documented contract, and this is the first
 * time anything checks that they answer the same way.
 */
class CatchupThenPushSubscriptionModelFixture implements SubscriptionModelFixture {

    private final PushSubscriptionModel liveFeed = new PushSubscriptionModel();
    private final InMemoryEventStore store = new InMemoryEventStore(liveFeed::accept);
    private final CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);
    private final String streamId = UUID.randomUUID().toString();

    @Override
    public SubscriptionModel subscriptionModel() {
        return model;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        // Unconditional, so the suite never has to know a stream version. One stream per fixture keeps the events in
        // the order they were published, which is the order every assertion here is about.
        store.write(streamId, events);
    }

    /**
     * Once {@code waitUntilStarted()} has returned, the replay is over and the id has been forgotten from the replaying
     * map, so pausing goes to {@code liveFeed.pauseSubscription(..)} and lands on the same
     * {@code RegisteringSubscribable.route} as the raw push model: dropped, not deferred.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return false;
    }

    /**
     * A live event reaches the handler through the live feed's unguarded {@code route(..)}, with no retry anywhere on
     * the path, so a handler that throws throws back out of the write that fed it.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return false;
    }

    /**
     * Reached only for the refusal below, since {@code CHECKPOINT} is not accepted and the refusal is decided by the
     * variant rather than by the value.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return GlobalCheckpoint.of(0);
    }

    /**
     * This model replays a whole history and then hands over to a live feed, so a caller's start position has nothing
     * to apply to and {@code subscribe} refuses every variant but the default rather than accepting one it would
     * ignore. The refusal is on the variant, so a dynamic position that would resolve to the default is refused too.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return Set.of(StartAtVariant.SUBSCRIPTION_MODEL_DEFAULT);
    }

    /**
     * The other half of the same contract: a subscription id this model has not seen before gets the whole history
     * before it gets anything live, which is what the model exists to do.
     */
    @Override
    public boolean replaysHistoryToANewSubscription() {
        return true;
    }

    /**
     * The live feed is a {@link PushSubscriptionModel} taking one consumer, and this model registers on it, so the
     * refusal of a second subscription is inherited.
     */
    @Override
    public boolean acceptsSeveralSubscriptions() {
        return false;
    }

    @Override
    public void close() {
        model.shutdown();
    }
}
