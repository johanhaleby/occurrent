/*
 * Copyright 2021 Johan Haleby
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

import java.util.OptionalLong;

/**
 * The contract for competing consumer strategies. A competing consumer strategy is used with a "competing consumer subscription model" to allow for
 * <a href="https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html">competing consumers</a> (i.e. concurrent message processing).
 * The purpose of a {@code CompetingConsumerStrategy} is to implement an algorithm or use some underlying infrastructure that makes sure that only one subscriber
 * reads events from a particular subscription. I.e. multiple subscribers can subscribe to the same subscription, but only one of them will receive a particular event.
 * If one subscriber crashes, the {@code CompetingConsumerStrategy} implementation, will notify the {@code CompetingConsumerSubscriptionModel} (which implements the
 * {@link CompetingConsumerListener} interface) that another subscriber may take over. This is typically done by (distributed) leader election.
 */
@NullMarked
public interface CompetingConsumerStrategy {

    /**
     * Register a new competing consumer that will be able to receive events (given that the conditions maintained by the {@code CompetingConsumerStrategy} allow for it).
     *
     * @param subscriptionId The subscription if to consume from
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the registered competing consumer has access (lock) to consume events, <code>false</code> otherwise.
     */
    boolean registerCompetingConsumer(String subscriptionId, String subscriberId);

    /**
     * Unregister a competing consumer, it'll no longer receive events. If this competing consumer currently has the lock to
     * receive events, the lock is handed to another subscriber for the same subscription, if there is one.
     * <p>
     * The strategy forgets the consumer, so it takes another {@link #registerCompetingConsumer(String, String)} to bring
     * it back. It will not acquire the lock again on its own, however long it waits and whether or not anybody else takes
     * the lock in the meantime. Use this when something has to happen before the consumer may consume again, the way a
     * subscription paused by a user needs an explicit resume. Where the consumer should come back by itself, use
     * {@link #releaseCompetingConsumer(String, String)} instead.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     */
    void unregisterCompetingConsumer(String subscriptionId, String subscriberId);


    /**
     * Release a competing consumer, it'll no longer receive events. If this competing consumer currently has the lock to
     * receive events, the lock becomes available to the subscribers competing for the same subscription.
     * <p>
     * The consumer stays registered, so it remains one of those subscribers and the strategy may grant it the lock again
     * on its own, with nobody calling {@link #registerCompetingConsumer(String, String)} a second time. This is the
     * weaker of the two ways to give a lock up, and the one to use when the consumer should come back by itself, the way
     * a subscription paused because a rival took the lock does. Where the consumer must not consume again until
     * something explicitly says so, use {@link #unregisterCompetingConsumer(String, String)} instead, which hands the
     * lock to a waiting subscriber rather than leaving this one competing for it.
     * <p>
     * Two things follow, and they are the difference between the two methods rather than wording. The subscriber that
     * released does not hold the lock from here on, so {@link #hasLock(String, String)} answers {@code false} for it
     * until it is granted the lock again. And the lock does not stay unheld: a subscriber competing for this
     * subscription ends up with it, which may be the one that released it if it wins the competition again.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     */
    void releaseCompetingConsumer(String subscriptionId, String subscriberId);

    /**
     * Check whether a particular subscriber has the lock (access) to read events for the given subscription.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the subscriber has the lock, <code>false</code> otherwise.
     */
    boolean hasLock(String subscriptionId, String subscriberId);

    /**
     * The current fencing token for the given subscription, a number a {@code CheckpointStorage} can compare
     * against a stored one to refuse a write from a lease that has moved on (see ADR 116). The value increases on
     * every genuine change of owner and is unchanged when the same holder refreshes. Empty means this node does
     * not believe it holds the lock, or this strategy has no token to give. The call must not block and must not
     * reach a database, since it runs on the per-event write path. An implementation that does not override this
     * method is not broken, it simply has no fence.
     *
     * @param subscriptionId The id of the subscription to get the fencing token for
     * @return The fencing token, or {@link OptionalLong#empty()} if this strategy has none to give
     */
    default OptionalLong fencingToken(String subscriptionId) {
        return OptionalLong.empty();
    }

    /**
     * Add a {@link CompetingConsumerListener} to this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to add.
     */
    void addListener(CompetingConsumerListener listenerConsumer);

    /**
     * Remove a {@link CompetingConsumerListener} from this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to remove.
     */
    void removeListener(CompetingConsumerListener listenerConsumer);

    /**
     * Perform some cleanup when shutting down the {@link CompetingConsumerStrategy}.
     */
    default void shutdown() {
    }

    /**
     * A {@code CompetingConsumerListener} will be called when certain life-cycle events occurs.
     * <p>
     * It is told what changed rather than what is currently true, so a subscriber is told once each time the answer for
     * it moves and not once per round of whatever coordination the strategy runs. A listener may therefore treat every
     * call as something to act on. Registering a listener is optional: a consumer that would rather ask than be told
     * uses {@link CompetingConsumerStrategy#hasLock(String, String)}, which answers the same thing.
     */
    interface CompetingConsumerListener {

        /**
         * Called when the lock has been granted to the given subscriber for the given subscription. This
         * means that the subscriber has access to consume events.
         *
         * @param subscriptionId The subscription id
         * @param subscriberId   The subscriber id
         */
        default void onConsumeGranted(String subscriptionId, String subscriberId) {
        }

        /**
         * Called when the lock is no longer available for the given subscriber for the given subscription. This
         * means that the subscriber no longer has access to consume events.
         *
         * @param subscriptionId The subscription id
         * @param subscriberId   The subscriber id
         */
        default void onConsumeProhibited(String subscriptionId, String subscriberId) {
        }
    }
}