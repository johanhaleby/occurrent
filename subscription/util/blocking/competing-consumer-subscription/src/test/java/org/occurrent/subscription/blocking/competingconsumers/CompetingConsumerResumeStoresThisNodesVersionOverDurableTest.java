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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscription made while this model is stopped, at an explicit checkpoint that is already stored, is held paused
 * and the stored checkpoint stays as it is. The write version source answers empty then, since this node holds no
 * lease. When {@code start()} grants this node a newer lease, the plain resume of the wrapped durable model stores
 * this node's version with that stored checkpoint, so the node that stored it with an older version, and may still
 * have a save on its way, is refused before this node has saved a checkpoint of its own.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerResumeStoresThisNodesVersionOverDurableTest {

    private static final String NODE = "this-node";
    private static final String ID = "subscription";
    private static final Checkpoint STORED_POSITION = new StringBasedCheckpoint("stored-position");
    private static final long VERSION_OF_THE_OLD_HOLDERS_LEASE = 1;
    private static final long VERSION_OF_THIS_NODES_LEASE = 2;

    private final Strategy strategy = new Strategy();
    private final Live live = new Live();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private CompetingConsumerSubscriptionModel model;

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
    }

    @Test
    void a_save_by_the_old_holder_with_its_older_version_is_refused_after_the_start_resumes_a_subscription_made_while_stopped_at_the_checkpoint_the_old_holder_stored() {
        // Given a checkpoint the old holder stored, and a subscription made at it while this model is stopped
        storage.save(ID, STORED_POSITION, CheckpointWriteCondition.notOlderThan(VERSION_OF_THE_OLD_HOLDERS_LEASE));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage, strategy::versionOfThisNodesLease);
        model = new CompetingConsumerSubscriptionModel(durable, strategy);
        model.stop();
        model.subscribe(NODE, ID, null, StartAt.checkpoint(STORED_POSITION), cloudEvent -> {
        });
        assertThat(live.isPaused(ID)).as("paused in the wrapped model while this model is stopped").isTrue();
        assertThat(storage.writeVersion(ID)).as("stored version before the start").hasValue(VERSION_OF_THE_OLD_HOLDERS_LEASE);

        // When the start grants this node a newer lease, and the old holder's delayed save comes before any handler of
        // this node has run
        strategy.grantOnRegister = true;
        model.start();
        await().untilAsserted(() -> assertThat(live.isPaused(ID)).as("paused in the wrapped model after the start").isFalse());
        Throwable saveOfTheOldHolder = catchThrowable(() -> storage.save(ID, new StringBasedCheckpoint("old-holders-position"), CheckpointWriteCondition.notOlderThan(VERSION_OF_THE_OLD_HOLDERS_LEASE)));

        // Then
        assertAll(
                () -> assertThat(saveOfTheOldHolder).as("save with the old holder's version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(STORED_POSITION),
                () -> assertThat(storage.writeVersion(ID)).as("stored version").hasValue(VERSION_OF_THIS_NODES_LEASE),
                () -> assertThat(live.plainResumes).as("plain resumes of the wrapped model").hasValue(1),
                () -> assertThat(live.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty()
        );
    }

    // Grants the lease on register when told to, and otherwise keeps the consumer waiting for a grant the test gives
    private static final class Strategy implements CompetingConsumerStrategy {
        final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        volatile boolean grantOnRegister;

        // The version of the lease this node holds, and none while it holds no lease
        OptionalLong versionOfThisNodesLease(String subscriptionId) {
            return holders.contains(subscriptionId) ? OptionalLong.of(VERSION_OF_THIS_NODES_LEASE) : OptionalLong.empty();
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }
    }

    // A live model that holds a subscription made while it is stopped paused, and can be resumed at a position or from
    // its own, and restarts at any checkpoint
    private static final class Live implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        final List<Checkpoint> resumedAt = new CopyOnWriteArrayList<>();
        final AtomicInteger plainResumes = new AtomicInteger();
        private final Map<String, Consumer<CloudEvent>> subscribed = new ConcurrentHashMap<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private volatile boolean stopped;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
                startAt.get(new StartAt.SubscriptionModelContext(Live.class));
            }
            subscribed.put(subscriptionId, action);
            if (stopped) {
                paused.add(subscriptionId);
            }
            return subscription(subscriptionId);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribe(subscriptionId, filter, startAt, action);
            paused.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("first-position");
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            stopped = false;
        }

        @Override
        public boolean isRunning() {
            return !stopped;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.containsKey(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            paused.remove(subscriptionId);
            plainResumes.incrementAndGet();
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            paused.remove(subscriptionId);
            resumedAt.add(((StartAt.StartAtCheckpoint) startAt).checkpoint);
            return subscription(subscriptionId);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
