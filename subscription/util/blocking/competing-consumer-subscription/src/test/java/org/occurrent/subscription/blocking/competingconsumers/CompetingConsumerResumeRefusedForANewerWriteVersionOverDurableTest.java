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

/**
 * A node that is granted the lease back finds, when its durable model resumes the paused subscription at the stored
 * checkpoint, that the storage holds a newer write version than its own, since the node that stored it holds a newer
 * lease. The durable model refuses the resume with the subscription still paused, and the competing consumer model
 * gives the lease back and waits for a later grant, which resumes the subscription once the newer version is gone.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerResumeRefusedForANewerWriteVersionOverDurableTest {

    private static final String NODE = "this-node";
    private static final String ID = "subscription";
    private static final Checkpoint OTHER_NODES_POSITION = new StringBasedCheckpoint("other-nodes-position");
    private static final long VERSION_OF_THIS_NODES_LEASE = 2;
    private static final long OTHER_NODES_VERSION = 3;

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
    void a_grant_whose_resume_the_durable_model_refuses_for_a_newer_write_version_stored_gives_the_lease_back_and_keeps_the_consumer_a_candidate() {
        // Given a node that lost the lease, while another node stored a position with a newer version
        aSubscriptionThatLostItsLeaseWhileAnotherNodeStored(OTHER_NODES_VERSION);

        // When
        Throwable thrown = catchThrowable(strategy::grantTheLeaseToThisNode);

        // Then
        await().untilAsserted(() -> assertThat(strategy.holders).as("holders of the lease").isEmpty());
        assertThat(thrown).as("thrown when the lease is granted").isNull();
        assertThat(live.plainResumes).as("plain resumes of the wrapped model").hasValue(0);
        assertThat(live.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty();
        assertThat(live.isPaused(ID)).as("paused in the wrapped model").isTrue();
        assertThat(strategy.registrations).as("registered as a candidate for the lease").containsExactly(ID);
        assertThat(storage.read(ID)).as("stored position").isEqualTo(OTHER_NODES_POSITION);
        assertThat(storage.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION);
    }

    @Test
    void a_later_grant_resumes_the_subscription_at_the_stored_position_with_this_nodes_version_once_the_newer_version_is_gone() {
        // Given a grant the durable model refused for a newer version
        aSubscriptionThatLostItsLeaseWhileAnotherNodeStored(OTHER_NODES_VERSION);
        strategy.grantTheLeaseToThisNode();
        await().untilAsserted(() -> assertThat(strategy.holders).as("holders of the lease after the refused grant").isEmpty());
        // and the node that stored the newer version giving up, which leaves a position stored with an older one
        storage.delete(ID);
        storage.save(ID, OTHER_NODES_POSITION, CheckpointWriteCondition.notOlderThan(1));

        // When
        strategy.grantTheLeaseToThisNode();

        // Then
        await().untilAsserted(() -> assertThat(live.isPaused(ID)).as("paused in the wrapped model").isFalse());
        Throwable saveOfAnOlderLease = catchThrowable(() -> storage.save(ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertThat(live.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(OTHER_NODES_POSITION);
        assertThat(storage.writeVersion(ID)).as("stored version").hasValue(VERSION_OF_THIS_NODES_LEASE);
        assertThat(saveOfAnOlderLease).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
        assertThat(strategy.holders).as("holders of the lease").containsExactly(ID);
    }

    private void aSubscriptionThatLostItsLeaseWhileAnotherNodeStored(long version) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage, id -> OptionalLong.of(VERSION_OF_THIS_NODES_LEASE));
        model = new CompetingConsumerSubscriptionModel(durable, strategy);
        strategy.grantOnRegister = true;
        model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        assertThat(live.isRunning(ID)).as("running in the wrapped model after the subscribe").isTrue();
        strategy.grantOnRegister = false;
        strategy.loseTheLease();
        await().untilAsserted(() -> assertThat(live.isPaused(ID)).as("paused in the wrapped model after the lease was lost").isTrue());
        storage.delete(ID);
        storage.save(ID, OTHER_NODES_POSITION, CheckpointWriteCondition.notOlderThan(version));
    }

    // Grants the lease on register when told to, and otherwise leaves the consumer waiting for a grant the test gives
    private final class Strategy implements CompetingConsumerStrategy {
        final Set<String> holders = ConcurrentHashMap.newKeySet();
        final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        volatile boolean grantOnRegister;

        // What the strategy does when it hands the lease to this node, on a thread of its own in production
        void grantTheLeaseToThisNode() {
            holders.add(ID);
            model.onConsumeGranted(ID, NODE);
        }

        void loseTheLease() {
            holders.remove(ID);
            model.onConsumeProhibited(ID, NODE);
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.remove(subscriptionId);
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

    // A live model that can be paused and resumed, at a position or from its own, and restarts at any checkpoint
    private static final class Live implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        final List<Checkpoint> resumedAt = new CopyOnWriteArrayList<>();
        final AtomicInteger plainResumes = new AtomicInteger();
        private final Map<String, Consumer<CloudEvent>> subscribed = new ConcurrentHashMap<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
                startAt.get(new StartAt.SubscriptionModelContext(Live.class));
            }
            subscribed.put(subscriptionId, action);
            return subscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("first-position");
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
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
