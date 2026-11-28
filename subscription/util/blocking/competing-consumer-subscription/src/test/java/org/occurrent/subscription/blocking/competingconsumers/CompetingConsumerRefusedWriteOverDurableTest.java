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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A node that won the lease and finds its checkpoint storage holding a newer write version than its own lease is not
 * the holder any more, since another node took the lease and stored that version. The durable model refuses the
 * subscribe with the storage's exception and writes nothing. The competing consumer model over it keeps the
 * subscription, gives the lease back and waits for a grant, and {@code subscribe} returns instead of throwing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerRefusedWriteOverDurableTest {

    private static final String NODE = "this-node";
    private static final String ID = "subscription";
    private static final String OTHER_NODES_POSITION = "other-nodes-position";
    private static final long OTHER_NODES_VERSION = 3;

    private final AtomicLong versionOfThisNodesLease = new AtomicLong(2);
    private final Strategy strategy = new Strategy();
    private final Live live = new Live();
    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
    private RaceStorage storage;
    private CompetingConsumerSubscriptionModel model;

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
    }

    @Test
    void a_subscribe_whose_wrapped_model_refuses_it_for_a_newer_write_version_stored_returns_without_throwing_and_gives_the_lease_back_but_stays_a_candidate() {
        // Given another node stored a position with a newer version after this node won the lease and read its position
        aModelWhereAnotherNodeStoresAPositionWithANewerVersionJustBeforeThisNodeSettlesItsFirstPosition();

        // When
        Throwable thrown = catchThrowable(this::subscribe);

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(storage.delegate.read(ID)).as("stored position").isEqualTo(new StringBasedCheckpoint(OTHER_NODES_POSITION)),
                () -> assertThat(storage.delegate.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION),
                () -> assertThat(live.positionsStartedFrom).as("positions the wrapped model started from").isEmpty(),
                () -> assertThat(live.isRunning(ID)).as("running in the wrapped model").isFalse(),
                () -> assertThat(strategy.holders).as("holders of the lease").isEmpty(),
                () -> assertThat(strategy.registrations).as("registered as a candidate for the lease").containsExactly(ID),
                () -> assertThat(model.isRunning(ID)).as("running in the competing consumer model").isFalse(),
                () -> assertThat(catchThrowable(this::subscribe)).as("subscribing the same id again").isInstanceOf(DuplicateSubscriptionIdException.class)
        );
    }

    @Test
    void a_node_that_is_granted_the_lease_with_a_version_not_older_than_the_one_stored_resumes_the_subscription_from_the_stored_position_and_delivers() {
        // Given a subscribe the wrapped model refused for a newer write version stored
        aModelWhereAnotherNodeStoresAPositionWithANewerVersionJustBeforeThisNodeSettlesItsFirstPosition();
        Throwable refused = catchThrowable(this::subscribe);
        assertThat(refused).as("thrown by the subscribe that was refused").isNull();
        assertThat(live.isRunning(ID)).as("running in the wrapped model before the lease is granted again").isFalse();
        // and the lease moving back to this node, with a version that is not older than the stored one
        versionOfThisNodesLease.set(OTHER_NODES_VERSION + 1);

        // When
        Throwable thrown = catchThrowable(strategy::grantTheLeaseToThisNode);
        await().untilAsserted(() -> assertThat(live.isRunning(ID)).as("running in the wrapped model").isTrue());
        live.deliver(anEvent("event-1", "position-1"));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown when the lease is granted").isNull(),
                () -> assertThat(live.positionsStartedFrom).as("positions the wrapped model started from").containsExactly(OTHER_NODES_POSITION),
                () -> assertThat(delivered).as("events delivered").extracting(CloudEvent::getId).containsExactly("event-1"),
                () -> assertThat(storage.delegate.read(ID)).as("stored position").isEqualTo(new StringBasedCheckpoint("position-1")),
                () -> assertThat(storage.delegate.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION + 1),
                () -> assertThat(strategy.holders).as("holders of the lease").containsExactly(ID)
        );
    }

    @Test
    void a_subscribe_refused_for_a_newer_write_version_stored_while_the_model_is_stopped_returns_and_competes_for_the_lease_again_after_start() {
        // Given another node stored a position with a newer version, and a model that is stopped
        aModelWhereAnotherNodeStoresAPositionWithANewerVersionJustBeforeThisNodeSettlesItsFirstPosition();
        model.stop();

        // When
        Throwable thrown = catchThrowable(this::subscribe);

        // Then it returns, and nothing runs or holds the lease
        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(storage.delegate.read(ID)).as("stored position").isEqualTo(new StringBasedCheckpoint(OTHER_NODES_POSITION)),
                () -> assertThat(storage.delegate.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION),
                () -> assertThat(live.isRunning(ID)).as("running in the wrapped model").isFalse(),
                () -> assertThat(strategy.holders).as("holders of the lease").isEmpty(),
                () -> assertThat(strategy.registrations).as("registered for the lease while stopped").isEmpty()
        );

        // When the lease moves back to this node with a version that is not older than the stored one, after start()
        versionOfThisNodesLease.set(OTHER_NODES_VERSION + 1);
        strategy.grantOnRegister = false;
        model.start();
        assertThat(strategy.registrations).as("registered for the lease after start").containsExactly(ID);
        Throwable granted = catchThrowable(strategy::grantTheLeaseToThisNode);
        await().untilAsserted(() -> assertThat(live.isRunning(ID)).as("running in the wrapped model").isTrue());
        live.deliver(anEvent("event-1", "position-1"));

        // Then
        assertAll(
                () -> assertThat(granted).as("thrown when the lease is granted").isNull(),
                () -> assertThat(live.positionsStartedFrom).as("positions the wrapped model started from").containsExactly(OTHER_NODES_POSITION),
                () -> assertThat(delivered).as("events delivered").extracting(CloudEvent::getId).containsExactly("event-1"),
                () -> assertThat(storage.delegate.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION + 1)
        );
    }

    private void aModelWhereAnotherNodeStoresAPositionWithANewerVersionJustBeforeThisNodeSettlesItsFirstPosition() {
        storage = new RaceStorage(() -> storage.delegate.save(ID, new StringBasedCheckpoint(OTHER_NODES_POSITION), CheckpointWriteCondition.notOlderThan(OTHER_NODES_VERSION)));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage, id -> OptionalLong.of(versionOfThisNodesLease.get()));
        model = new CompetingConsumerSubscriptionModel(durable, strategy);
        strategy.grantOnRegister = true;
    }

    private Subscription subscribe() {
        return model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), delivered::add);
    }

    private static CloudEvent anEvent(String id, String position) {
        CloudEvent event = CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
        return new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(position));
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

        @Override
        public void shutdown() {
        }
    }

    /**
     * Delegates to a real {@link InMemoryCheckpointStorage}, which evaluates write conditions, and runs a hook once
     * before the first three-argument resolver, which stands in for another node writing just before this one does.
     */
    private static final class RaceStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        private final Runnable beforeTheFirstThreeArgumentResolver;
        private boolean hookHasRun = false;

        RaceStorage(Runnable beforeTheFirstThreeArgumentResolver) {
            this.beforeTheFirstThreeArgumentResolver = beforeTheFirstThreeArgumentResolver;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }

        @Override
        public synchronized Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (!hookHasRun) {
                hookHasRun = true;
                beforeTheFirstThreeArgumentResolver.run();
            }
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    // A live model with a global checkpoint, which delivers the events a test hands it and evaluates a dynamic start on subscribe
    private static final class Live implements CheckpointAwareSubscriptionModel {
        final List<String> positionsStartedFrom = new CopyOnWriteArrayList<>();
        private final java.util.Map<String, Consumer<CloudEvent>> running = new ConcurrentHashMap<>();

        void deliver(CloudEvent event) {
            running.values().forEach(action -> action.accept(event));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic() && startAt.get(new SubscriptionModelContext(Live.class)) instanceof StartAt.StartAtCheckpoint checkpoint) {
                positionsStartedFrom.add(checkpoint.checkpoint.asString());
            }
            running.put(subscriptionId, action);
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
            return running.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running.remove(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
        }
    }
}
