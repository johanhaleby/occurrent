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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * #1251. A resume at a position is not reachable through a {@link CompetingConsumerSubscriptionModel}, since it would
 * run the subscription in the model beneath while the competing consumer still records it as paused and holds no
 * lease for it. Over a {@link DurableSubscriptionModel}, a node that never held the lease would also store the position
 * over the checkpoint the lease holder stored.
 */
@DisplayName("CompetingConsumerHasNoResumeAtPosition")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerHasNoResumeAtPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final String SUBSCRIBER_ID = "subscriber";
    private static final Checkpoint POSITION = new StringBasedCheckpoint("position-to-resume-at");

    private CompetingConsumerSubscriptionModel model;

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
    }

    static Stream<Named<Boolean>> nodes() {
        return Stream.of(Named.of("a node that holds the lease", true), Named.of("a node that does not hold the lease", false));
    }

    @Nested
    @DisplayName("when a subscription is paused through the competing consumer model")
    class When_a_subscription_is_paused_through_the_competing_consumer_model {

        @ParameterizedTest
        @MethodSource("org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerHasNoResumeAtPositionTest#nodes")
        @DisplayName("a resume at a position has no way in and the model keeps reporting the subscription paused")
        void a_resume_at_a_position_has_no_way_in_and_the_model_keeps_reporting_the_subscription_paused(boolean holdsTheLease) {
            // Given
            RepositionableLiveModel wrapped = new RepositionableLiveModel();
            model = new CompetingConsumerSubscriptionModel(wrapped, new LeaseStrategy(holdsTheLease));
            model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), e -> {
            });
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(model);
            found.ifPresent(repositionable -> repositionable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(POSITION)));

            // Then
            assertAll(
                    () -> assertThat(found).as("the repositionable model found").isEmpty(),
                    () -> assertThat(model.isPaused(SUBSCRIPTION_ID)).as("the subscription is paused in the competing consumer model").isTrue(),
                    () -> assertThat(wrapped.repositionedTo).as("positions the model beneath was resumed at").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a node that never held the lease is over a durable model that shares its checkpoint storage with the lease holder")
    class When_a_node_that_never_held_the_lease_is_over_a_durable_model_that_shares_its_checkpoint_storage_with_the_lease_holder {

        @Test
        void a_resume_at_a_position_leaves_the_checkpoint_the_lease_holder_stored() {
            // Given
            InMemoryCheckpointStorage sharedStorage = new InMemoryCheckpointStorage();
            Checkpoint storedByTheLeaseHolder = new StringBasedCheckpoint("stored-by-the-lease-holder");
            sharedStorage.save(SUBSCRIPTION_ID, storedByTheLeaseHolder, CheckpointWriteCondition.notOlderThan(7));
            // A node that never held the lease has no write version to stamp its checkpoint writes with
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RepositionableLiveModel(), sharedStorage, subscriptionId -> OptionalLong.empty());
            model = new CompetingConsumerSubscriptionModel(durable, new LeaseStrategy(false));
            // Made while the model is stopped, a subscription goes to the model beneath held paused, and this node never wins its lease
            model.stop();
            model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), e -> {
            });
            model.pauseSubscription(SUBSCRIPTION_ID);
            assertThat(durable.isPaused(SUBSCRIPTION_ID)).as("the durable model holds the subscription paused").isTrue();

            // When
            Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(model);
            found.ifPresent(repositionable -> repositionable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(POSITION)));

            // Then
            assertAll(
                    () -> assertThat(sharedStorage.read(SUBSCRIPTION_ID)).as("checkpoint in the shared storage").isEqualTo(storedByTheLeaseHolder),
                    () -> assertThat(found).as("the repositionable model found").isEmpty()
            );
        }
    }

    // A live model that can be resumed at a position and keeps track of which subscriptions are paused
    private static final class RepositionableLiveModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        final List<StartAt> repositionedTo = new CopyOnWriteArrayList<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribed.add(subscriptionId);
            paused.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribed.add(subscriptionId);
            paused.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            repositionedTo.add(startAt);
            paused.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            paused.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (subscribed.contains(subscriptionId)) {
                paused.add(subscriptionId);
            }
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public void stop() {
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // Answers every registration the same, so a node either holds the lease for every subscription or for none
    private record LeaseStrategy(boolean holdsTheLease) implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return holdsTheLease;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holdsTheLease;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }
}
