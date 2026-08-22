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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers what this model does to the start position it is handed. It resolves the position to find out whether to
 * compete for the subscription, and the model it wraps receives the caller's own object either way, which is what
 * {@link CompetingConsumerSubscriptionModel#decidesWhereTheSubscriptionStarts()} answering {@code false} tells
 * {@link ManualStartSubscriptionModel} so that a registration under this model is recorded from the model below it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStartPositionTest {

    private static final String SUBSCRIPTION_ID = "subscriptionId";

    private final FakeSubscriptionModel delegate = new FakeSubscriptionModel();
    private final FakeCompetingConsumerStrategy strategy = new FakeCompetingConsumerStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void the_wrapped_model_receives_the_callers_own_position_when_this_model_competes() {
        StartAt callersOwnStartAt = StartAt.dynamic(context -> StartAt.subscriptionModelDefault());

        model.subscribe(SUBSCRIPTION_ID, null, callersOwnStartAt, __ -> {
        });

        assertThat(delegate.startAtReceived).isSameAs(callersOwnStartAt);
    }

    @Test
    void the_wrapped_model_receives_the_callers_own_position_when_the_caller_declines_to_compete() {
        StartAt callersOwnStartAt = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        model.subscribe(SUBSCRIPTION_ID, null, callersOwnStartAt, __ -> {
        });

        assertThat(delegate.startAtReceived).isSameAs(callersOwnStartAt);
    }

    @Test
    void a_registration_under_this_model_is_recorded_from_the_position_the_model_below_it_reads() {
        // ManualStartSubscriptionModel wraps this competing consumer model, which wraps a fake delegate, two layers
        // under the walk rather than the four-layer starter stack. A start position answering for each layer
        // separately used to end the walk at the competing consumer's answer, which does not decide where the
        // subscription starts, leaving the delegate to record a position when the subscription started and skip
        // everything written since registration.
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ManualStartSubscriptionModel manualStart = ManualStartSubscriptionModel.stoppedByDefault(
                model, () -> new StringCheckpoint("at-registration"), storage);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(FakeSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());

        manualStart.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    private record StringCheckpoint(String value) implements Checkpoint {
        @Override
        public String asString() {
            return value;
        }
    }

    // Only ifAbsent() is evaluated for real, which is the condition a registration is written with.
    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (condition instanceof CheckpointWriteCondition.IfAbsent && checkpoints.containsKey(subscriptionId)) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }

    // Records the start position it was handed, since that is the whole subject of these tests.
    private static final class FakeSubscriptionModel implements SubscriptionModel {
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;
        @Nullable StartAt startAtReceived = null;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            startAtReceived = startAt;
            runningIds.add(subscriptionId);
            pausedIds.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId) && !pausedIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            pausedIds.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pausedIds.add(subscriptionId);
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // Grants the lock to whoever asks first, which is all these tests need from a strategy.
    private static final class FakeCompetingConsumerStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> lockHolder = new HashMap<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            boolean acquired = !lockHolder.containsKey(subscriptionId) || subscriberId.equals(lockHolder.get(subscriptionId));
            if (acquired) {
                lockHolder.put(subscriptionId, subscriberId);
            }
            return acquired;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            if (subscriberId.equals(lockHolder.get(subscriptionId))) {
                lockHolder.remove(subscriptionId);
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            unregisterCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return subscriberId.equals(lockHolder.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void shutdown() {
            lockHolder.clear();
            listeners.clear();
        }
    }
}
