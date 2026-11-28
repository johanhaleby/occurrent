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
import org.junit.jupiter.api.BeforeEach;
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
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe the wrapped model refuses with a {@link CheckpointWriteConditionNotFulfilledException} waits for the lease
 * only when the refusal names a stored write version newer than the one this node wrote with, nothing runs in the wrapped
 * model, and the model and the id are still there. Every other refusal is thrown by the subscribe.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerWaitsForTheLeaseAfterARefusedWriteTest {

    private static final String NODE = "this-node";
    private static final String ID = "subscription";
    private static final long THIS_NODES_VERSION = 2;
    private static final CheckpointWriteCondition THIS_NODES_CONDITION = CheckpointWriteCondition.notOlderThan(THIS_NODES_VERSION);

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Strategy strategy = new Strategy();
    private final Refusing refusing = new Refusing();
    private CompetingConsumerSubscriptionModel model;

    @BeforeEach
    void createModel() {
        model = new CompetingConsumerSubscriptionModel(refusing, strategy);
    }

    @AfterEach
    void shutdown() {
        model.shutdown();
        executor.shutdownNow();
    }

    @Test
    void a_refusal_naming_a_newer_stored_version_makes_the_subscribe_return_and_wait_for_the_lease() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, THIS_NODES_CONDITION);

        Throwable thrown = catchThrowable(this::subscribe);

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(strategy.holders).as("holders of the lease").isEmpty(),
                () -> assertThat(strategy.registrations).as("registered as a candidate for the lease").containsExactly(ID)
        );
    }

    @Test
    void a_refusal_is_thrown_when_the_wrapped_model_runs_the_subscription_after_all() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, THIS_NODES_CONDITION);
        refusing.runs = () -> true;

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    @Test
    void a_refusal_is_thrown_with_the_failure_suppressed_when_the_wrapped_model_cannot_say_whether_it_runs_the_subscription() {
        IllegalStateException cannotSay = new IllegalStateException("cannot say");
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, THIS_NODES_CONDITION);
        refusing.runs = () -> {
            throw cannotSay;
        };

        Throwable thrown = catchThrowable(this::subscribe);

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal),
                () -> assertThat(thrown.getSuppressed()).as("suppressed by the refusal").anyMatch(suppressed -> isOrIsCausedBy(suppressed, cannotSay))
        );
    }

    @Test
    void a_refusal_is_thrown_when_the_model_is_shut_down_while_the_subscribe_is_being_made() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, THIS_NODES_CONDITION);
        refusing.whileBeingMade = () -> model.shutdown();

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    @Test
    void a_refusal_is_thrown_when_the_id_is_cancelled_while_the_subscribe_is_being_made() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, THIS_NODES_CONDITION);
        refusing.whileBeingMade = () -> model.cancelSubscription(ID);

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    @Test
    void a_refusal_naming_the_same_version_as_the_one_this_node_wrote_with_is_thrown() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION, THIS_NODES_CONDITION);

        Throwable thrown = catchThrowable(this::subscribe);

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal),
                () -> assertThat(strategy.registrations).as("registered as a candidate for the lease").isEmpty()
        );
    }

    @Test
    void a_refusal_naming_an_older_version_than_the_one_this_node_wrote_with_is_thrown() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION - 1, THIS_NODES_CONDITION);

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    @Test
    void a_refusal_naming_no_stored_version_is_thrown() {
        refusing.refusal = new CheckpointWriteConditionNotFulfilledException(ID, OptionalLong.empty(), THIS_NODES_CONDITION);

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    @Test
    void a_refusal_of_a_condition_that_is_not_a_not_older_than_condition_is_thrown() {
        refusing.refusal = refusalOf(THIS_NODES_VERSION + 1, CheckpointWriteCondition.ifAbsent());

        Throwable thrown = catchThrowable(this::subscribe);

        assertThat(thrown).as("thrown by subscribe").isSameAs(refusing.refusal);
    }

    private Subscription subscribe() {
        return model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    private static CheckpointWriteConditionNotFulfilledException refusalOf(long storedVersion, CheckpointWriteCondition condition) {
        return new CheckpointWriteConditionNotFulfilledException(ID, OptionalLong.of(storedVersion), condition);
    }

    private static boolean isOrIsCausedBy(Throwable failure, Throwable expected) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current == expected) {
                return true;
            }
        }
        return false;
    }

    // Grants the lease on register, so the consumer holds it while the wrapped model is asked to subscribe
    private static final class Strategy implements CompetingConsumerStrategy {
        final Set<String> holders = ConcurrentHashMap.newKeySet();
        final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            holders.add(subscriptionId);
            return true;
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

    // Refuses every subscribe with refusal, after running whileBeingMade on a thread of its own. What it answers to
    // isRunning is up to runs, and by default nothing runs
    private final class Refusing implements CheckpointAwareSubscriptionModel {
        volatile @Nullable CheckpointWriteConditionNotFulfilledException refusal;
        volatile @Nullable Runnable whileBeingMade;
        volatile java.util.function.BooleanSupplier runs = () -> false;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Runnable hook = whileBeingMade;
            if (hook != null) {
                try {
                    executor.submit(hook).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("The hook run while the subscribe is being made failed or did not finish", e);
                }
            }
            throw refusal;
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
            return runs.getAsBoolean();
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
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
