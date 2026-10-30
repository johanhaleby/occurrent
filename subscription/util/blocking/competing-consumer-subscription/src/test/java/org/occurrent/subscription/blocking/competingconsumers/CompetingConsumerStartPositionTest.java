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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReads;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
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
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Covers what this model does to the start position it is handed. It resolves the position to find out whether to
 * compete for the subscription, and the model it wraps receives the caller's own object either way, which is what
 * {@link CompetingConsumerSubscriptionModel#decidesWhereTheSubscriptionStarts()} answering {@code false} tells
 * {@link ManualStartSubscriptionModel} so that a registration under this model is recorded from the model below it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStartPositionTest {

    private static final String SUBSCRIPTION_ID = "subscriptionId";
    private static final String SUBSCRIBER_ID = "this node";
    private static final StartAt POSITION_CHECKPOINT = StartAt.checkpoint(GlobalCheckpoint.of(3));
    private static final String REFUSAL = "Subscription 'subscriptionId' has no subscription filter and starts at a position checkpoint";
    private static final String LEASE_GIVEN_BACK = "The wrapped subscription model failed to start itself, or to start or resume a subscription this node holds the lease for, so the lease is given back (subscriberId={}, subscriptionId={})";

    private final FakeSubscriptionModel delegate = new FakeSubscriptionModel();
    private final FakeCompetingConsumerStrategy strategy = new FakeCompetingConsumerStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    @BeforeEach
    void captureTheLog() {
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void stopCapturingTheLog() {
        modelLog.detachAppender(logged);
    }

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

    @Test
    void a_start_the_dual_mode_dispatcher_refuses_when_the_lease_is_won_at_subscribe_is_thrown_to_the_caller_unlogged_and_gives_the_registration_up() {
        // Given
        FakeSubscriptionModel live = new FakeSubscriptionModel();
        FakeCompetingConsumerStrategy leases = new FakeCompetingConsumerStrategy();
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(dualModeDispatcherOver(live), leases);
        List<CloudEvent> delivered = new ArrayList<>();

        // When
        Throwable refusal = catchThrowable(() -> competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, POSITION_CHECKPOINT, delivered::add));
        Throwable laterGrant = catchThrowable(() -> leases.grant(SUBSCRIPTION_ID, SUBSCRIBER_ID));

        // Then
        assertThat(refusal).as("what the subscribe throws").isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith(REFUSAL);
        assertThat(leases.registrations).as("registrations with the lease strategy").containsExactly(SUBSCRIBER_ID);
        assertThat(leases.unregistrations).as("registrations given up").containsExactly(SUBSCRIBER_ID);
        assertThat(warnings()).as("warnings logged").isEmpty();
        assertThat(laterGrant).as("what a grant after the refusal throws").isNull();
        assertThat(live.startAtReceived).as("what the live model was subscribed from").isNull();
        assertThat(delivered).as("what was delivered").isEmpty();
        competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.now(), delivered::add);
        assertThat(live.startAtReceived).as("what the live model is subscribed from once the id is used again").isNotNull();
        competingConsumer.shutdown();
    }

    @Test
    void a_start_the_dual_mode_dispatcher_refuses_at_a_later_grant_is_logged_thrown_to_the_grant_gives_the_lease_back_and_is_tried_again_at_the_next_grant() {
        // Given
        FakeSubscriptionModel live = new FakeSubscriptionModel();
        FakeCompetingConsumerStrategy leases = new FakeCompetingConsumerStrategy();
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(dualModeDispatcherOver(live), leases);
        List<CloudEvent> delivered = new ArrayList<>();
        leases.holdFor(SUBSCRIPTION_ID, "another node");
        competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, POSITION_CHECKPOINT, delivered::add);

        // When
        Throwable firstGrant = catchThrowable(() -> leases.grant(SUBSCRIPTION_ID, SUBSCRIBER_ID));
        boolean leaseHeldAfterTheFirstGrant = leases.hasLock(SUBSCRIPTION_ID, SUBSCRIBER_ID);
        Throwable secondGrant = catchThrowable(() -> leases.grant(SUBSCRIPTION_ID, SUBSCRIBER_ID));

        // Then
        assertThat(firstGrant).as("what the first grant throws").isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith(REFUSAL);
        assertThat(secondGrant).as("what the second grant throws").isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith(REFUSAL);
        assertThat(leaseHeldAfterTheFirstGrant).as("lease held after the first grant").isFalse();
        assertThat(leases.hasLock(SUBSCRIPTION_ID, SUBSCRIBER_ID)).as("lease held after the second grant").isFalse();
        assertThat(warnings()).as("warnings logged").containsExactly(LEASE_GIVEN_BACK, LEASE_GIVEN_BACK);
        assertThat(leases.registrations).as("registrations with the lease strategy").containsExactly(SUBSCRIBER_ID);
        assertThat(leases.unregistrations).as("registrations given up").isEmpty();
        assertThat(live.startAtReceived).as("what the live model was subscribed from").isNull();
        assertThat(delivered).as("what was delivered").isEmpty();
        assertThat(catchThrowable(() -> competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.now(), delivered::add)))
                .as("what a second subscribe for the same id throws").isInstanceOf(DuplicateSubscriptionIdException.class);
        competingConsumer.shutdown();
    }

    @Test
    void a_start_the_dual_mode_dispatcher_refuses_while_this_model_is_stopped_is_thrown_to_the_caller_at_subscribe_without_asking_for_a_lease() {
        // Given
        FakeSubscriptionModel live = new FakeSubscriptionModel();
        FakeCompetingConsumerStrategy leases = new FakeCompetingConsumerStrategy();
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(dualModeDispatcherOver(live), leases);
        List<CloudEvent> delivered = new ArrayList<>();
        competingConsumer.stop();

        // When
        Throwable refusal = catchThrowable(() -> competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, POSITION_CHECKPOINT, delivered::add));

        // Then
        assertThat(refusal).as("what the subscribe throws").isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith(REFUSAL);
        assertThat(leases.registrations).as("registrations with the lease strategy").isEmpty();
        assertThat(warnings()).as("warnings logged").isEmpty();
        assertThat(live.startAtReceived).as("what the live model was subscribed from").isNull();
        assertThat(delivered).as("what was delivered").isEmpty();
        assertThat(catchThrowable(() -> competingConsumer.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.now(), delivered::add)))
                .as("what a second subscribe for the same id throws").isNull();
        competingConsumer.shutdown();
    }

    private List<String> warnings() {
        return logged.list.stream().filter(event -> event.getLevel() == Level.WARN).map(ILoggingEvent::getMessage).toList();
    }

    private static SubscriptionModel dualModeDispatcherOver(CheckpointAwareSubscriptionModel live) {
        return new CatchupSubscriptionModel(live, unused(EventStoreQueries.class, CatchupReads.class), unused(DcbEventStore.class, DcbCatchupReads.class),
                DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(100));
    }

    // An implementation of the interfaces that answers the methods an interface defaults and the ones returning a boolean, and fails every other call
    @SuppressWarnings("unchecked")
    private static <T> T unused(Class<T> type, Class<?>... moreTypes) {
        Class<?>[] interfaces = new Class<?>[1 + moreTypes.length];
        interfaces[0] = type;
        System.arraycopy(moreTypes, 0, interfaces, 1, moreTypes.length);
        return (T) Proxy.newProxyInstance(type.getClassLoader(), interfaces, (proxy, method, args) -> {
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "an unused " + type.getSimpleName();
                };
            }
            if (method.getReturnType() == boolean.class) {
                return false;
            }
            throw new UnsupportedOperationException(method.getName());
        });
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
    private static final class FakeSubscriptionModel implements CheckpointAwareSubscriptionModel {
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

    // Grants the lock to whoever asks first, which is all these tests need from a strategy.
    private static final class FakeCompetingConsumerStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> lockHolder = new HashMap<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();
        final List<String> registrations = new ArrayList<>();
        final List<String> unregistrations = new ArrayList<>();

        void holdFor(String subscriptionId, String subscriberId) {
            lockHolder.put(subscriptionId, subscriberId);
        }

        void grant(String subscriptionId, String subscriberId) {
            lockHolder.put(subscriptionId, subscriberId);
            List.copyOf(listeners).forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriberId);
            boolean acquired = !lockHolder.containsKey(subscriptionId) || subscriberId.equals(lockHolder.get(subscriptionId));
            if (acquired) {
                lockHolder.put(subscriptionId, subscriberId);
            }
            return acquired;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            unregistrations.add(subscriberId);
            if (subscriberId.equals(lockHolder.get(subscriptionId))) {
                lockHolder.remove(subscriptionId);
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            if (subscriberId.equals(lockHolder.get(subscriptionId))) {
                lockHolder.remove(subscriptionId);
            }
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
