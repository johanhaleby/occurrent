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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * When the position of a subscription that receives no events is saved, and with which write condition. The wrapped
 * model is a fake the test reads from by hand, so each test decides when a read happens and what it returns.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelQuietPositionTest {

    private static final Duration INTERVAL = Duration.ofMillis(200);
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void the_quiet_position_is_saved_once_the_interval_has_passed_since_the_subscribe() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });

        // When
        boolean savedRightAfterTheSubscribe = wrapped.readNothing("id", QUIET);
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterTheInterval = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedRightAfterTheSubscribe).as("wanted the quiet position right after the subscribe").isFalse();
        assertThat(savedAfterTheInterval).as("wanted the quiet position after the interval").isTrue();
        assertThat(storage.read("id")).isEqualTo(QUIET);
    }

    @Test
    void the_quiet_position_is_saved_at_most_once_per_interval() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean first = wrapped.readNothing("id", QUIET);
        boolean second = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-2"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean third = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-3"));

        // Then
        assertThat(List.of(first, second, third)).containsExactly(true, false, true);
        assertThat(storage.read("id")).isEqualTo(new StringBasedCheckpoint("quiet-3"));
    }

    @Test
    void a_checkpoint_saved_for_an_event_starts_the_interval_again() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        Checkpoint positionOfTheEvent = new StringBasedCheckpoint("event");
        wrapped.deliver("id", positionOfTheEvent);
        boolean savedRightAfterTheEvent = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedRightAfterTheEvent).isFalse();
        assertThat(storage.read("id")).isEqualTo(positionOfTheEvent);
    }

    @Test
    void the_write_condition_is_the_one_read_before_the_wrapped_model_read() throws InterruptedException {
        // Given
        AtomicLong leaseVersion = new AtomicLong(1);
        RecordingStorage recordingStorage = new RecordingStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, recordingStorage, saveQuietPositionEvery(INTERVAL), subscriptionId -> OptionalLong.of(leaseVersion.get()));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        Consumer<Checkpoint> saver = wrapped.beforeReading("id");
        leaseVersion.set(5);
        saver.accept(QUIET);

        // Then
        assertThat(recordingStorage.conditions).containsExactly(CheckpointWriteCondition.notOlderThan(1));
    }

    @Test
    void a_refused_write_of_the_quiet_position_is_thrown_to_the_wrapped_model() throws InterruptedException {
        // Given
        storage.save("id", START, CheckpointWriteCondition.notOlderThan(2));
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL), subscriptionId -> OptionalLong.of(1));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        Consumer<Checkpoint> saver = wrapped.beforeReading("id");

        // Then
        assertThatThrownBy(() -> saver.accept(QUIET)).isExactlyInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
        assertThat(storage.read("id")).isEqualTo(START);
        assertThat(storage.writeVersion("id")).hasValue(2);
    }

    @Test
    void a_storage_failure_is_not_thrown_to_the_wrapped_model_and_the_save_is_tried_again_after_the_interval() throws InterruptedException {
        // Given
        RecordingStorage failingStorage = new RecordingStorage();
        failingStorage.failing.set(true);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, failingStorage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean whileFailing = wrapped.readNothing("id", QUIET);
        boolean rightAfterTheFailure = wrapped.readNothing("id", QUIET);
        failingStorage.failing.set(false);
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean afterTheInterval = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(List.of(whileFailing, rightAfterTheFailure, afterTheInterval)).containsExactly(true, false, true);
        assertThat(failingStorage.read("id")).isEqualTo(QUIET);
    }

    @Test
    void a_write_version_source_that_throws_is_not_thrown_to_the_wrapped_model_and_is_asked_again_after_the_interval() throws InterruptedException {
        // Given
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger asked = new AtomicInteger();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL), subscriptionId -> {
            asked.incrementAndGet();
            if (failing.get()) {
                throw new IllegalStateException("expected");
            }
            return OptionalLong.of(1);
        });
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean whileFailing = wrapped.readNothing("id", QUIET);
        boolean rightAfterTheFailure = wrapped.readNothing("id", QUIET);
        int askedWhileFailing = asked.get();
        failing.set(false);
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean afterTheInterval = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(List.of(whileFailing, rightAfterTheFailure, afterTheInterval)).containsExactly(false, false, true);
        assertThat(askedWhileFailing).isEqualTo(1);
        assertThat(storage.read("id")).isEqualTo(QUIET);
    }

    @Test
    void a_read_that_began_before_a_cancel_saves_nothing_for_a_later_subscribe_of_the_same_id() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);
        Consumer<Checkpoint> saverFromBeforeTheCancel = wrapped.beforeReading("id");

        // When
        model.cancelSubscription("id");
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        saverFromBeforeTheCancel.accept(QUIET);

        // Then
        assertThat(storage.read("id")).isNull();
    }

    @Test
    void nothing_is_saved_for_a_cancelled_subscription() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        model.cancelSubscription("id");

        // Then
        assertThat(wrapped.readNothing("id", QUIET)).isFalse();
        assertThat(storage.read("id")).isNull();
    }

    @Test
    void nothing_is_saved_for_a_subscription_this_model_stores_no_checkpoints_for() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.dynamic(() -> null), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // Then
        assertThat(wrapped.readNothing("id", QUIET)).isFalse();
        assertThat(storage.read("id")).isNull();
    }

    @Test
    void no_listener_is_added_when_the_quiet_position_is_never_saved() {
        new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());

        assertThat(wrapped.listeners).isEmpty();
    }

    @Test
    void shutting_down_removes_the_quiet_position_listener_it_added_to_the_wrapped_model() {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
        assertThat(wrapped.listeners).hasSize(1);

        model.shutdown();

        assertThat(wrapped.listeners).isEmpty();
    }

    @Test
    void the_quiet_position_is_saved_every_minute_unless_configured_otherwise() {
        assertThat(new DurableSubscriptionModelConfig(everyEvent()).quietPositionSaveInterval).isEqualTo(Duration.ofMinutes(1));
        assertThat(new DurableSubscriptionModelConfig(3).startWhenNoStartPositionCanBeRecorded(true).quietPositionSaveInterval).isEqualTo(Duration.ofMinutes(1));
        assertThat(saveQuietPositionEvery(INTERVAL).startWhenNoStartPositionCanBeRecorded(true).quietPositionSaveInterval).isEqualTo(INTERVAL);
        assertThat(new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition().quietPositionSaveInterval).isNull();
        assertThatThrownBy(() -> saveQuietPositionEvery(Duration.ZERO)).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    private static DurableSubscriptionModelConfig saveQuietPositionEvery(Duration interval) {
        return new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(interval);
    }

    private static final class RecordingStorage extends InMemoryCheckpointStorage {
        final List<CheckpointWriteCondition> conditions = new ArrayList<>();
        final AtomicBoolean failing = new AtomicBoolean();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (failing.get()) {
                throw new IllegalStateException("expected");
            }
            conditions.add(condition);
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    private static final class QuietPositionReportingModel implements CheckpointAwareSubscriptionModel, QuietPositionReportingSubscriptions {
        final List<QuietPositionListener> listeners = new ArrayList<>();
        final Map<String, Consumer<CloudEvent>> actions = new HashMap<>();

        // A read that returned no event. True when a listener wanted the quiet position
        boolean readNothing(String subscriptionId, Checkpoint quietPosition) {
            Consumer<Checkpoint> saver = beforeReading(subscriptionId);
            if (saver == null) {
                return false;
            }
            saver.accept(quietPosition);
            return true;
        }

        @Nullable Consumer<Checkpoint> beforeReading(String subscriptionId) {
            return listeners.isEmpty() ? null : listeners.getFirst().beforeReading(subscriptionId);
        }

        void deliver(String subscriptionId, Checkpoint position) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("type").build();
            actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(cloudEvent, position));
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
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
            return null;
        }

        @Override
        public void shutdown() {
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
            return actions.containsKey(subscriptionId);
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
            actions.remove(subscriptionId);
        }
    }
}
