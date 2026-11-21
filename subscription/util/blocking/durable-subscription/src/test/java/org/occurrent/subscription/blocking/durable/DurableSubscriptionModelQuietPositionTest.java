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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static java.util.concurrent.TimeUnit.SECONDS;
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
        subscribeFromTheStoredStart(model, storage, __ -> {
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
        subscribeFromTheStoredStart(model, storage, __ -> {
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
    void a_persist_predicate_that_never_stores_has_the_quiet_position_saved_until_its_first_event_once_a_position_is_stored() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, storage, __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean savedBeforeAnyEvent = wrapped.readNothing("id", QUIET);
        wrapped.deliver("id", new StringBasedCheckpoint("event"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterADeclinedEvent = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-2"));

        // Then
        assertThat(List.of(savedBeforeAnyEvent, savedAfterADeclinedEvent)).containsExactly(true, false);
        assertThat(storage.read("id")).isEqualTo(QUIET);
    }

    @Test
    void the_quiet_position_is_not_saved_past_an_event_the_persist_predicate_declined_to_store() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(3).saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });

        // When
        List.of("e1", "e2", "e3", "e4", "e5").forEach(position -> wrapped.deliver("id", new StringBasedCheckpoint(position)));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterADeclinedEvent = wrapped.readNothing("id", QUIET);
        wrapped.deliver("id", new StringBasedCheckpoint("e6"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterAStoredEvent = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedAfterADeclinedEvent).as("wanted the quiet position after e4 and e5 were declined").isFalse();
        assertThat(savedAfterAStoredEvent).as("wanted the quiet position after e6 was stored").isTrue();
        assertThat(storage.read("id")).isEqualTo(QUIET);
    }

    @Test
    void a_persist_predicate_other_than_every_n_has_the_quiet_position_saved_before_its_first_event() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> true).saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, storage, __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean savedBeforeAnyEvent = wrapped.readNothing("id", QUIET);
        wrapped.deliver("id", new StringBasedCheckpoint("event"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterAStoredEvent = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-2"));

        // Then
        assertThat(List.of(savedBeforeAnyEvent, savedAfterAStoredEvent)).containsExactly(true, true);
        assertThat(storage.read("id")).isEqualTo(new StringBasedCheckpoint("quiet-2"));
    }

    @Test
    void a_persist_predicate_that_never_stores_has_no_position_saved_for_a_subscription_from_a_start_position_of_its_own() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean savedBeforeAnyEvent = wrapped.readNothing("id", QUIET);
        wrapped.deliver("id", new StringBasedCheckpoint("event"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterADeclinedEvent = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-2"));

        // Then
        assertThat(storage.read("id")).as("checkpoint stored for a subscription from a start position of its own whose predicate never stores").isNull();
        assertThat(List.of(savedBeforeAnyEvent, savedAfterADeclinedEvent)).as("wanted the quiet position before any event and after a declined one").containsExactly(false, false);
    }

    @Test
    void the_quiet_position_is_saved_before_the_first_event_of_a_subscription_whose_start_position_was_recorded() throws InterruptedException {
        // Given
        wrapped.globalCheckpoint = new StringBasedCheckpoint("recorded");
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean savedBeforeAnyEvent = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedBeforeAnyEvent).as("wanted the quiet position before any event").isTrue();
        assertThat(storage.read("id")).as("checkpoint stored").isEqualTo(QUIET);
    }

    @Test
    void the_write_condition_is_the_one_read_before_the_wrapped_model_read() throws InterruptedException {
        // Given
        AtomicLong leaseVersion = new AtomicLong(1);
        RecordingStorage recordingStorage = new RecordingStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, recordingStorage, saveQuietPositionEvery(INTERVAL), subscriptionId -> OptionalLong.of(leaseVersion.get()));
        subscribeFromTheStoredStart(model, recordingStorage, __ -> {
        });
        recordingStorage.conditions.clear();
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
        model.subscribe("id", null, StartAt.subscriptionModelDefault(), __ -> {
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
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, failingStorage, saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, failingStorage, __ -> {
        });
        failingStorage.failing.set(true);
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
        subscribeFromTheStoredStart(model, storage, __ -> {
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
        subscribeFromTheStoredStart(model, storage, __ -> {
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
    void a_quiet_position_saver_obtained_before_shutdown_saves_nothing_when_it_is_called_after_it() throws InterruptedException {
        // Given a saver the wrapped model obtained for a read that began before shutdown
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, storage, __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);
        Consumer<Checkpoint> saverFromBeforeShutdown = wrapped.beforeReading("id");
        assertThat(saverFromBeforeShutdown).as("saver").isNotNull();

        // When shutdown returns and the saver is then called
        model.shutdown();
        saverFromBeforeShutdown.accept(QUIET);

        // Then the checkpoint stored before is still the one stored
        assertThat(storage.read("id")).isEqualTo(START);
    }

    @Test
    void nothing_is_saved_for_a_cancelled_subscription() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, storage, __ -> {
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
    void the_listener_added_when_the_quiet_position_is_never_saved_saves_no_quiet_position() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        subscribeFromTheStoredStart(model, storage, __ -> {
        });
        Checkpoint positionOfTheEvent = new StringBasedCheckpoint("event");
        wrapped.deliver("id", positionOfTheEvent);
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean saved = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(wrapped.listeners).hasSize(1);
        assertThat(saved).isFalse();
        assertThat(storage.read("id")).isEqualTo(positionOfTheEvent);
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

    @Test
    void a_closed_run_whose_read_of_the_write_version_outlasts_the_pause_does_not_let_the_resumed_run_save_a_quiet_position_past_an_event_the_predicate_declined() throws InterruptedException {
        // Given a closed run whose read of the write version for e2 is held past the pause, and a resumed run that
        // delivers e2 again and then e3, which the persist predicate declines
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(declines("e3")).saveQuietPositionEvery(INTERVAL), secondReadHeld(reading, answer));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        Thread closedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e2")));
        assertThat(reading.await(5, SECONDS)).isTrue();
        wrapped.deliver("id", new StringBasedCheckpoint("e2"));
        wrapped.deliver("id", new StringBasedCheckpoint("e3"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedBeforeTheLateCall = wrapped.readNothing("id", QUIET);

        // When
        answer.countDown();
        closedRun.join(5000);
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedAfterTheLateCall = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedBeforeTheLateCall).as("quiet save wanted while e3, declined, is the last event the resumed run delivered").isFalse();
        assertThat(savedAfterTheLateCall).as("quiet save wanted after the closed run's late call, while e3 is still the last event the resumed run delivered").isFalse();
        assertThat(storage.read("id")).as("checkpoint").isNotEqualTo(QUIET);
    }

    @Test
    void the_checkpoint_stays_before_an_event_a_batching_action_has_only_buffered_when_a_closed_run_returns_late() throws InterruptedException {
        // Given an action that buffers every event and writes the buffer out on an event the persist predicate stores,
        // a closed run whose read of the write version for e2 is held past the pause, and a resumed run that delivers
        // e2 again and then e3, which the predicate declines
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        CountDownLatch e3Started = new CountDownLatch(1);
        CountDownLatch e3MayBeBuffered = new CountDownLatch(1);
        List<String> buffer = new ArrayList<>();
        List<String> writtenOut = new CopyOnWriteArrayList<>();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(declines("e3")).saveQuietPositionEvery(INTERVAL), secondReadHeld(reading, answer));
        model.subscribe("id", null, StartAt.checkpoint(START), cloudEvent -> {
            String position = positionOf(cloudEvent);
            if (position.equals("e3")) {
                e3Started.countDown();
                awaitQuietly(e3MayBeBuffered);
            }
            synchronized (buffer) {
                buffer.add(position);
                if (!position.equals("e3")) {
                    writtenOut.addAll(buffer);
                    buffer.clear();
                }
            }
        });
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        Thread closedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e2")));
        assertThat(reading.await(5, SECONDS)).isTrue();
        wrapped.deliver("id", new StringBasedCheckpoint("e2"));
        Thread resumedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e3")));
        assertThat(e3Started.await(5, SECONDS)).isTrue();

        // When the closed run's call returns while e3 is not buffered yet, and the resumed run then reads nothing
        answer.countDown();
        closedRun.join(5000);
        e3MayBeBuffered.countDown();
        resumedRun.join(5000);
        Thread.sleep(INTERVAL.toMillis() + 50);
        wrapped.readNothing("id", QUIET);

        // Then a restart, which loses the buffer, starts from before e3
        assertThat(writtenOut).as("events the action wrote out").doesNotContain("e3");
        assertThat(storage.read("id")).as("checkpoint a restart starts after, while e3 is only in the buffer").isNotEqualTo(QUIET);
    }

    @Test
    void a_quiet_position_an_earlier_run_read_is_not_saved_while_a_later_run_delivers_an_event() throws InterruptedException {
        // Given an earlier run that was given a saver for its read, and a later run, opened at an earlier position,
        // whose action for e1 has not returned
        CountDownLatch e1Started = new CountDownLatch(1);
        CountDownLatch e1MayReturn = new CountDownLatch(1);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        subscribeFromTheStoredStart(model, storage, __ -> {
            e1Started.countDown();
            awaitQuietly(e1MayReturn);
        });
        Thread.sleep(INTERVAL.toMillis() + 50);
        Consumer<Checkpoint> saverOfTheEarlierRun = wrapped.beforeReading("id");
        Thread laterRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
        assertThat(e1Started.await(5, SECONDS)).isTrue();

        // When
        saverOfTheEarlierRun.accept(QUIET);
        Checkpoint whileE1IsDelivered = storage.read("id");
        e1MayReturn.countDown();
        laterRun.join(5000);

        // Then
        assertThat(whileE1IsDelivered).as("checkpoint while the action for e1 has not returned").isNotEqualTo(QUIET);
        assertThat(storage.read("id")).as("checkpoint once the action for e1 has returned").isEqualTo(new StringBasedCheckpoint("e1"));
    }

    @Test
    void a_closed_run_whose_action_returns_after_the_resumed_run_has_stored_later_events_does_not_stop_the_quiet_position_being_saved() throws InterruptedException {
        // Given a closed run whose action for e1 returns only after a resumed run has delivered e1 again and then e2,
        // both stored
        CountDownLatch e1Started = new CountDownLatch(1);
        CountDownLatch e1MayReturn = new CountDownLatch(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
            if (firstCall.compareAndSet(true, false)) {
                e1Started.countDown();
                awaitQuietly(e1MayReturn);
            }
        });
        Thread closedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
        assertThat(e1Started.await(5, SECONDS)).isTrue();
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        wrapped.deliver("id", new StringBasedCheckpoint("e2"));
        e1MayReturn.countDown();
        closedRun.join(5000);

        // When the resumed run then reads nothing for longer than the interval
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean saved = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(saved).as("quiet save wanted once the closed run's late action has returned").isTrue();
        assertThat(storage.read("id")).as("checkpoint").isEqualTo(QUIET);
    }

    @Test
    void a_closed_run_whose_action_has_not_returned_keeps_the_quiet_position_from_being_saved_until_it_returns() throws InterruptedException {
        // Given a closed run whose action for e1 has not returned, and a resumed run that delivers e1 again and then
        // e2, both stored
        CountDownLatch e1Started = new CountDownLatch(1);
        CountDownLatch e1MayReturn = new CountDownLatch(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
            if (firstCall.compareAndSet(true, false)) {
                e1Started.countDown();
                awaitQuietly(e1MayReturn);
            }
        });
        Thread closedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
        assertThat(e1Started.await(5, SECONDS)).isTrue();
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        wrapped.deliver("id", new StringBasedCheckpoint("e2"));
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedWhileTheActionRuns = wrapped.readNothing("id", QUIET);
        Checkpoint whileTheActionRuns = storage.read("id");

        // When
        e1MayReturn.countDown();
        closedRun.join(5000);
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedOnceItHasReturned = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedWhileTheActionRuns).as("quiet save wanted while the closed run's action for e1 has not returned").isFalse();
        assertThat(whileTheActionRuns).as("checkpoint while the closed run's action for e1 has not returned").isEqualTo(new StringBasedCheckpoint("e2"));
        assertThat(savedOnceItHasReturned).as("quiet save wanted once the closed run's action for e1 has returned").isTrue();
        assertThat(storage.read("id")).as("checkpoint once the closed run's action for e1 has returned").isEqualTo(QUIET);
    }

    @Test
    void a_closed_run_whose_action_is_called_after_the_resumed_run_delivered_an_event_the_predicate_declined_does_not_let_a_quiet_position_past_that_event_be_saved() throws Exception {
        // Given a closed run that read e1 before the pause, and a resumed run that reads and delivers e1 again and then
        // e2, which the persist predicate declines
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(declines("e2")).saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        ExecutorService closedRun = Executors.newSingleThreadExecutor();
        try {
            on(closedRun, () -> wrapped.beforeReading("id"));
            wrapped.beforeReading("id");
            wrapped.deliver("id", new StringBasedCheckpoint("e1"));
            wrapped.beforeReading("id");
            wrapped.deliver("id", new StringBasedCheckpoint("e2"));

            // When the closed run's action is called with e1 only now, and the resumed run then reads nothing
            on(closedRun, () -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
            Thread.sleep(INTERVAL.toMillis() + 50);
            boolean saved = wrapped.readNothing("id", QUIET);

            // Then
            assertThat(saved).as("quiet save wanted while e2, declined, is the last event the resumed run delivered").isFalse();
            assertThat(storage.read("id")).as("checkpoint").isEqualTo(new StringBasedCheckpoint("e1"));
        } finally {
            closedRun.shutdownNow();
        }
    }

    @Test
    void a_closed_run_that_reads_once_more_without_delivering_does_not_keep_the_resumed_run_from_saving_its_quiet_position() throws Exception {
        // Given a resumed run that delivers e1, which the persist predicate declines, and then e2, which it stores, and
        // a closed run that reads once more between the resumed run's read of e2 and its delivery of it
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(declines("e1")).saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        ExecutorService closedRun = Executors.newSingleThreadExecutor();
        try {
            wrapped.beforeReading("id");
            wrapped.deliver("id", new StringBasedCheckpoint("e1"));
            wrapped.beforeReading("id");
            on(closedRun, () -> wrapped.beforeReading("id"));
            wrapped.deliver("id", new StringBasedCheckpoint("e2"));

            // When the resumed run then reads nothing
            Thread.sleep(INTERVAL.toMillis() + 50);
            boolean saved = wrapped.readNothing("id", QUIET);

            // Then
            assertThat(saved).as("quiet save wanted once the resumed run has stored e2").isTrue();
            assertThat(storage.read("id")).as("checkpoint").isEqualTo(QUIET);
        } finally {
            closedRun.shutdownNow();
        }
    }

    @Test
    void the_save_of_a_closed_run_that_started_before_the_resumed_run_is_refused_once_the_resumed_run_has_stored_a_later_event_and_the_quiet_position_is_still_saved() throws InterruptedException {
        // Given a closed run whose action for e1 returns only after a resumed run has delivered e1 again and then e2
        CountDownLatch e1Started = new CountDownLatch(1);
        CountDownLatch e1MayReturn = new CountDownLatch(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
            if (firstCall.compareAndSet(true, false)) {
                e1Started.countDown();
                awaitQuietly(e1MayReturn);
            }
        });
        Thread closedRun = Thread.ofPlatform().start(() -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
        assertThat(e1Started.await(5, SECONDS)).isTrue();
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        wrapped.deliver("id", new StringBasedCheckpoint("e2"));

        // When the closed run's action returns, and the resumed run then reads nothing for longer than the interval
        e1MayReturn.countDown();
        closedRun.join(5000);
        Checkpoint afterTheLateSave = storage.read("id");
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean saved = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(afterTheLateSave).as("checkpoint once the closed run's action has returned").isEqualTo(new StringBasedCheckpoint("e2"));
        assertThat(saved).as("quiet save wanted").isTrue();
        assertThat(storage.read("id")).as("checkpoint").isEqualTo(QUIET);
    }

    @Test
    void the_save_of_a_closed_run_whose_action_is_called_only_after_the_resumed_run_has_stored_a_later_event_is_refused_and_the_quiet_position_is_still_saved() throws Exception {
        // Given a closed run that read e1 before the pause, and a resumed run that reads and delivers e1 again and then e2
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, saveQuietPositionEvery(INTERVAL));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        ExecutorService closedRun = Executors.newSingleThreadExecutor();
        try {
            on(closedRun, () -> wrapped.beforeReading("id"));
            wrapped.beforeReading("id");
            wrapped.deliver("id", new StringBasedCheckpoint("e1"));
            wrapped.beforeReading("id");
            wrapped.deliver("id", new StringBasedCheckpoint("e2"));

            // When the closed run's action is called with e1 only now, and the resumed run then reads nothing
            on(closedRun, () -> wrapped.deliver("id", new StringBasedCheckpoint("e1")));
            Checkpoint afterTheLateSave = storage.read("id");
            Thread.sleep(INTERVAL.toMillis() + 50);
            boolean saved = wrapped.readNothing("id", QUIET);

            // Then
            assertThat(afterTheLateSave).as("checkpoint once the closed run's action has returned").isEqualTo(new StringBasedCheckpoint("e2"));
            assertThat(saved).as("quiet save wanted").isTrue();
            assertThat(storage.read("id")).as("checkpoint").isEqualTo(QUIET);
        } finally {
            closedRun.shutdownNow();
        }
    }

    @Test
    void a_quiet_save_that_wrote_and_threw_is_recovered_through_a_read_with_no_new_event() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        DurableSubscriptionModel model = subscribedAndStoredE1(flaky);
        flaky.writesThenThrows.add("quiet-1");

        // When
        boolean first = quietTickWhenWanted("quiet-1");
        boolean second = quietTickWhenWanted("quiet-2");

        // Then
        assertThat(List.of(first, second)).containsExactly(true, true);
        assertThat(flaky.operations).containsExactly("save quiet-1", "read", "save quiet-2");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-2"));
        assertThat(model.positionsRememberedForQuietSaves("id")).isLessThanOrEqualTo(2);
    }

    @Test
    void a_quiet_save_that_threw_without_writing_is_recovered_through_a_read_with_no_new_event() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        DurableSubscriptionModel model = subscribedAndStoredE1(flaky);
        flaky.throwsWithoutWriting.add("quiet-1");

        // When
        boolean first = quietTickWhenWanted("quiet-1");
        boolean second = quietTickWhenWanted("quiet-2");

        // Then
        assertThat(List.of(first, second)).containsExactly(true, true);
        assertThat(flaky.operations).containsExactly("save quiet-1", "read", "save quiet-2");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-2"));
        assertThat(model.positionsRememberedForQuietSaves("id")).isLessThanOrEqualTo(2);
    }

    @Test
    void a_quiet_save_that_wrote_and_threw_followed_by_one_that_threw_without_writing_is_recovered_through_a_read_with_no_new_event() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        DurableSubscriptionModel model = subscribedAndStoredE1(flaky);
        flaky.writesThenThrows.add("quiet-1");
        flaky.throwsWithoutWriting.add("quiet-2");

        // When
        quietTickWhenWanted("quiet-1");
        quietTickWhenWanted("quiet-2");
        quietTickWhenWanted("quiet-3");

        // Then
        assertThat(flaky.operations).containsExactly("save quiet-1", "read", "save quiet-2", "read", "save quiet-3");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-3"));
        assertThat(model.positionsRememberedForQuietSaves("id")).isLessThanOrEqualTo(2);
    }

    @Test
    void a_quiet_save_that_threw_without_writing_followed_by_one_that_wrote_and_threw_is_recovered_through_a_read_with_no_new_event() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        DurableSubscriptionModel model = subscribedAndStoredE1(flaky);
        flaky.throwsWithoutWriting.add("quiet-1");
        flaky.writesThenThrows.add("quiet-2");

        // When
        quietTickWhenWanted("quiet-1");
        quietTickWhenWanted("quiet-2");
        quietTickWhenWanted("quiet-3");

        // Then
        assertThat(flaky.operations).containsExactly("save quiet-1", "read", "save quiet-2", "read", "save quiet-3");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-3"));
        assertThat(model.positionsRememberedForQuietSaves("id")).isLessThanOrEqualTo(2);
    }

    @Test
    void many_quiet_saves_that_fail_in_a_row_keep_at_most_two_positions_remembered_and_are_recovered_through_a_read() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        DurableSubscriptionModel model = subscribedAndStoredE1(flaky);
        for (int i = 0; i < 25; i++) {
            (i % 2 == 0 ? flaky.writesThenThrows : flaky.throwsWithoutWriting).add("quiet-" + i);
        }

        // When
        List<Integer> remembered = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            quietTickWhenWanted("quiet-" + i);
            remembered.add(model.positionsRememberedForQuietSaves("id"));
        }
        quietTickWhenWanted("recovered");

        // Then
        assertThat(remembered).as("positions remembered after each failed quiet save").allMatch(count -> count <= 2);
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("recovered"));
    }

    @Test
    void a_read_that_throws_after_a_quiet_save_that_wrote_and_threw_keeps_the_quiet_position_unsaved_and_the_next_tick_reads_again() throws InterruptedException {
        // Given a quiet save that wrote and threw, and a read that throws while that position is stored
        FlakyStorage flaky = new FlakyStorage();
        subscribedAndStoredE1(flaky);
        flaky.writesThenThrows.add("quiet-1");
        flaky.readsThrowWhileStored.add("quiet-1");
        quietTickWhenWanted("quiet-1");

        // When two ticks read and throw, and the read then works
        quietTickWhenWanted("quiet-2");
        quietTickWhenWanted("quiet-3");
        flaky.readsThrowWhileStored.clear();
        quietTickWhenWanted("quiet-4");

        // Then
        assertThat(flaky.operations).containsExactly("save quiet-1", "read", "read", "read", "save quiet-4");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-4"));
    }

    @Test
    void a_delivery_save_that_wrote_and_threw_has_the_next_quiet_tick_read_and_save_the_quiet_position_with_no_new_event() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        subscribedAndStoredE1(flaky);
        flaky.writesThenThrows.add("e2");
        assertThatThrownBy(() -> wrapped.deliver("id", new StringBasedCheckpoint("e2"))).isInstanceOf(IllegalStateException.class);

        // When
        boolean wanted = quietTickWhenWanted("quiet-1");

        // Then
        assertThat(wanted).as("quiet save wanted").isTrue();
        assertThat(flaky.operations).containsExactly("save e2", "read", "save quiet-1");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-1"));
    }

    @Test
    void a_delivery_save_that_threw_without_writing_keeps_the_quiet_position_unsaved_until_a_delivery_stores_its_checkpoint() throws InterruptedException {
        // Given
        FlakyStorage flaky = new FlakyStorage();
        subscribedAndStoredE1(flaky);
        flaky.throwsWithoutWriting.add("e2");
        assertThatThrownBy(() -> wrapped.deliver("id", new StringBasedCheckpoint("e2"))).isInstanceOf(IllegalStateException.class);

        // When the next tick reads what is stored, and later ticks come with no new event
        boolean readTick = quietTickWhenWanted("quiet-1");
        Thread.sleep(10);
        boolean laterTick = wrapped.readNothing("id", new StringBasedCheckpoint("quiet-2"));
        wrapped.deliver("id", new StringBasedCheckpoint("e3"));
        boolean afterE3 = quietTickWhenWanted("quiet-3");

        // Then
        assertThat(readTick).as("tick that read what is stored").isTrue();
        assertThat(laterTick).as("quiet save wanted after the read found e1, which does not cover e2").isFalse();
        assertThat(afterE3).as("quiet save wanted once e3 stored its checkpoint").isTrue();
        assertThat(flaky.operations).containsExactly("save e2", "read", "save e3", "save quiet-3");
        assertThat(flaky.stored("id")).isEqualTo(new StringBasedCheckpoint("quiet-3"));
    }

    // A subscribe at START that then stores e1, so a quiet position covers the delivery that ended last
    private DurableSubscriptionModel subscribedAndStoredE1(FlakyStorage flaky) {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, flaky, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe("id", null, StartAt.checkpoint(START), __ -> {
        });
        wrapped.deliver("id", new StringBasedCheckpoint("e1"));
        flaky.operations.clear();
        return model;
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment
    private boolean quietTickWhenWanted(String quietPosition) throws InterruptedException {
        long deadline = System.nanoTime() + SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (wrapped.readNothing("id", new StringBasedCheckpoint(quietPosition))) {
                return true;
            }
            Thread.sleep(2);
        }
        return false;
    }

    // Fails by the content it is asked to save or read, never by how many calls came before
    private static final class FlakyStorage extends InMemoryCheckpointStorage {
        final List<String> operations = new CopyOnWriteArrayList<>();
        final Set<String> writesThenThrows = ConcurrentHashMap.newKeySet();
        final Set<String> throwsWithoutWriting = ConcurrentHashMap.newKeySet();
        final Set<String> readsThrowWhileStored = ConcurrentHashMap.newKeySet();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            String position = checkpoint.asString();
            operations.add("save " + position);
            if (throwsWithoutWriting.contains(position)) {
                throw new IllegalStateException("The save of " + position + " failed");
            }
            Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
            if (writesThenThrows.contains(position)) {
                throw new IllegalStateException("The reply of the save of " + position + " was lost");
            }
            return saved;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            operations.add("read");
            Checkpoint current = super.read(subscriptionId);
            if (current != null && readsThrowWhileStored.contains(current.asString())) {
                throw new IllegalStateException("The read failed while " + current.asString() + " is stored");
            }
            return current;
        }

        Checkpoint stored(String subscriptionId) {
            return super.read(subscriptionId);
        }
    }

    // A subscribe from the model default that finds START stored, as after an earlier run stored a position
    private static void subscribeFromTheStoredStart(DurableSubscriptionModel model, CheckpointStorage storage, Consumer<CloudEvent> action) {
        storage.save("id", START);
        model.subscribe("id", null, StartAt.subscriptionModelDefault(), action);
    }

    private static DurableSubscriptionModelConfig saveQuietPositionEvery(Duration interval) {
        return new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(interval);
    }

    private static Predicate<CloudEvent> declines(String position) {
        return cloudEvent -> !positionOf(cloudEvent).equals(position);
    }

    private static String positionOf(CloudEvent cloudEvent) {
        return CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(cloudEvent).asString();
    }

    // Holds the second read of the write version, the one for the second event, until answer is counted down
    private static CheckpointWriteVersionSource secondReadHeld(CountDownLatch reading, CountDownLatch answer) {
        AtomicInteger reads = new AtomicInteger();
        return subscriptionId -> {
            if (reads.incrementAndGet() == 2) {
                reading.countDown();
                awaitQuietly(answer);
            }
            return OptionalLong.empty();
        };
    }

    // Runs the step on the thread of the run and waits for it, so the test decides the order of what each run does
    private static void on(ExecutorService run, Runnable step) throws Exception {
        run.submit(step).get(5, SECONDS);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
}
