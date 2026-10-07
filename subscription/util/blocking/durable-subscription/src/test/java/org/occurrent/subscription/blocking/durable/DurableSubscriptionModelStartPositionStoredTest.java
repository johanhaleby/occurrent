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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a subscription from the model default may have its quiet position saved before its first event. That needs a
 * position of the subscription to be stored, whether the subscribe read it, recorded it, or a later evaluation of
 * the start position recorded it. The wrapped model is a fake the test reads from by hand.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStartPositionStoredTest {

    private static final Duration INTERVAL = Duration.ofMillis(200);
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint RECORDED = new StringBasedCheckpoint("recorded");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void the_quiet_position_is_saved_before_the_first_event_when_the_start_position_was_stored_before_a_subscribe_whose_wrapped_model_evaluates_it_inside_subscribe() throws InterruptedException {
        // Given
        storage.save("id", START);
        wrapped.evaluatesStartAtInSubscribe = true;
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent());
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
    void the_quiet_position_is_saved_before_the_first_event_when_an_evaluation_in_the_wrapped_subscribe_records_the_start_position() throws InterruptedException {
        // Given
        wrapped.globalCheckpoint = RECORDED;
        wrapped.evaluatesStartAtInSubscribe = true;
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent());
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
    void the_quiet_position_is_saved_before_the_first_event_when_the_subscribe_reads_the_stored_start_position_after_the_wrapped_subscribe_returned() throws InterruptedException {
        // Given
        storage.save("id", START);
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent());
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
    void the_quiet_position_is_saved_before_the_first_event_when_the_subscribe_records_the_start_position_after_the_wrapped_subscribe_returned() throws InterruptedException {
        // Given
        wrapped.globalCheckpoint = RECORDED;
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent());
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
    void the_quiet_position_is_saved_before_the_first_event_once_a_later_evaluation_records_the_start_position_the_subscribe_could_not() throws InterruptedException {
        // Given
        wrapped.evaluatesStartAtInSubscribe = true;
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent().startWhenNoStartPositionCanBeRecorded(true));
        model.subscribe("id", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedBeforeAPositionWasStored = wrapped.readNothing("id", QUIET);
        Checkpoint storedBeforeTheLaterEvaluation = storage.read("id");

        // When
        wrapped.globalCheckpoint = RECORDED;
        wrapped.evaluateStartAt("id");
        Thread.sleep(INTERVAL.toMillis() + 50);
        boolean savedOnceAPositionWasStored = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedBeforeAPositionWasStored).as("wanted the quiet position before a position was stored, once the interval had passed").isFalse();
        assertThat(storedBeforeTheLaterEvaluation).as("checkpoint stored before the later evaluation").isNull();
        assertThat(savedOnceAPositionWasStored).as("wanted the quiet position once the later evaluation stored a position").isTrue();
        assertThat(storage.read("id")).as("checkpoint stored").isEqualTo(QUIET);
    }

    @Test
    void no_quiet_position_is_saved_before_the_first_event_when_the_subscribe_could_record_no_start_position() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, neverStoresAnEvent().startWhenNoStartPositionCanBeRecorded(true));
        model.subscribe("id", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);

        // When
        boolean savedBeforeAnyEvent = wrapped.readNothing("id", QUIET);

        // Then
        assertThat(savedBeforeAnyEvent).as("wanted the quiet position before any event, once the interval had passed").isFalse();
        assertThat(storage.read("id")).as("checkpoint stored").isNull();
    }

    // No event stores a checkpoint, so only a quiet save can write one after the start position
    private static DurableSubscriptionModelConfig neverStoresAnEvent() {
        return new DurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(INTERVAL);
    }
}
