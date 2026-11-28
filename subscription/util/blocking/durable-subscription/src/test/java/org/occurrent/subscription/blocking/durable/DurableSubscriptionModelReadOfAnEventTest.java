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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * The read an event was delivered from, on a wrapped model that reads on the thread that delivers, asks the quiet
 * position listener before each read and retries an action with the same event instance. A retry of an event belongs
 * to the run that read it, also after a pause and a resume. An event of no read of its own belongs to the latest one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelReadOfAnEventTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint STORED_BEFORE_THE_RESUME = new StringBasedCheckpoint("stored-before-the-resume");
    private static final Checkpoint FIRST_EVENT = new StringBasedCheckpoint("first-event");
    private static final Checkpoint SECOND_EVENT = new StringBasedCheckpoint("second-event");

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();

    @Test
    void a_retry_of_the_same_event_instance_after_a_pause_and_a_resume_is_still_refused_since_it_was_read_before_the_resume() {
        // Given
        AtomicInteger attempts = new AtomicInteger();
        DurableSubscriptionModel model = subscribedModel(cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("the first attempt failed");
            }
        });
        storage.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_RESUME);
        Consumer<CloudEvent> action = wrapped.actions.get(SUBSCRIPTION_ID);
        wrapped.beforeReading(SUBSCRIPTION_ID);
        CloudEvent event = eventAt(FIRST_EVENT);
        assertThatThrownBy(() -> action.accept(event)).isInstanceOf(IllegalStateException.class);
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        action.accept(event);

        // Then
        assertThat(attempts).as("attempts").hasValue(2);
        assertThat(wrapped.resumedAt).as("positions the wrapped model resumed at").hasSize(1);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the retry").isEqualTo(STORED_BEFORE_THE_RESUME);
    }

    @Test
    void a_different_event_delivered_on_the_thread_without_a_new_read_is_saved_after_a_pause_and_a_resume() {
        // Given
        DurableSubscriptionModel model = subscribedModel(cloudEvent -> {
        });
        Consumer<CloudEvent> action = wrapped.actions.get(SUBSCRIPTION_ID);
        wrapped.beforeReading(SUBSCRIPTION_ID);
        action.accept(eventAt(FIRST_EVENT));
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        action.accept(eventAt(SECOND_EVENT));

        // Then
        assertThat(wrapped.resumedAt).as("positions the wrapped model resumed at").hasSize(1);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the second event").isEqualTo(SECOND_EVENT);
    }

    private DurableSubscriptionModel subscribedModel(Consumer<CloudEvent> action) {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofHours(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), action);
        return model;
    }

    private static CloudEvent eventAt(Checkpoint checkpoint) {
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("type").build();
        return new CheckpointAwareCloudEvent(cloudEvent, checkpoint);
    }
}
