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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A catch-up replay a subscribe started, whose action throws {@link AttemptNotMade}, is held instead of running again
 * by itself, since a withdrawn event is delivered again only when the subscription is resumed or started.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupAttemptNotMadeHoldsTheReplayTest {

    private static final String ID = "subscription";
    private static final String WITHDRAWN = "event2";
    // The replay's first backoff after a failure is 100 ms, and the next is 200 ms, so a replay that runs again by itself does within this
    private static final Duration QUIET_PERIOD = Duration.ofSeconds(2);

    private final CopyOnWriteArrayList<String> deliveries = new CopyOnWriteArrayList<>();
    private final AtomicBoolean withdrawNext = new AtomicBoolean(true);
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        List.of("event1", WITHDRAWN, "event3").forEach(name -> write(new NameDefined(UUID.randomUUID().toString(), time, "name", name)));
        catchup = new StreamCatchupSubscriptionModel(new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, false, false, () -> {
        }).neverLosingAnOpen(), eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_replay_a_subscribe_started_whose_action_throws_AttemptNotMade_does_not_deliver_the_withdrawn_event_again_by_itself_and_counts_as_paused_until_it_is_resumed() {
        // Given
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), withdrawingTheEventOnce());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(deliveries).as("deliveries before the withdrawal").contains(WITHDRAWN));

        // When nothing pauses, stops or cancels the subscription, over a period well beyond the backoff after a failure
        // Then
        await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(10)).untilAsserted(() ->
                assertThat(deliveries).as("deliveries of the withdrawn event").filteredOn(WITHDRAWN::equals).hasSize(1));
        assertThat(catchup.isPaused(ID)).as("the subscription is paused").isTrue();

        // When
        catchup.resumeSubscription(ID);

        // Then
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(deliveries).as("deliveries of the withdrawn event after the resume").filteredOn(WITHDRAWN::equals).hasSize(2));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(deliveries).as("deliveries").endsWith("event2", "event3"));
        assertThat(catchup.isPaused(ID)).as("paused after the resume").isFalse();
    }

    // Records each event it is given, and throws AttemptNotMade the first time it is given the withdrawn one
    private Consumer<CloudEvent> withdrawingTheEventOnce() {
        return cloudEvent -> {
            String name = ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
            deliveries.add(name);
            if (WITHDRAWN.equals(name) && withdrawNext.getAndSet(false)) {
                throw new AttemptNotMade("not made");
            }
        };
    }

    private void write(DomainEvent event) {
        eventStore.write(event.eventId(), cloudEventConverter.toCloudEvents(List.of(event)));
    }
}
