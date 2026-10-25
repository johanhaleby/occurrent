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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A takeover of a replay without the live delegate replays again from the marked form of a time replay. On a store
 * that writes no positions that form must replay by time, never be taken for a global position the store cannot
 * replay.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupMarkedTimeTakeoverTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    @Timeout(60)
    void a_takeover_of_a_replay_without_the_live_delegate_on_a_store_writing_no_positions_replays_again_by_time() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        NameDefined writtenInTheGap = nameDefined("writtenInTheGap");
        LiveStartLosingSubscriptionModel liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, false, false, () -> write(eventStore, writtenInTheGap));
        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        StartAt withoutTheLiveDelegate = StartAt.dynamic(context -> context.hasSubscriptionModelType(LiveStartLosingSubscriptionModel.class) ? null : StartAtTime.beginningOfTime());
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription started = catchup.subscribe("subscription", withoutTheLiveDelegate, (CloudEvent event) -> received.add(cloudEventConverter.toDomainEvent(event)));
            boolean startedInTime = started.waitUntilStarted(Duration.ofSeconds(10));

            // Then
            assertThat(liveModel.lostLiveStarts).as("the live starts the live model lost").hasSize(1);
            assertThat(received).as("the history, and then all of it again by time together with what was written in between")
                    .containsExactlyElementsOf(Stream.concat(history.stream(), Stream.concat(history.stream(), Stream.of(writtenInTheGap))).toList());
            assertThat(startedInTime).as("started").isTrue();
            assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1).doesNotContainAnyElementsOf(liveModel.lostLiveStarts);
            assertThat(liveModel.restartedFromThePresent).as("restarts from the present").isEmpty();
            NameDefined afterHandOver = nameDefined("afterHandOver");
            write(eventStore, afterHandOver);
            await().untilAsserted(() -> assertThat(received).as("received").endsWith(writtenInTheGap, afterHandOver));
        } finally {
            catchup.shutdown();
        }
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(InMemoryEventStore eventStore, DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
