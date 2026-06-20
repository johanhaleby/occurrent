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

package org.occurrent.dsl.dcb.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionsTest {

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private DcbSubscriptions<DomainEvent> dcbSubscriptions;
    private LocalDateTime time;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        dcbSubscriptions = new DcbSubscriptions<>(subscriptionModel, cloudEventConverter);
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void delivers_only_matching_dcb_events() {
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        dcbSubscriptions.subscribe("subscription", DcbQuery.tagsAllOf("name:1"), (DomainEvent event) -> received.add(event));

        NameDefined matching = new NameDefined("eventId1", time, "name", "Some Doe");
        append("name:1", matching);
        append("other:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));

        await().untilAsserted(() -> assertThat(received).containsExactly(matching));
    }

    @Test
    void metadata_overload_exposes_dcb_position_and_tags() {
        CopyOnWriteArrayList<OptionalLong> positions = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Set<String>> tags = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<DomainEvent> events = new CopyOnWriteArrayList<>();

        dcbSubscriptions.subscribeWithMetadata("subscription", DcbQuery.tagsAllOf("name:1"), (metadata, event) -> {
            positions.add(metadata.dcbPosition());
            tags.add(metadata.dcbTags());
            events.add(event);
        });

        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        append(List.of("name:1", "tenant:2"), nameDefined);

        await().untilAsserted(() -> {
            assertThat(events).containsExactly(nameDefined);
            assertThat(positions).containsExactly(OptionalLong.of(1));
            assertThat(tags.get(0)).containsExactlyInAnyOrder("name:1", "tenant:2");
        });
    }

    private void append(String tag, DomainEvent... events) {
        append(List.of(tag), events);
    }

    private void append(List<String> tags, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, tags))
                .toList();
        eventStore.append("dcb:partition:0", cloudEvents);
    }
}
