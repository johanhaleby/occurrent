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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries.query;
import static org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries.queryWithPosition;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbDomainEventQueriesTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private DomainEventQueries<DomainEvent> domainEventQueries;
    private LocalDateTime time;

    @BeforeEach
    void createInstances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        domainEventQueries = new DomainEventQueries<>(eventStore, cloudEventConverter);
        time = LocalDateTime.now();
    }

    @Test
    void query_converts_matching_dcb_events_to_domain_events() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined, nameWasChanged);

        List<DomainEvent> events = query(domainEventQueries, DcbQuery.tagsAllOf("name:1")).toList();

        assertThat(events).containsExactly(nameDefined, nameWasChanged);
    }

    @Test
    void query_honors_read_options_after_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined);
        append("name:1", nameWasChanged);

        List<DomainEvent> events = query(domainEventQueries, DcbQuery.tagsAllOf("name:1"), DcbReadOptions.afterSequencePosition(1)).toList();

        assertThat(events).containsExactly(nameWasChanged);
    }

    @Test
    void query_with_position_preserves_last_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        append("name:1", nameDefined);
        append("other:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));

        DcbDomainEventStream<DomainEvent> eventStream = queryWithPosition(domainEventQueries, DcbQuery.tagsAllOf("name:1"));

        assertThat(eventStream.events()).containsExactly(nameDefined);
        assertThat(eventStream.stream()).containsExactly(nameDefined);
        assertThat(eventStream.lastSequencePosition()).isEqualTo(2);
    }

    @Test
    void java_callers_can_subscribe_to_dcb_events_with_regular_event_metadata() {
        InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
        InMemoryEventStore eventStoreWithSubscriptions = new InMemoryEventStore(subscriptionModel);
        CopyOnWriteArrayList<EventMetadata> metadata = new CopyOnWriteArrayList<>();

        DcbSubscriptionsKt.subscribeDcbWithMetadata(subscriptionModel, "subscription", cloudEventConverter, DcbQuery.tagsAllOf("name:1"), null, true, (eventMetadata, event) -> {
            metadata.add(eventMetadata);
            return kotlin.Unit.INSTANCE;
        });

        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(new NameDefined("eventId1", time, "name", "Some Doe")))
                .map(event -> DcbCloudEvents.withTags(event, List.of("name:1")))
                .toList();
        eventStoreWithSubscriptions.append("dcb:partition:0", cloudEvents);

        assertThat(metadata).hasSize(1);
        assertThat(metadata.get(0).getStreamId()).isEqualTo("dcb:partition:0");
        assertThat(metadata.get(0).getStreamVersion()).isEqualTo(1);
    }

    private void append(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(tag)))
                .toList();
        eventStore.append("dcb:partition:0", cloudEvents);
    }
}
