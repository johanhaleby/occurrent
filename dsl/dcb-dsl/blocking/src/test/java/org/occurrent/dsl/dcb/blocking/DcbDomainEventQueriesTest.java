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

import org.occurrent.dsl.dcb.DcbDomainEventStream;

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
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbDomainEventQueriesTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private DomainEventQueries<DomainEvent> domainEventQueries;
    private DcbDomainEventQueries<DomainEvent> dcbQueries;
    private LocalDateTime time;

    @BeforeEach
    void createInstances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        domainEventQueries = new DomainEventQueries<>(eventStore, cloudEventConverter);
        dcbQueries = new DcbDomainEventQueries<>(domainEventQueries);
        time = LocalDateTime.now();
    }

    @Test
    void query_converts_matching_dcb_events_to_domain_events() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined, nameWasChanged);

        List<DomainEvent> events = dcbQueries.query(DcbCriteria.tags(Tag.of("name", "1"))).toList();

        assertThat(events).containsExactly(nameDefined, nameWasChanged);
    }

    @Test
    void query_honors_read_options_after_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined);
        append("name:1", nameWasChanged);

        List<DomainEvent> events = dcbQueries.query(DcbCriteria.tags(Tag.of("name", "1")), DcbReadOptions.afterPosition(1)).toList();

        assertThat(events).containsExactly(nameWasChanged);
    }

    @Test
    void query_with_position_preserves_last_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        append("name:1", nameDefined);
        append("other:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));

        DcbDomainEventStream<DomainEvent> eventStream = dcbQueries.queryWithPosition(DcbCriteria.tags(Tag.of("name", "1")));

        assertThat(eventStream.events()).containsExactly(nameDefined);
        assertThat(eventStream.stream()).containsExactly(nameDefined);
        assertThat(eventStream.lastSequencePosition()).isEqualTo(2);
    }

    @Test
    void query_with_position_exposes_a_usable_consistency_token() {
        append("name:1", new NameDefined("eventId1", time, "name", "Some Doe"));

        DcbDomainEventStream<DomainEvent> eventStream = dcbQueries.queryWithPosition(DcbCriteria.tags(Tag.of("name", "1")));
        DcbConsistencyToken token = eventStream.consistencyToken();
        assertThat(token).isNotNull();

        // A matching event committed after the DSL read invalidates the token, so a conditional append carrying it back
        // to the store is correctly rejected. This proves the token flows through the DSL projection, not just the position.
        append("name:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));
        List<CloudEvent> newEvents = cloudEventConverter.toCloudEvents(List.of(new NameWasChanged("eventId3", time, "name", "Joe Doe"))).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.of("name", "1"))))
                .toList();

        assertThatThrownBy(() -> eventStore.append(newEvents, DcbAppendCondition.failIfEventsMatch(DcbCriteria.tags(Tag.of("name", "1")), token)))
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void java_callers_can_subscribe_to_dcb_events_with_regular_event_metadata() {
        InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
        InMemoryEventStore eventStoreWithSubscriptions = new InMemoryEventStore(subscriptionModel);
        CopyOnWriteArrayList<EventMetadata> metadata = new CopyOnWriteArrayList<>();

        DcbSubscriptions<DomainEvent> dcbSubscriptions = new DcbSubscriptions<>(subscriptionModel, cloudEventConverter);
        dcbSubscriptions.subscribeWithMetadata("subscription", DcbCriteria.tags(Tag.of("name", "1")), (dcbMetadata, event) -> metadata.add(dcbMetadata.eventMetadata()));

        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new NameDefined("eventId1", time, "name", "Some Doe"))).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.of("name", "1"))))
                .toList();
        eventStoreWithSubscriptions.append(cloudEvents);

        // The in-memory subscription model dispatches asynchronously, so wait for the callback like the sibling tests do.
        await().untilAsserted(() -> {
            assertThat(metadata).hasSize(1);
            assertThat(metadata.get(0).getStreamId()).startsWith("dcb:partition:");
            assertThat(metadata.get(0).getStreamVersion()).isPositive();
        });
    }

    @Test
    void types_returns_only_events_of_the_given_types() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined, nameWasChanged);

        List<NameWasChanged> byType = dcbQueries.types(NameWasChanged.class).toList();
        List<DomainEvent> byTypes = dcbQueries.types(NameDefined.class, NameWasChanged.class).toList();

        assertThat(byType).containsExactly(nameWasChanged);
        assertThat(byTypes).containsExactly(nameDefined, nameWasChanged);
    }

    @Test
    void tags_returns_events_matching_all_of_the_tags() {
        NameDefined taggedWithBoth = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged taggedWithNameOnly = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        appendTagged(List.of(Tag.of("name", "1"), Tag.of("tenant", "1")), taggedWithBoth);
        append("name:1", taggedWithNameOnly);

        assertThat(dcbQueries.tags(Tag.of("name", "1"), Tag.of("tenant", "1")).toList()).containsExactly(taggedWithBoth);
        assertThat(dcbQueries.tags("name:1", "tenant:1").toList()).containsExactly(taggedWithBoth);
    }

    @Test
    void tags_anyOf_returns_events_matching_any_of_the_tags() {
        NameDefined name = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged other = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", name);
        append("other:1", other);

        assertThat(dcbQueries.tagsAnyOf(Tag.of("name", "1"), Tag.of("other", "1")).toList()).containsExactly(name, other);
        assertThat(dcbQueries.tagsAnyOf("name:1", "other:1").toList()).containsExactly(name, other);
    }

    @Test
    void a_colon_less_string_is_a_value_less_tag() {
        // A tag no longer has to be key:value, so a colon-less string is a valid value-less tag rather than an error.
        assertThat(dcbQueries.tags("premium")).isNotNull();
    }

    @Test
    void a_blank_string_tag_is_rejected() {
        assertThatThrownBy(() -> dcbQueries.tags(" ")).isInstanceOf(IllegalArgumentException.class);
    }

    private void append(String tag, DomainEvent... events) {
        appendTagged(List.of(Tag.parse(tag)), events);
    }

    private void appendTagged(List<Tag> tags, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, tags))
                .toList();
        eventStore.append(cloudEvents);
    }
}
