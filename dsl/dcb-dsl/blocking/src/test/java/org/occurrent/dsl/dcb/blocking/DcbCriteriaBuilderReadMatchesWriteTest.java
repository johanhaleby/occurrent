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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A DCB read built from a domain event {@link Class} must match the CloudEvent type string the events were written
 * with. The correctness guard here is a {@link ReflectionCloudEventTypeMapper#simple(Class) simple-name} type mapper
 * shared by the write path and the {@code criteria()} builder: the CloudEvent type is {@code "NameDefined"}, never the
 * fully qualified {@link Class#getName()}. Had the builder resolved the class through {@code Class.getName()} instead of
 * the mapper, the criterion would carry {@code "org.occurrent.domain.NameDefined"} and match nothing, and this test
 * would fail.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCriteriaBuilderReadMatchesWriteTest {

    @Test
    void criteria_built_from_a_class_matches_events_written_with_the_same_type_mapper() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(DomainEvent.class))
                .idMapper(DomainEvent::eventId)
                .build();
        DcbDomainEventQueries<DomainEvent> queries = new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, converter));

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.now(), "name", "Some Doe");
        List<CloudEvent> cloudEvents = converter.toCloudEvents(Stream.of(nameDefined))
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.of("name", "1"))))
                .toList();
        eventStore.append(cloudEvents);

        // Guard: the events really were written under the simple name, not the FQN.
        assertThat(eventStore.all()).extracting(CloudEvent::getType).containsExactly("NameDefined");

        DcbCriteria criteria = queries.criteria().types(NameDefined.class);
        assertThat(queries.query(criteria).toList()).containsExactly(nameDefined);
    }
}
