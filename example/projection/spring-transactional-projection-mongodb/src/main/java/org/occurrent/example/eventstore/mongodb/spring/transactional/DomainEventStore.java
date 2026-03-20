/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.eventstore.mongodb.spring.transactional;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import tools.jackson.databind.ObjectMapper;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Component
public class DomainEventStore {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public DomainEventStore(EventStore eventStore, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
    }

    public void append(UUID id, long expectedVersion, List<DomainEvent> events) {
        eventStore.write(id.toString(), expectedVersion, serialize(id, events));
    }

    public EventStream<DomainEvent> loadEventStream(UUID id) {
        return eventStore.read(id.toString()).map(this::deserialize);
    }

    private Stream<CloudEvent> serialize(UUID id, List<DomainEvent> events) {
        CloudEventConverter<DomainEvent> cloudEventConverter = converter(__ -> id.toString());
        return events.stream()
                .map(cloudEventConverter::toCloudEvent);
    }

    private DomainEvent deserialize(CloudEvent cloudEvent) {
        return converter(DomainEvent::eventId).toDomainEvent(cloudEvent);
    }

    CloudEventConverter<DomainEvent> converter(Function<DomainEvent, String> idMapper) {
        return new JacksonCloudEventConverter.Builder<DomainEvent>(objectMapper, URI.create("http://name"))
                .idMapper(idMapper)
                .typeMapper(ReflectionCloudEventTypeMapper.simple(DomainEvent.class))
                .subjectMapper(DomainEvent::name)
                .timeMapper(event -> toLocalDateTime(event.timestamp()).atOffset(UTC))
                .build();
    }
}
