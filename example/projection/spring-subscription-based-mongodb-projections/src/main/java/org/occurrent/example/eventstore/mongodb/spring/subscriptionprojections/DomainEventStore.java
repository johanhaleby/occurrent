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

package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Component
public class DomainEventStore {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public DomainEventStore(EventStore eventStore, ObjectMapper objectMapper, DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    public void append(UUID id, List<DomainEvent> events) {
        eventStore.write(id.toString(), serialize(id, events));
    }

    public EventStream<DomainEvent> loadEventStream(UUID id) {
        return eventStore.read(id.toString()).map(deserializeCloudEventToDomainEvent::deserialize);
    }

    private Stream<CloudEvent> serialize(UUID id, List<DomainEvent> events) {
        return events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(id.toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }
}