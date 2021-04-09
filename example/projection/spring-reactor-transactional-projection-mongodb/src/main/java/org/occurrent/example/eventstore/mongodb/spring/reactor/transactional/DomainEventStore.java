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

package org.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

@Component
public class DomainEventStore {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public DomainEventStore(EventStore eventStore, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
    }

    public Mono<Void> append(UUID id, long expectedVersion, List<DomainEvent> events) {
        return eventStore.write(id.toString(), expectedVersion, serialize(events)).then();
    }

    public Mono<EventStream<DomainEvent>> loadEventStream(UUID id) {
        return eventStore
                .read(id.toString())
                .map(es -> es.map(this::deserialize));
    }

    private Flux<CloudEvent> serialize(List<DomainEvent> events) {
        return Flux.fromIterable(events)
                .map(e -> CloudEventBuilder.v1()
                        .withId(e.getEventId())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getName())
                        .withTime(TimeConversion.toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }

    @SuppressWarnings("ConstantConditions")
    private DomainEvent deserialize(CloudEvent cloudEvent) {
        try {
            return (DomainEvent) objectMapper.readValue(cloudEvent.getData().toBytes(), Class.forName(cloudEvent.getType()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
