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

package org.occurrent.example.eventstore.mongodb.spring.transactional;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DomainEventStoreTest {

    @Test
    void domain_event_converter_round_trips_without_default_typing() {
        DomainEventStore store = new DomainEventStore(Mockito.mock(org.occurrent.eventstore.api.blocking.EventStore.class), new ObjectMapper());
        DomainEvent event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "user-1", "John Doe");

        CloudEvent cloudEvent = store.converter(__ -> "aggregate-1").toCloudEvent(event);

        assertThat(cloudEvent.getId()).isEqualTo("aggregate-1");
        assertThat(cloudEvent.getType()).isEqualTo(NameDefined.class.getSimpleName());
        assertThat(cloudEvent.getSubject()).isEqualTo("John Doe");
        assertThat(store.converter(DomainEvent::eventId).toDomainEvent(cloudEvent)).isEqualTo(event);
    }
}
