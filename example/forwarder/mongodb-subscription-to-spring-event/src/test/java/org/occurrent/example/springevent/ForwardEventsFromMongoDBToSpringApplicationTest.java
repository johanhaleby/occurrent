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

package org.occurrent.example.springevent;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ForwardEventsFromMongoDBToSpringApplicationTest {

    @Test
    void domain_event_converter_round_trips_without_default_typing() {
        ForwardEventsFromMongoDBToSpringApplication application = new ForwardEventsFromMongoDBToSpringApplication();
        var converter = application.domainEventConverter(new ObjectMapper());

        assertRoundTrip(converter, new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name"));
        assertRoundTrip(converter, new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "another name"));
    }

    private static void assertRoundTrip(org.occurrent.application.converter.CloudEventConverter<DomainEvent> converter, DomainEvent event) {
        CloudEvent cloudEvent = converter.toCloudEvent(event);

        assertThat(cloudEvent.getType()).isEqualTo(event.getClass().getSimpleName());
        assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(event);
    }
}
