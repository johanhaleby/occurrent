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

package org.occurrent.application.service.blocking.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.SynchronousEventDispatcher;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.Name;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericApplicationServiceSynchronousSubscriptionsTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;

    @BeforeEach
    void initialize() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        eventStore = new InMemoryEventStore();
    }

    @Test
    void dispatches_written_events_synchronously_before_execute_returns_and_enriched_with_stream_version() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(dispatcher)
                .build();
        String streamId = UUID.randomUUID().toString();

        applicationService.execute(streamId, events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"));

        assertThat(dispatcher.dispatched).hasSize(1);
        CloudEvent dispatched = dispatcher.dispatched.getFirst();
        assertThat(dispatched.getType()).isEqualTo("org.occurrent.domain.NameDefined");
        // The store enriches on write; the dispatched events are the re-read, enriched ones, not the pre-write converter output.
        assertThat(dispatched.getExtension("streamversion")).isEqualTo(1L);
    }

    @Test
    void does_not_dispatch_when_no_synchronous_subscriptions_are_registered() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        dispatcher.hasSubscriptions = false;
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(dispatcher)
                .build();

        applicationService.execute(UUID.randomUUID().toString(), events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"));

        assertThat(dispatcher.dispatched).isEmpty();
    }

    @Test
    void does_not_dispatch_when_the_command_produces_no_events() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(dispatcher)
                .build();

        applicationService.execute(UUID.randomUUID().toString(), events -> List.of());

        assertThat(dispatcher.dispatched).isEmpty();
    }

    private static final class RecordingDispatcher implements SynchronousEventDispatcher {
        private final List<CloudEvent> dispatched = new ArrayList<>();
        private boolean hasSubscriptions = true;

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents) {
            dispatched.addAll(writtenCloudEvents);
        }

        @Override
        public boolean hasSubscriptions() {
            return hasSubscriptions;
        }
    }
}
