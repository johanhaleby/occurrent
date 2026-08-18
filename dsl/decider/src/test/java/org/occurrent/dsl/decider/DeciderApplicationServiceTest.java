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

package org.occurrent.dsl.decider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("ApplicationServiceDeciders")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DeciderApplicationServiceTest {

    private Decider<NameCommand, String, DomainEvent> decider;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private ApplicationService<DomainEvent> applicationService;
    private DeciderApplicationService<DomainEvent> deciderApplicationService;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        time = LocalDateTime.now();
        decider = Decider.create(
                "",
                (NameCommand command, String state) -> switch (command) {
                    case DefineName defineName -> List.of(new NameDefined("event-1", time, "name", defineName.name()));
                    case ChangeName changeName -> List.of(new NameWasChanged("event-2", time, "name", changeName.name()));
                },
                (state, event) -> event.name()
        );
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        deciderApplicationService = new DeciderApplicationService<>(applicationService);
    }

    @AfterEach
    void delete_all_events_in_event_store() {
        eventStore.deleteAll();
    }

    @Nested
    @DisplayName("execute")
    class Execute {

        @Test
        void appends_the_events_decided_for_a_single_command() {
            // Given
            var streamId = UUID.randomUUID();
            var command = new DefineName("Jane Doe");

            // When
            var result = deciderApplicationService.execute(streamId, command, decider);

            // Then
            assertThat(result.streamId()).isEqualTo(streamId.toString());
            assertThat(result.oldStreamVersion()).isEqualTo(0);
            assertThat(result.newStreamVersion()).isEqualTo(1);
        }

        @Test
        void appends_the_events_decided_for_a_single_command_given_a_string_stream_id() {
            // Given
            var streamId = UUID.randomUUID().toString();
            var command = new DefineName("Jane Doe");

            // When
            var result = deciderApplicationService.execute(streamId, command, decider);

            // Then
            assertThat(result.streamId()).isEqualTo(streamId);
            assertThat(result.oldStreamVersion()).isEqualTo(0);
            assertThat(result.newStreamVersion()).isEqualTo(1);
        }

        @Test
        void appends_the_events_decided_for_a_list_of_commands_in_order() {
            // Given
            var streamId = UUID.randomUUID();
            var command1 = new DefineName("John");
            var command2 = new ChangeName("Johan");

            // When
            var result = deciderApplicationService.execute(streamId, List.of(command1, command2), decider);

            // Then
            assertAll(
                    () -> assertThat(result.streamId()).isEqualTo(streamId.toString()),
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(0),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(2),
                    () -> assertThat(readNameEvents()).containsExactly(
                            new NameDefined("event-1", time, "name", "John"),
                            new NameWasChanged("event-2", time, "name", "Johan")
                    )
            );
        }
    }

    private sealed interface NameCommand {
    }

    private record DefineName(String name) implements NameCommand {
    }

    private record ChangeName(String name) implements NameCommand {
    }

    private List<DomainEvent> readNameEvents() {
        return eventStore.all().map(cloudEventConverter::toDomainEvent).toList();
    }
}
