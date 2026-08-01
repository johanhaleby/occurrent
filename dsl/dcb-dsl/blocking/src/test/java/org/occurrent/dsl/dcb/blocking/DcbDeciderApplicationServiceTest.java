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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("DcbApplicationServiceDeciders")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbDeciderApplicationServiceTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private DcbApplicationService<DomainEvent> applicationService;
    private DcbDeciderApplicationService<DomainEvent> deciderApplicationService;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericDcbApplicationService<>(
                eventStore,
                cloudEventConverter,
                (DomainEvent event) -> Set.of(tagFor(event)),
                GenericDcbApplicationService.defaultRetryStrategy()
        );
        deciderApplicationService = new DcbDeciderApplicationService<>(applicationService);
        time = LocalDateTime.now();
    }

    @Nested
    @DisplayName("execute")
    class Execute {

        @Test
        void appends_the_events_decided_by_the_decider() {
            // When
            Optional<DcbAppendResult> result = deciderApplicationService.execute(new DefineName("Jane Doe"), nameDcbDecider());

            // Then
            assertAll(
                    () -> assertThat(result).isPresent(),
                    () -> assertThat(result.orElseThrow().eventCount()).isEqualTo(1),
                    () -> assertThat(readNameEvents("name")).containsExactly(new NameDefined("event-1", time, "name", "Jane Doe"))
            );
        }

        @Test
        void returns_empty_when_the_decision_produces_no_new_events() {
            // Given
            append(new NameDefined("event-0", time, "name", "Jane Doe"));

            // When
            Optional<DcbAppendResult> result = deciderApplicationService.execute(new DefineName("Jane Doe"), nameDcbDecider());

            // Then
            assertAll(
                    () -> assertThat(result).isEmpty(),
                    () -> assertThat(readNameEvents("name")).containsExactly(new NameDefined("event-0", time, "name", "Jane Doe"))
            );
        }

        @Test
        void appends_events_for_multiple_commands_that_resolve_to_the_same_boundary() {
            // When
            Optional<DcbAppendResult> result = deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider());

            // Then
            assertAll(
                    () -> assertThat(result).isPresent(),
                    () -> assertThat(readNameEvents("name")).containsExactly(
                            new NameDefined("event-1", time, "name", "Jane Doe"),
                            new NameWasChanged("event-2", time, "name", "John Doe")
                    )
            );
        }

        @Test
        void throws_IllegalArgumentException_when_the_decider_does_not_recognize_the_command() {
            // Given
            var unrecognizingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> command instanceof DefineName ? nameQuery("name") : null,
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            var thrown = catchThrowable(() -> deciderApplicationService.execute(new ChangeName("John Doe"), unrecognizingDecider));

            // Then
            assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void throws_IllegalArgumentException_when_commands_in_a_batch_resolve_to_different_boundaries() {
            // Given
            var perCommandDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> switch (command) {
                        case DefineName defineName -> nameQuery(defineName.name());
                        case ChangeName changeName -> nameQuery(changeName.name());
                        case NoOp __ -> nameQuery("name");
                    },
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            var thrown = catchThrowable(() -> deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), perCommandDecider));

            // Then
            assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void executes_under_the_boundary_it_is_given_without_asking_the_decider_for_one() {
            // Given a criteria function that counts how often it is asked
            AtomicInteger derivations = new AtomicInteger();
            var countingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> {
                        derivations.incrementAndGet();
                        return nameQuery("name");
                    },
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When the boundary is passed in rather than derived
            Optional<DcbAppendResult> result = deciderApplicationService.execute(nameQuery("name"), List.of(new DefineName("Jane Doe")), countingDecider);

            // Then the events landed and the decider was never asked to resolve a boundary
            assertAll(
                    () -> assertThat(result).isPresent(),
                    () -> assertThat(derivations).hasValue(0),
                    () -> assertThat(readNameEvents("name")).containsExactly(new NameDefined("event-1", time, "name", "Jane Doe"))
            );
        }
    }

    @Nested
    @DisplayName("executeAndReturnDecision")
    class ExecuteAndReturnDecision {

        @Test
        void returns_the_folded_state_plus_the_new_events() {
            // Given
            append(new NameDefined("event-0", time, "name", "Jane Doe"));

            // When
            Decider.Decision<String, DomainEvent> decision = deciderApplicationService.executeAndReturnDecision(new ChangeName("John Doe"), nameDcbDecider());

            // Then
            assertAll(
                    () -> assertThat(decision.state()).isEqualTo("John Doe"),
                    () -> assertThat(decision.events()).containsExactly(new NameWasChanged("event-2", time, "name", "John Doe"))
            );
        }

        @Test
        void throws_IllegalArgumentException_when_the_decider_does_not_recognize_the_command() {
            // Given
            var unrecognizingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> command instanceof DefineName ? nameQuery("name") : null,
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            var thrown = catchThrowable(() -> deciderApplicationService.executeAndReturnDecision(new ChangeName("John Doe"), unrecognizingDecider));

            // Then
            assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("executeAndReturnState")
    class ExecuteAndReturnState {

        @Test
        void returns_the_folded_state_after_multiple_commands() {
            // When
            deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider());
            String state = deciderApplicationService.executeAndReturnState(NoOp.INSTANCE, nameDcbDecider());

            // Then
            assertThat(state).isEqualTo("John Doe");
        }
    }

    @Nested
    @DisplayName("executeAndReturnEvents")
    class ExecuteAndReturnEvents {

        @Test
        void returns_the_new_events_for_multiple_commands_in_order() {
            // When
            List<DomainEvent> newEvents = deciderApplicationService.executeAndReturnEvents(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider());

            // Then
            assertAll(
                    () -> assertThat(newEvents).containsExactly(
                            new NameDefined("event-1", time, "name", "Jane Doe"),
                            new NameWasChanged("event-2", time, "name", "John Doe")
                    ),
                    () -> assertThat(readNameEvents("name")).containsExactlyElementsOf(newEvents)
            );
        }
    }

    // executeAndReturnState cannot carry a null state, so this decider uses a non-null String, with an empty string
    // for "no name yet".
    private Decider<NameCommand, String, DomainEvent> nameDecider() {
        return Decider.create(
                "",
                (NameCommand command, String state) -> switch (command) {
                    case DefineName defineName -> state.isEmpty() ? List.of(new NameDefined("event-1", time, "name", defineName.name())) : List.of();
                    case ChangeName changeName -> List.of(new NameWasChanged("event-2", time, "name", changeName.name()));
                    case NoOp __ -> List.of();
                },
                (state, event) -> event.name()
        );
    }

    private DcbDecider<NameCommand, String, DomainEvent> nameDcbDecider() {
        return DcbDecider.from(nameDecider(), command -> nameQuery("name"), event -> Set.of(tagFor(event)));
    }

    private void append(DomainEvent... events) {
        List<Tag> tags = List.of(Tag.of("name", "name"));
        var cloudEvents = cloudEventConverter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, tags))
                .toList();
        eventStore.append(cloudEvents);
    }

    private List<DomainEvent> readNameEvents(String nameId) {
        return cloudEventConverter.toDomainEvents(eventStore.read(nameQuery(nameId)).stream()).toList();
    }

    private static DcbCriteria nameQuery(String nameId) {
        return DcbCriteria.tags(Tag.of("name", nameId));
    }

    private static Tag tagFor(DomainEvent event) {
        return Tag.of("name", event.userId());
    }

    private sealed interface NameCommand {
    }

    private record DefineName(String name) implements NameCommand {
    }

    private record ChangeName(String name) implements NameCommand {
    }

    private enum NoOp implements NameCommand {
        INSTANCE
    }
}
