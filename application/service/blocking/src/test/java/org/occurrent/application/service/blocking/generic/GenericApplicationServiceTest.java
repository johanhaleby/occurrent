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

package org.occurrent.application.service.blocking.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.ExecuteFilter;
import org.occurrent.application.service.blocking.PolicySideEffect;
import org.occurrent.application.service.blocking.generic.support.CountNumberOfNamesDefinedPolicy;
import org.occurrent.application.service.blocking.generic.support.WhenNameDefinedThenCountAverageSizeOfNamePolicy;
import org.occurrent.domain.*;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.ConditionallyWriteToEventStream;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.api.blocking.ReadEventStream;
import org.occurrent.eventstore.api.blocking.UnconditionallyWriteToEventStream;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.eventstore.api.WriteCondition;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.application.composition.command.CommandConversion.toStreamCommand;
import static org.occurrent.application.service.blocking.ExecuteOptions.options;
import static org.occurrent.application.service.blocking.PolicySideEffect.executePolicy;

@SuppressWarnings("removal")
@DisplayName("generic application service")
@DisplayNameGeneration(ReplaceUnderscores.class)
public class GenericApplicationServiceTest {

    private ApplicationService<DomainEvent> applicationService;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;

    @BeforeEach
    void initialize_application_service() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        eventStore = new InMemoryEventStore();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
    }

    @Test
    void returns_write_result() {
        // Given
        UUID streamId = UUID.randomUUID();

        // When
        WriteResult writeResult = applicationService.execute(streamId,
                toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")));

        // Then
        assertAll(
                () -> assertThat(writeResult.streamId()).isEqualTo(streamId.toString()),
                () -> assertThat(writeResult.newStreamVersion()).isEqualTo(1L)
        );
    }

    @Test
    void supports_execute_options() {
        // Given
        String streamId = UUID.randomUUID().toString();
        cloudEventConverter = customCloudEventConverter();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        var initialEvents = cloudEventConverter.toCloudEvents(Stream.of(
                new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"),
                new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Mattias"))
        );
        eventStore.write(streamId, initialEvents);
        AtomicReference<String> sideEffectPayload = new AtomicReference<>("not-called");
        // When
        WriteResult writeResult = applicationService.execute(streamId,
                options().filter(ExecuteFilter.type(NameDefined.class)).sideEffect(events -> sideEffectPayload.set(events.findFirst().map(DomainEvent::name).orElse("empty"))),
                toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name")));

        // Then
        assertAll(
                () -> assertThat(writeResult.streamId()).isEqualTo(streamId),
                () -> assertThat(writeResult.newStreamVersion()).isEqualTo(3L),
                () -> assertThat(sideEffectPayload.get()).isEqualTo("New Name")
        );
    }

    @Test
    void supports_execute_filter_as_direct_execute_argument() {
        // Given
        String streamId = UUID.randomUUID().toString();
        cloudEventConverter = customCloudEventConverter();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(Stream.of(
                new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"),
                new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Mattias"))
        ));

        // When
        WriteResult writeResult = applicationService.execute(streamId, ExecuteFilter.type(NameDefined.class),
                toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name")));

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L);
    }

    @Test
    void supports_excluding_types_using_execute_filter() {
        // Given
        String streamId = UUID.randomUUID().toString();
        cloudEventConverter = customCloudEventConverter();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(Stream.of(
                new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"),
                new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Mattias"))
        ));
        AtomicReference<String> observedName = new AtomicReference<>("not-called");

        // When
        applicationService.execute(streamId,
                options().filter(ExecuteFilter.excludeTypes(NameWasChanged.class)).sideEffect(events -> observedName.set(events.findFirst().map(DomainEvent::name).orElse("empty"))),
                toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name")));

        // Then
        assertThat(observedName.get()).isEqualTo("New Name");
    }

    @Test
    void throws_when_execute_filter_is_used_with_event_store_that_does_not_support_filtered_reads() {
        // Given
        cloudEventConverter = customCloudEventConverter();
        applicationService = new GenericApplicationService<>(new EventStoreWithoutFilterSupport(eventStore), cloudEventConverter);

        // When
        Throwable throwable = catchThrowable(() -> applicationService.execute(UUID.randomUUID().toString(),
                ExecuteFilter.type(NameDefined.class),
                toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name"))));

        // Then
        assertThat(throwable)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support reading with a StreamReadFilter");
    }

    @Nested
    @DisplayName("side effects")
    class SideEffectsTest {

        @Test
        void are_executed_after_call_to_execute() {
            // Given
            WhenNameDefinedThenCountAverageSizeOfNamePolicy averageSizePolicy = new WhenNameDefinedThenCountAverageSizeOfNamePolicy();

            // When
            PolicySideEffect<DomainEvent> sideEffect = executePolicy(NameDefined.class, averageSizePolicy::whenNameDefinedThenCountAverageSizeOfName);
            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "tina", "Tina")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "abbe", "Abbe")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "agnes", "Agnes")),
                    sideEffect);

            // Then
            assertThat(averageSizePolicy.getAverageSizeOfName()).isEqualTo(4);
        }

        @Test
        void are_composable_using_and_then_execute_another_policy() {
            // Given
            CountNumberOfNamesDefinedPolicy countPolicy = new CountNumberOfNamesDefinedPolicy();
            WhenNameDefinedThenCountAverageSizeOfNamePolicy averageSizePolicy = new WhenNameDefinedThenCountAverageSizeOfNamePolicy();

            PolicySideEffect<DomainEvent> policy = PolicySideEffect.<DomainEvent, NameDefined>executePolicy(NameDefined.class, averageSizePolicy::whenNameDefinedThenCountAverageSizeOfName)
                    .andThenExecuteAnotherPolicy(NameDefined.class, countPolicy::whenNameDefinedThenCountHowManyNamesThatHaveBeenDefined);

            // When
            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")),
                    policy);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "agnes", "Agnes")),
                    policy);

            // Then
            assertAll(
                    () -> assertThat(averageSizePolicy.getAverageSizeOfName()).isEqualTo(5),
                    () -> assertThat(countPolicy.getNumberOfNamesDefined()).isEqualTo(2)
            );
        }

        @Test
        void are_not_executed_when_event_is_not_in_stream() {
            // Given
            WhenNameDefinedThenCountAverageSizeOfNamePolicy averageSizePolicy = new WhenNameDefinedThenCountAverageSizeOfNamePolicy();
            UUID streamId = UUID.randomUUID();

            // When
            applicationService.execute(streamId, toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")));

            applicationService.execute(streamId,
                    toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "tina", "Tina")),
                    executePolicy(NameDefined.class, averageSizePolicy::whenNameDefinedThenCountAverageSizeOfName));

            // Then
            assertThat(averageSizePolicy.getAverageSizeOfName()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("retries")
    class RetryTest {

        @Test
        void automatically_retries_when_write_condition_not_fulfilled_is_thrown() {
            // Given
            UUID streamId = UUID.randomUUID();
            AtomicInteger atomicInteger = new AtomicInteger();

            // When
            applicationService.execute(streamId, stream -> {
                if (atomicInteger.getAndIncrement() == 0) {
                    throw new WriteConditionNotFulfilledException(streamId.toString(), 2L, null, null);
                } else {
                    return Stream.empty();
                }
            });

            // Then
            assertThat(atomicInteger.get()).isEqualTo(2);
        }

        @Test
        void does_not_retry_automatically_when_other_exception_than_write_condition_not_fulfilled_is_thrown() {
            // Given
            UUID streamId = UUID.randomUUID();

            // When
            Throwable throwable = catchThrowable(() -> applicationService.execute(streamId, stream -> {
                throw new IllegalArgumentException("expected");
            }));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected");
        }

        @Test
        void retries_automatically_when_other_exception_than_write_condition_not_fulfilled_is_thrown_and_retry_strategy_is_configured_to_retry_this_exception() {
            // Given
            UUID streamId = UUID.randomUUID();
            AtomicInteger atomicInteger = new AtomicInteger();
            applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter,
                    GenericApplicationService.defaultRetryStrategy().mapRetryPredicate(p -> p.or(IllegalArgumentException.class::isInstance)));

            // When
            applicationService.execute(streamId, stream -> {
                if (atomicInteger.getAndIncrement() == 0) {
                    throw new IllegalArgumentException("expected");
                } else {
                    return Stream.empty();
                }
            });

            // Then
            assertThat(atomicInteger.get()).isEqualTo(2);
        }

        @Test
        void number_of_retries_are_restricted_by_default() {
            // Given
            UUID streamId = UUID.randomUUID();

            // When
            Throwable throwable = catchThrowable(() -> applicationService.execute(streamId, stream -> {
                throw new WriteConditionNotFulfilledException(streamId.toString(), 2L, null, null);
            }));

            // Then
            assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class);
        }
    }

    private CloudEventConverter<DomainEvent> customCloudEventConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new GenericCloudEventConverter<DomainEvent>(
                cloudEvent -> {
                    try {
                        return switch (cloudEvent.getType()) {
                            case "name-defined-v1" -> objectMapper.readValue(cloudEvent.getData().toBytes(), NameDefined.class);
                            case "name-was-changed-v1" -> objectMapper.readValue(cloudEvent.getData().toBytes(), NameWasChanged.class);
                            default -> throw new IllegalArgumentException("Unsupported event type " + cloudEvent.getType());
                        };
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                event -> {
                    try {
                        return CloudEventBuilder.v1()
                                .withId(event.eventId())
                                .withSource(URI.create("http://name"))
                                .withType(customCloudEventType(event.getClass()))
                                .withTime(event.timestamp().toInstant().atOffset(ZoneOffset.UTC))
                                .withSubject(event.name())
                                .withDataContentType("application/json")
                                .withData(objectMapper.writeValueAsBytes(event))
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                GenericApplicationServiceTest::customCloudEventType
        );
    }

    private static String customCloudEventType(Class<? extends DomainEvent> eventType) {
        if (eventType.equals(NameDefined.class)) {
            return "name-defined-v1";
        } else if (eventType.equals(NameWasChanged.class)) {
            return "name-was-changed-v1";
        }
        return eventType.getName();
    }

    private static final class EventStoreWithoutFilterSupport implements EventStore {
        private final InMemoryEventStore delegate;

        private EventStoreWithoutFilterSupport(InMemoryEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
            return delegate.read(streamId, skip, limit);
        }

        @Override
        public WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
            return ((ConditionallyWriteToEventStream) delegate).write(streamId, writeCondition, events);
        }

        @Override
        public WriteResult write(String streamId, Stream<CloudEvent> events) {
            return ((UnconditionallyWriteToEventStream) delegate).write(streamId, events);
        }

        @Override
        public boolean exists(String streamId) {
            return delegate.exists(streamId);
        }
    }
}
