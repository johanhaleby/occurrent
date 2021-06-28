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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.PolicySideEffect;
import org.occurrent.application.service.blocking.generic.support.CountNumberOfNamesDefinedPolicy;
import org.occurrent.application.service.blocking.generic.support.WhenNameDefinedThenCountAverageSizeOfNamePolicy;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.application.composition.command.CommandConversion.toStreamCommand;
import static org.occurrent.application.service.blocking.PolicySideEffect.executePolicy;

@DisplayName("generic application service")
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
                toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")));

        // Then
        assertAll(
                () -> assertThat(writeResult.getStreamId()).isEqualTo(streamId.toString()),
                () -> assertThat(writeResult.getStreamVersion()).isEqualTo(1L)
        );
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
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Tina")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Abbe")),
                    sideEffect);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Agnes")),
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
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")),
                    policy);

            applicationService.execute(UUID.randomUUID(),
                    toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Agnes")),
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
            applicationService.execute(streamId, toStreamCommand(events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")));

            applicationService.execute(streamId,
                    toStreamCommand(events -> Name.changeName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "Tina")),
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
        void retries_automatically_when_other_exception_than_write_condition_not_fulfilled_is_thrown_and_retry_strategy_is_configured_to_retry_this_expection() {
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
}