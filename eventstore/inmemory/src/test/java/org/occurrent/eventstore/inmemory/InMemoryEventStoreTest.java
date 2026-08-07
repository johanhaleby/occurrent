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

package org.occurrent.eventstore.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.filter.Filter.data;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@SuppressWarnings("ConstantConditions")
@DisplayNameGeneration(ReplaceUnderscores.class)
public class InMemoryEventStoreTest {

    private static final URI NAME_SOURCE = URI.create("http://name");
    private ObjectMapper objectMapper;

    @BeforeEach
    void create_object_mapper() {
        objectMapper = new ObjectMapper();
    }

    /**
     * The conformance suite asserts that a store declaring it cannot filter on the data payload rejects
     * {@code Filter.data(..)}, but only the type, since no other store can be held to this wording. The message names
     * the issue tracking the missing feature, so it is worth keeping where it belongs.
     */
    @Test
    void rejecting_a_data_filter_says_how_to_enable_one() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        // An event has to be there for the filter to be evaluated at all, since a query over nothing never reaches it.
        unconditionallyPersist(eventStore, "name1", new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name"));

        Throwable thrown = catchThrowable(() -> eventStore.query(data("name", eq("name2"))).forEach(__ -> {
        }));

        assertThat(thrown)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("was not given a DataFieldReader")
                .hasMessageContaining("occurrent-common-inmemory-filter-matching-jackson");
    }

    @Nested
    @DisplayName("duplicates")
    class DuplicateTest {

        @Test
        void writing_events_with_same_id_and_source_to_an_empty_event_stream_throws_duplicate_cloud_event_exception() {
            // Given
            InMemoryEventStore inMemoryEventStore = new InMemoryEventStore();
            LocalDateTime now = LocalDateTime.now();
            String eventId = UUID.randomUUID().toString();

            // When
            DomainEvent event1 = new NameDefined(eventId, now, "name", "John Doe");
            DomainEvent event2 = new NameWasChanged(eventId, now, "name", "Jan Doe");
            Throwable throwable = catchThrowable(() -> unconditionallyPersist(inMemoryEventStore, "name", Stream.of(event1, event2)));

            // Then
            assertThat(throwable)
                    .isExactlyInstanceOf(DuplicateCloudEventException.class)
                    .hasMessage("Duplicate CloudEvent detected with id " + eventId + " and source " + NAME_SOURCE + ".");
        }

        @Test
        void writing_events_with_same_id_and_source_to_an_existing_event_stream_throws_duplicate_cloud_event_exception() {
            // Given
            InMemoryEventStore inMemoryEventStore = new InMemoryEventStore();
            LocalDateTime now = LocalDateTime.now();
            String eventId = UUID.randomUUID().toString();

            unconditionallyPersist(inMemoryEventStore, "name", new NameDefined(eventId, now, "name", "John Doe"));

            // When
            Throwable throwable = catchThrowable(() -> unconditionallyPersist(inMemoryEventStore, "name", new NameWasChanged(eventId, now, "name", "Jan Doe")));

            // Then
            assertThat(throwable)
                    .isExactlyInstanceOf(DuplicateCloudEventException.class)
                    .hasMessage("Duplicate CloudEvent detected with id " + eventId + " and source " + NAME_SOURCE + ".");
        }
    }

    @Nested
    @DisplayName("listener")
    class Listener {

        @Test
        void listener_is_does_not_get_called_with_any_events_when_writing_empty_set_of_events() {
            // Given
            CopyOnWriteArrayList<DomainEvent> events = new CopyOnWriteArrayList<>();

            InMemoryEventStore inMemoryEventStore = new InMemoryEventStore(list -> events.addAll(list.stream().map(deserialize(objectMapper)).toList()));

            // When
            unconditionallyPersist(inMemoryEventStore, "name", Stream.empty());

            // Then
            assertThat(events).isEmpty();
        }

        @Test
        void listener_is_invoked_synchronously_when_the_first_events_are_written_to_a_stream() {
            // Given
            CopyOnWriteArrayList<DomainEvent> events = new CopyOnWriteArrayList<>();

            InMemoryEventStore inMemoryEventStore = new InMemoryEventStore(list -> events.addAll(list.stream().map(deserialize(objectMapper)).toList()));
            LocalDateTime now = LocalDateTime.now();

            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "Jan Doe");

            // When
            unconditionallyPersist(inMemoryEventStore, "name", Stream.of(event1, event2));

            // Then
            assertThat(events).containsExactly(event1, event2);
        }

        @Test
        void listener_is_invoked_synchronously_when_the_additional_events_are_written_to_a_stream() {
            // Given
            CopyOnWriteArrayList<DomainEvent> events = new CopyOnWriteArrayList<>();

            InMemoryEventStore inMemoryEventStore = new InMemoryEventStore(list -> events.addAll(list.stream().map(deserialize(objectMapper)).toList()));
            LocalDateTime now = LocalDateTime.now();

            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "Jan Doe");

            unconditionallyPersist(inMemoryEventStore, "name", Stream.of(event1, event2));

            DomainEvent event3 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "Jan Doe1");
            DomainEvent event4 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(3), "name", "Jan Doe2");

            // When
            unconditionallyPersist(inMemoryEventStore, "name", Stream.of(event3, event4));

            // Then
            assertThat(events).containsExactly(event1, event2, event3, event4);
        }
    }

    // The suites below are not covered by the TCK's query and operations conformance suites (which assert
    // in-memory-specific behavior, or exercise attributes/edge cases the suites don't touch), so they stay.

    @Nested
    @DisplayName("concurrent queries")
    class ConcurrentQueriesTest {
        private InMemoryEventStore inMemoryEventStore;

        @BeforeEach
        void create_event_store() {
            inMemoryEventStore = new InMemoryEventStore();
        }

        @Test
        void query_consumed_concurrently_with_writes_to_new_streams_does_not_throw_and_returns_consistent_result() throws Exception {
            // Given
            LocalDateTime now = LocalDateTime.now();
            int numberOfWrites = 500;
            CountDownLatch writerStarted = new CountDownLatch(1);
            AtomicReference<Throwable> readerFailure = new AtomicReference<>();
            ExecutorService executor = Executors.newSingleThreadExecutor();

            // When
            Future<?> writer = executor.submit(() -> {
                writerStarted.countDown();
                for (int i = 0; i < numberOfWrites; i++) {
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(i), "name" + i, "name" + i);
                    unconditionallyPersist(inMemoryEventStore, "stream" + i, nameDefined);
                }
            });

            // Consume the lazily evaluated query stream (sort/skip/limit happen on the terminal operation)
            // while the writer keeps structurally modifying the backing map.
            assertThat(writerStarted.await(10, TimeUnit.SECONDS)).as("Writer thread should start within 10 seconds").isTrue();
            while (!writer.isDone()) {
                try {
                    List<CloudEvent> result = inMemoryEventStore.query(Filter.all(), 0, Integer.MAX_VALUE, SortBy.natural(DESCENDING)).collect(Collectors.toList());
                    // The snapshot must be internally consistent: no duplicate events from an in-flight write.
                    assertThat(result).doesNotHaveDuplicates();
                } catch (Throwable t) {
                    readerFailure.set(t);
                    break;
                }
            }

            writer.get(60, TimeUnit.SECONDS);
            executor.shutdownNow();

            // Then
            assertThat(readerFailure.get()).as("Concurrent query must not fail while writes happen").isNull();
            List<CloudEvent> finalResult = inMemoryEventStore.query(Filter.all(), 0, Integer.MAX_VALUE, SortBy.natural(DESCENDING)).collect(Collectors.toList());
            assertThat(finalResult).hasSize(numberOfWrites);
        }
    }

    private void unconditionallyPersist(EventStore inMemoryEventStore, String eventStreamId, DomainEvent event) {
        unconditionallyPersist(inMemoryEventStore, eventStreamId, Stream.of(event));
    }

    private void unconditionallyPersist(EventStore inMemoryEventStore, String eventStreamId, List<DomainEvent> events) {
        unconditionallyPersist(inMemoryEventStore, eventStreamId, events.stream());
    }

    private WriteResult unconditionallyPersist(EventStore inMemoryEventStore, String eventStreamId, Stream<DomainEvent> events) {
        return inMemoryEventStore.write(eventStreamId, events.map(convertDomainEventToCloudEvent(objectMapper)).collect(Collectors.toList()));
    }

    private WriteResult conditionallyPersist(EventStore inMemoryEventStore, String eventStreamId, WriteCondition writeCondition, Stream<DomainEvent> events) {
        return inMemoryEventStore.write(eventStreamId, writeCondition, events.map(convertDomainEventToCloudEvent(objectMapper)).collect(Collectors.toList()));
    }

    private static Function<DomainEvent, CloudEvent> convertDomainEventToCloudEvent(ObjectMapper objectMapper) {
        return e -> CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(NAME_SOURCE)
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build();
    }

    private Function<CloudEvent, DomainEvent> deserialize(ObjectMapper objectMapper) {
        return cloudEvent -> {
            try {
                return (DomainEvent) objectMapper.readValue(cloudEvent.getData().toBytes(), Class.forName(cloudEvent.getType()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
