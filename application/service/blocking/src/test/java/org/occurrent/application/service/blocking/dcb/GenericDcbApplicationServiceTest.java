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

package org.occurrent.application.service.blocking.dcb;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tagsAllOf;

@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceTest {

    @Test
    void reads_by_dcb_query_and_appends_with_tags_from_domain_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of("name:1"))));
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(event.name()),
                GenericDcbApplicationService.defaultRetryStrategy());

        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"), events -> {
            List<DomainEvent> currentEvents = events.toList();
            assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined");
            return Stream.of(new DomainEvent("NameChanged", "name:1"));
        });

        assertThat(result).hasValue(new DcbAppendResult(2, 2, 1));
        DcbEventStream eventStream = eventStore.read(tagsAllOf("name:1"));
        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged");
        assertThat(eventStore.all()).extracting(CloudEvent::getType).contains("NameDefined", "NameChanged");
    }

    @Test
    void does_not_append_when_domain_function_returns_no_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(event.name()));

        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"), events -> Stream.empty());

        assertThat(result).isEmpty();
        assertThat(eventStore.read(tagsAllOf("name:1")).events()).isEmpty();
    }

    @Test
    void retries_from_a_fresh_dcb_read_when_append_condition_detects_a_conflict() {
        InMemoryEventStore delegate = new InMemoryEventStore();
        delegate.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of("name:1"))));
        ConflictingOnceDcbEventStore eventStore = new ConflictingOnceDcbEventStore(delegate, converter().toCloudEvent(new DomainEvent("NameChangedByOther", "name:1")));
        AtomicInteger attempts = new AtomicInteger();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(event.name()),
                GenericDcbApplicationService.defaultRetryStrategy());

        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"), events -> {
            attempts.incrementAndGet();
            List<DomainEvent> currentEvents = events.toList();
            if (attempts.get() == 1) {
                assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined");
            } else {
                assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined", "NameChangedByOther");
            }
            return Stream.of(new DomainEvent("NameChangedByService", "name:1"));
        });

        assertThat(result).hasValue(new DcbAppendResult(3, 3, 1));
        assertThat(attempts).hasValue(2);
        assertThat(delegate.read(tagsAllOf("name:1")).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChangedByOther", "NameChangedByService");
    }

    private static CloudEventConverter<DomainEvent> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(DomainEvent domainEvent) {
                return CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("urn:test"))
                        .withType(domainEvent.type())
                        .withData(domainEvent.name().getBytes(UTF_8))
                        .build();
            }

            @Override
            public DomainEvent toDomainEvent(CloudEvent cloudEvent) {
                return new DomainEvent(cloudEvent.getType(), new String(cloudEvent.getData().toBytes(), UTF_8));
            }

            @Override
            public String getCloudEventType(Class<? extends DomainEvent> type) {
                return type.getName();
            }
        };
    }

    private record DomainEvent(String type, String name) {
    }

    private static class ConflictingOnceDcbEventStore implements DcbEventStore {
        private final InMemoryEventStore delegate;
        private final CloudEvent conflictingEvent;
        private final AtomicBoolean conflictInserted = new AtomicBoolean();

        private ConflictingOnceDcbEventStore(InMemoryEventStore delegate, CloudEvent conflictingEvent) {
            this.delegate = delegate;
            this.conflictingEvent = DcbCloudEvents.withTags(conflictingEvent, Set.of("name:1"));
        }

        @Override
        public DcbEventStream read(DcbQuery query, DcbReadOptions options) {
            return delegate.read(query, options);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            if (conflictInserted.compareAndSet(false, true)) {
                delegate.append(List.of(conflictingEvent));
            }
            return delegate.append(events, condition);
        }
    }
}
