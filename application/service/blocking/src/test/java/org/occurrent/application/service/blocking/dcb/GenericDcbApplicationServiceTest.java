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
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceTest {

    @Test
    void reads_by_dcb_query_and_appends_with_tags_from_domain_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of(Tag.parse("name:1")))));
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(Tag.parse(event.name())),
                GenericDcbApplicationService.defaultRetryStrategy());

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("name:1")), events -> {
            assertThat(events).extracting(DomainEvent::type).containsExactly("NameDefined");
            return List.of(new DomainEvent("NameChanged", "name:1"));
        });

        assertThat(result).hasValue(new DcbAppendResult(2, 2, 1));
        DcbEventStream eventStream = eventStore.read(tags(Tag.parse("name:1")));
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
                event -> Set.of(Tag.parse(event.name())));

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("name:1")), events -> List.of());

        assertThat(result).isEmpty();
        assertThat(eventStore.read(tags(Tag.parse("name:1"))).events()).isEmpty();
    }

    @Test
    void retries_from_a_fresh_dcb_read_when_append_condition_detects_a_conflict() {
        InMemoryEventStore delegate = new InMemoryEventStore();
        delegate.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of(Tag.parse("name:1")))));
        ConflictingOnceDcbEventStore eventStore = new ConflictingOnceDcbEventStore(delegate, converter().toCloudEvent(new DomainEvent("NameChangedByOther", "name:1")));
        AtomicInteger attempts = new AtomicInteger();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(Tag.parse(event.name())),
                GenericDcbApplicationService.defaultRetryStrategy());

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("name:1")), events -> {
            attempts.incrementAndGet();
            if (attempts.get() == 1) {
                assertThat(events).extracting(DomainEvent::type).containsExactly("NameDefined");
            } else {
                assertThat(events).extracting(DomainEvent::type).containsExactly("NameDefined", "NameChangedByOther");
            }
            return List.of(new DomainEvent("NameChangedByService", "name:1"));
        });

        assertThat(result).hasValue(new DcbAppendResult(3, 3, 1));
        assertThat(attempts).hasValue(2);
        assertThat(delegate.read(tags(Tag.parse("name:1"))).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChangedByOther", "NameChangedByService");
    }

    @Test
    void default_retry_strategy_makes_five_attempts_in_total_before_rethrowing() {
        InMemoryEventStore delegate = new InMemoryEventStore();
        delegate.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of(Tag.parse("name:1")))));
        AlwaysConflictingDcbEventStore eventStore = new AlwaysConflictingDcbEventStore(delegate, converter());
        AtomicInteger attempts = new AtomicInteger();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(Tag.parse(event.name())),
                GenericDcbApplicationService.defaultRetryStrategy());

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> applicationService.execute(tags(Tag.parse("name:1")), events -> {
                    attempts.incrementAndGet();
                    return List.of(new DomainEvent("NameChangedByService", "name:1"));
                }))
                .isInstanceOf(org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException.class);

        // The default policy allows five attempts in total (the initial attempt plus four retries) before giving up,
        // matching the reactive counterpart's defaultRetry.
        assertThat(attempts).hasValue(5);
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
            this.conflictingEvent = DcbCloudEvents.withTags(conflictingEvent, Set.of(Tag.parse("name:1")));
        }

        @Override
        public DcbEventStream read(DcbCriteria query, DcbReadOptions options) {
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

    private static class AlwaysConflictingDcbEventStore implements DcbEventStore {
        private final DcbEventStore delegate;
        private final CloudEventConverter<DomainEvent> converter;

        private AlwaysConflictingDcbEventStore(DcbEventStore delegate, CloudEventConverter<DomainEvent> converter) {
            this.delegate = delegate;
            this.converter = converter;
        }

        @Override
        public DcbEventStream read(DcbCriteria query, DcbReadOptions options) {
            return delegate.read(query, options);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            CloudEvent interloper = DcbCloudEvents.withTags(converter.toCloudEvent(new DomainEvent("NameChangedByOther", "name:1")), Set.of(Tag.parse("name:1")));
            delegate.append(List.of(interloper));
            return delegate.append(events, condition);
        }
    }
}
