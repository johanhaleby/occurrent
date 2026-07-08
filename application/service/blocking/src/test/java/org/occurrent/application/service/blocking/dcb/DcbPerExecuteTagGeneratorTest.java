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

package org.occurrent.application.service.blocking.dcb;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * Tests that {@link DcbExecuteOptions#tagGenerator(org.occurrent.application.service.dcb.TagGenerator)} overrides any
 * global {@link org.occurrent.application.service.dcb.TagGenerator} configured on
 * {@link GenericDcbApplicationService}, and that a service constructed without a global tagger still works as long
 * as a per-execute tagger is supplied, but fails loudly when neither is available.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbPerExecuteTagGeneratorTest {

    @Test
    void per_execute_tag_generator_overrides_the_global_one() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                event -> Set.of(Tag.parse("global:tag")),
                GenericDcbApplicationService.defaultRetryStrategy());

        DcbExecuteOptions<DomainEvent> options = DcbExecuteOptions.<DomainEvent>options()
                .tagGenerator(event -> Set.of(Tag.parse("per-execute:tag")));

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("per-execute:tag")), options, events ->
                List.of(new DomainEvent("NameDefined", "name:1")));

        assertThat(result).isPresent();

        DcbEventStream matchingPerExecuteTag = eventStore.read(tags(Tag.parse("per-execute:tag")));
        assertThat(matchingPerExecuteTag.events()).extracting(CloudEvent::getType).containsExactly("NameDefined");

        DcbEventStream matchingGlobalTag = eventStore.read(tags(Tag.parse("global:tag")));
        assertThat(matchingGlobalTag.events()).isEmpty();
    }

    @Test
    void service_without_a_global_tag_generator_can_append_when_a_per_execute_tagger_is_supplied() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                GenericDcbApplicationService.defaultRetryStrategy());

        DcbExecuteOptions<DomainEvent> options = DcbExecuteOptions.<DomainEvent>options()
                .tagGenerator(event -> Set.of(Tag.parse("name:1")));

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("name:1")), options, events ->
                List.of(new DomainEvent("NameDefined", "name:1")));

        assertThat(result).isPresent();
        assertThat(eventStore.read(tags(Tag.parse("name:1"))).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void service_without_a_global_tag_generator_and_no_per_execute_tagger_throws_when_events_must_be_tagged() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                converter(),
                GenericDcbApplicationService.defaultRetryStrategy());

        assertThatThrownBy(() -> applicationService.execute(tags(Tag.parse("name:1")), events ->
                List.of(new DomainEvent("NameDefined", "name:1"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No TagGenerator available");
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
}
