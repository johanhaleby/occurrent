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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.Name;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceSynchronousSubscriptionsTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;

    @BeforeEach
    void initialize() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        eventStore = new InMemoryEventStore();
    }

    @Test
    void dispatches_the_appended_block_synchronously_enriched_with_positions() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(Tag.parse("name:1")))
                .synchronousSubscriptions(dispatcher)
                .build();

        Optional<DcbAppendResult> result = applicationService.execute(tags(Tag.parse("name:1")),
                events -> Name.defineName(events, UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"));

        assertThat(result).isPresent();
        // The dispatched events are exactly the just-appended global-position block, read back from the store enriched.
        List<CloudEvent> appendedBlock = eventStore.read(DcbCriteria.all(),
                DcbReadOptions.between(result.get().firstSequencePosition() - 1, result.get().lastSequencePosition())).events();
        assertThat(dispatcher.dispatched).extracting(CloudEvent::getId).isEqualTo(appendedBlock.stream().map(CloudEvent::getId).toList());
        assertThat(dispatcher.dispatched).extracting(CloudEvent::getType).containsExactly("org.occurrent.domain.NameDefined");
    }

    @Test
    void does_not_dispatch_when_the_command_produces_no_events() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(Tag.parse("name:1")))
                .synchronousSubscriptions(dispatcher)
                .build();

        applicationService.execute(tags(Tag.parse("name:1")), events -> List.of());

        assertThat(dispatcher.dispatched).isEmpty();
    }

    private static final class RecordingDispatcher implements SynchronousEventDispatcher {
        private final List<CloudEvent> dispatched = new ArrayList<>();

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            dispatched.addAll(writtenCloudEvents);
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
