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

package org.occurrent.dsl.snapshot.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SnapshotViews")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnapshotViewsTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private ApplicationService<DomainEvent> applicationService;
    private SnapshotStore<String> store;
    private SnapshotView<String, DomainEvent> snapshotView;
    private LocalDateTime time;

    @BeforeEach
    void setup() {
        time = LocalDateTime.now();
        eventStore = new InMemoryEventStore();
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        store = SnapshotStore.inMemory();
        snapshotView = SnapshotView.<String, DomainEvent>builder("")
                .on(NameDefined.class, (state, event) -> event.name())
                .on(NameWasChanged.class, (state, event) -> event.name())
                .schemaVersion(1)
                .build();
    }

    @Test
    void reads_the_current_state_and_writes_a_snapshot() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
        applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));

        String state = SnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always());

        assertAll(
                () -> assertThat(state).isEqualTo("Janet"),
                () -> assertThat(store.findLatest(streamId)).isPresent(),
                () -> assertThat(store.findLatest(streamId).orElseThrow().state()).isEqualTo("Janet"),
                () -> assertThat(store.findLatest(streamId).orElseThrow().version()).isEqualTo(2L)
        );
    }

    @Test
    void a_second_read_resumes_from_the_snapshot() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
        SnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always());

        applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
        String state = SnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always());

        assertThat(state).isEqualTo("Janet");
    }
}
