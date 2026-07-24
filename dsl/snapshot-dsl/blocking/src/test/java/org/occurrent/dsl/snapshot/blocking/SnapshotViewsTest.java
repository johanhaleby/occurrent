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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
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

    @Nested
    @DisplayName("readState")
    class ReadState {

        @Test
        void returns_the_current_state_when_no_snapshot_exists() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            String state = views.readState(streamId, snapshotView);

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void resumes_from_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            store.save(streamId, new Snapshot<>("Jane", 1L, snapshotView.schemaVersion()));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            String state = views.readState(streamId, snapshotView);

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void rebuilds_from_the_whole_stream_when_the_snapshot_schema_version_does_not_match() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            store.save(streamId, new Snapshot<>("some-stale-shape", 1L, snapshotView.schemaVersion() + 1));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            String state = views.readState(streamId, snapshotView);

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void does_not_write_a_snapshot_when_none_existed() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            views.readState(streamId, snapshotView);

            assertThat(store.findLatest(streamId)).isEmpty();
        }

        @Test
        void does_not_overwrite_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            Snapshot<String> existing = new Snapshot<>("Jane", 1L, snapshotView.schemaVersion());
            store.save(streamId, existing);
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            views.readState(streamId, snapshotView);

            assertThat(store.findLatest(streamId)).contains(existing);
        }
    }

    @Nested
    @DisplayName("refresh")
    class Refresh {

        @Test
        void writes_a_snapshot_of_the_current_head_unconditionally() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);

            views.refresh(streamId, snapshotView);

            assertAll(
                    () -> assertThat(store.findLatest(streamId)).isPresent(),
                    () -> assertThat(store.findLatest(streamId).orElseThrow().state()).isEqualTo("Janet"),
                    () -> assertThat(store.findLatest(streamId).orElseThrow().version()).isEqualTo(2L),
                    () -> assertThat(store.findLatest(streamId).orElseThrow().schemaVersion()).isEqualTo(snapshotView.schemaVersion())
            );
        }

        @Test
        void overwrites_an_existing_snapshot_even_when_it_is_already_up_to_date() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, store);
            views.refresh(streamId, snapshotView);
            Snapshot<String> firstSnapshot = store.findLatest(streamId).orElseThrow();

            views.refresh(streamId, snapshotView);

            assertThat(store.findLatest(streamId)).contains(firstSnapshot);
        }

        @Test
        void throws_RuntimeException_when_the_store_save_fails() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            SnapshotViews<String, DomainEvent> views = SnapshotViews.create(eventStore, converter, new ThrowingSnapshotStore<>());

            Throwable thrown = catchThrowable(() -> views.refresh(streamId, snapshotView));

            assertThat(thrown).isInstanceOf(RuntimeException.class);
        }
    }

    private static class ThrowingSnapshotStore<S> implements SnapshotStore<S> {
        @Override
        public Optional<Snapshot<S>> findLatest(String key) {
            return Optional.empty();
        }

        @Override
        public void save(String key, Snapshot<S> snapshot) {
            throw new RuntimeException("save failed");
        }
    }
}
