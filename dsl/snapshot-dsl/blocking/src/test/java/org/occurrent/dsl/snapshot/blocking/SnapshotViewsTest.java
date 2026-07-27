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
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Test
    void from_throws_NullPointerException_when_the_view_is_null() {
        assertThatThrownBy(() -> SnapshotViewSource.from(null, store))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("view");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        assertThatThrownBy(() -> SnapshotViewSource.from(snapshotView, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Nested
    @DisplayName("readState")
    class ReadState {

        @Test
        void returns_the_current_state_when_no_snapshot_exists() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, SnapshotViewSource.from(snapshotView, store));

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void resumes_from_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            store.save(streamId, new Snapshot<>("Jane", 1L, snapshotView.schemaVersion()));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, SnapshotViewSource.from(snapshotView, store));

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void rebuilds_from_the_whole_stream_when_the_snapshot_schema_version_does_not_match() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            store.save(streamId, new Snapshot<>("some-stale-shape", 1L, snapshotView.schemaVersion() + 1));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, SnapshotViewSource.from(snapshotView, store));

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void does_not_write_a_snapshot_when_none_existed() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            views.readState(streamId, SnapshotViewSource.from(snapshotView, store));

            assertThat(store.findLatest(streamId)).isEmpty();
        }

        @Test
        void does_not_overwrite_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            Snapshot<String> existing = new Snapshot<>("Jane", 1L, snapshotView.schemaVersion());
            store.save(streamId, existing);
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            views.readState(streamId, SnapshotViewSource.from(snapshotView, store));

            assertThat(store.findLatest(streamId)).contains(existing);
        }

        @Test
        void folds_each_event_with_its_own_stream_version_instead_of_empty_metadata() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet")));
            SnapshotView<Long, DomainEvent> streamVersionView = SnapshotView.<Long, DomainEvent>builder(0L)
                    .on(NameDefined.class, (state, metadata, event) -> metadata.getStreamVersion())
                    .on(NameWasChanged.class, (state, metadata, event) -> metadata.getStreamVersion())
                    .schemaVersion(1)
                    .build();
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            Long streamVersion = views.readState(streamId, SnapshotViewSource.from(streamVersionView, SnapshotStore.inMemory()));

            assertThat(streamVersion).isEqualTo(2L);
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
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            views.refresh(streamId, SnapshotViewSource.from(snapshotView, store));

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
            CountingSnapshotStore<String> countingStore = new CountingSnapshotStore<>(SnapshotStore.inMemory());
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);
            var source = SnapshotViewSource.from(snapshotView, countingStore);
            views.refresh(streamId, source);
            Snapshot<String> firstSnapshot = countingStore.findLatest(streamId).orElseThrow();

            views.refresh(streamId, source);

            assertAll(
                    () -> assertThat(countingStore.saveCount()).isEqualTo(2),
                    () -> assertThat(countingStore.findLatest(streamId)).contains(firstSnapshot)
            );
        }

        @Test
        void throws_RuntimeException_when_the_store_save_fails() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane")));
            SnapshotViews<DomainEvent> views = SnapshotViews.create(eventStore, converter);

            Throwable thrown = catchThrowable(() -> views.refresh(streamId, SnapshotViewSource.from(snapshotView, new ThrowingSnapshotStore<>())));

            assertThat(thrown).isInstanceOf(RuntimeException.class);
        }
    }

    private static class CountingSnapshotStore<S> implements SnapshotStore<S> {
        private final SnapshotStore<S> delegate;
        private int saveCount = 0;

        private CountingSnapshotStore(SnapshotStore<S> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Snapshot<S>> findLatest(String key) {
            return delegate.findLatest(key);
        }

        @Override
        public void save(String key, Snapshot<S> snapshot) {
            saveCount++;
            delegate.save(key, snapshot);
        }

        int saveCount() {
            return saveCount;
        }
    }
}
