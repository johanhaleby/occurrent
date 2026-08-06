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

package org.occurrent.dsl.snapshot.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotViews")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotViewsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private ApplicationService<DomainEvent> applicationService;
    private ReactiveSnapshotStore<String> store;
    private SnapshotView<String, DomainEvent> snapshotView;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotviews");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        store = ReactiveSnapshotStore.inMemory();
        time = LocalDateTime.now();
        snapshotView = SnapshotView.<String, DomainEvent>builder("")
                .on(NameDefined.class, (state, event) -> event.name())
                .on(NameWasChanged.class, (state, event) -> event.name())
                .schemaVersion(1)
                .build();
    }

    @Test
    void from_throws_NullPointerException_when_the_view_is_null() {
        assertThatThrownBy(() -> ReactiveSnapshotViewSource.from(null, store))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("view");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        assertThatThrownBy(() -> ReactiveSnapshotViewSource.from(snapshotView, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Test
    void fails_fast_with_guidance_when_the_view_folds_to_a_null_state() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
        SnapshotView<String, DomainEvent> foldsToNull = SnapshotView.<String, DomainEvent>builder("")
                .on(NameDefined.class, (state, event) -> null)
                .schemaVersion(1)
                .build();
        ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

        assertThatThrownBy(() -> views.readState(streamId, ReactiveSnapshotViewSource.from(foldsToNull, store)).block())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");
    }

    @Nested
    @DisplayName("readState")
    class ReadState {

        @Test
        void returns_the_current_state_when_no_snapshot_exists() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void resumes_from_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            store.save(streamId, new Snapshot<>("Jane", 1L, snapshotView.schemaVersion())).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void rebuilds_from_the_whole_stream_when_the_snapshot_schema_version_does_not_match() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            store.save(streamId, new Snapshot<>("some-stale-shape", 1L, snapshotView.schemaVersion() + 1)).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            String state = views.readState(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertThat(state).isEqualTo("Janet");
        }

        @Test
        void does_not_write_a_snapshot_when_none_existed() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            views.readState(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertThat(store.findLatest(streamId).blockOptional()).isEmpty();
        }

        @Test
        void does_not_overwrite_an_existing_snapshot() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            Snapshot<String> existing = new Snapshot<>("Jane", 1L, snapshotView.schemaVersion());
            store.save(streamId, existing).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            views.readState(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertThat(store.findLatest(streamId).blockOptional()).contains(existing);
        }

        @Test
        void folds_each_event_with_its_own_stream_version_instead_of_empty_metadata() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            SnapshotView<Long, DomainEvent> streamVersionView = SnapshotView.<Long, DomainEvent>builder(0L)
                    .on(NameDefined.class, (state, metadata, event) -> metadata.getStreamVersion())
                    .on(NameWasChanged.class, (state, metadata, event) -> metadata.getStreamVersion())
                    .schemaVersion(1)
                    .build();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            Long streamVersion = views.readState(streamId, ReactiveSnapshotViewSource.from(streamVersionView, ReactiveSnapshotStore.inMemory())).block();

            assertThat(streamVersion).isEqualTo(2L);
        }
    }

    @Nested
    @DisplayName("refresh")
    class Refresh {

        @Test
        void writes_a_snapshot_of_the_current_head_unconditionally() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            views.refresh(streamId, ReactiveSnapshotViewSource.from(snapshotView, store)).block();

            assertAll(
                    () -> assertThat(store.findLatest(streamId).blockOptional()).isPresent(),
                    () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().state()).isEqualTo("Janet"),
                    () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).isEqualTo(2L),
                    () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().schemaVersion()).isEqualTo(snapshotView.schemaVersion())
            );
        }

        @Test
        void overwrites_an_existing_snapshot_even_when_it_is_already_up_to_date() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            CountingReactiveSnapshotStore<String> countingStore = new CountingReactiveSnapshotStore<>(ReactiveSnapshotStore.inMemory());
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);
            var source = ReactiveSnapshotViewSource.from(snapshotView, countingStore);
            views.refresh(streamId, source).block();
            Snapshot<String> firstSnapshot = countingStore.findLatest(streamId).blockOptional().orElseThrow();

            views.refresh(streamId, source).block();

            assertAll(
                    () -> assertThat(countingStore.saveCount()).isEqualTo(2),
                    () -> assertThat(countingStore.findLatest(streamId).blockOptional()).contains(firstSnapshot)
            );
        }

        @Test
        void throws_RuntimeException_when_the_store_save_fails() {
            String streamId = UUID.randomUUID().toString();
            applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
            ReactiveSnapshotViews<DomainEvent> views = ReactiveSnapshotViews.create(eventStore, converter);

            Throwable thrown = catchThrowable(() -> views.refresh(streamId, ReactiveSnapshotViewSource.from(snapshotView, new ThrowingReactiveSnapshotStore<>())).block());

            assertThat(thrown).isInstanceOf(RuntimeException.class);
        }
    }

    private static class CountingReactiveSnapshotStore<S> implements ReactiveSnapshotStore<S> {
        private final ReactiveSnapshotStore<S> delegate;
        private final AtomicInteger saveCount = new AtomicInteger();

        private CountingReactiveSnapshotStore(ReactiveSnapshotStore<S> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<Snapshot<S>> findLatest(String key) {
            return delegate.findLatest(key);
        }

        @Override
        public Mono<Void> save(String key, Snapshot<S> snapshot) {
            return Mono.defer(() -> {
                saveCount.incrementAndGet();
                return delegate.save(key, snapshot);
            });
        }

        int saveCount() {
            return saveCount.get();
        }
    }
}
