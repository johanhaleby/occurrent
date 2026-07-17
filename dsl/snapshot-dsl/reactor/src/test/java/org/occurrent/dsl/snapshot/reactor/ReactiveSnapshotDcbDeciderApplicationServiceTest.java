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
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotDcbDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotDcbDeciderApplicationServiceTest {

    private static final String ACCOUNT = "acct";

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdcb"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> snapshotService;
    private ReactiveSnapshotStore<String> store;
    private AtomicInteger evolveCount;
    private DcbDecider<Cmd, String, DomainEvent> dcbDecider;
    private String key;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdcb");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        DcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        snapshotService = new ReactiveSnapshotDcbDeciderApplicationService<>(applicationService);
        store = ReactiveSnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        Decider<Cmd, String, DomainEvent> decider = countingDecider(evolveCount, time = LocalDateTime.now());
        dcbDecider = DcbDecider.from(decider, command -> criteria(), event -> Set.of(tag()));
        key = DcbSnapshotKeys.canonicalKey(criteria());
    }

    @Test
    void first_execute_appends_and_saves_a_snapshot_keyed_by_criteria() {
        snapshotService.execute(new Define("A"), dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())).block();

        assertAll(
                () -> assertThat(store.findLatest(key).blockOptional()).isPresent(),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        snapshotService.execute(new Define("A"), dcbDecider, store, options).block();

        evolveCount.set(0);
        snapshotService.execute(new Change("B"), dcbDecider, store, options).block();

        assertAll(
                // Empty tail after the snapshot, so only the produced event is folded (1), not a full replay.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void the_resume_read_folds_events_appended_after_the_snapshot_by_another_writer() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        snapshotService.execute(new Define("A"), dcbDecider, store, options).block();

        // An event lands in the same boundary out-of-band, so the snapshot is now behind the boundary head.
        appendOutOfBand(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "X"));

        evolveCount.set(0);
        snapshotService.execute(new Change("B"), dcbDecider, store, options).block();

        assertAll(
                // The out-of-band X and the produced B are both folded (2), proving the tail after the snapshot was read.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot() {
        snapshotService.execute(new Define("A"), dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())).block();

        evolveCount.set(0);
        snapshotService.execute(new Change("B"), dcbDecider, store, SnapshotOptions.of(2, SnapshotPolicy.always())).block();

        assertAll(
                // Old schema-1 snapshot ignored, whole boundary replayed: A (1) plus produced B (1) = 2.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().schemaVersion()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    private void appendOutOfBand(DomainEvent event) {
        List<Tag> tags = List.of(tag());
        eventStore.append(converter.toCloudEvents(List.of(event)).stream().map(ce -> DcbCloudEvents.withTags(ce, tags)).toList()).block();
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private sealed interface Cmd {
    }

    private record Define(String name) implements Cmd {
    }

    private record Change(String name) implements Cmd {
    }

    private static Decider<Cmd, String, DomainEvent> countingDecider(AtomicInteger evolveCount, LocalDateTime time) {
        return new Decider<>() {
            @Override
            public String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Cmd command, String state) {
                return switch (command) {
                    case Define d -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", d.name()));
                    case Change c -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", c.name()));
                };
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                evolveCount.incrementAndGet();
                return event.name();
            }
        };
    }
}
