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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.spring.SpringTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A snapshot decider with a synchronous subscription saves snapshots through a mongos, where the store reads the marker
 * versions in a snapshot transaction of its own (ADR 167).
 */
@Testcontainers
@DisplayName("SnapshotDcbDeciderApplicationService over SpringMongoEventStore on a sharded cluster")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotDcbDeciderOnShardedClusterTest {

    private static final String ACCOUNT = "acct";

    @Container
    private static final MongoDBContainer mongos =
            new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                    .withSharding()
                    .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");

    private MongoClient mongoClient;
    private MongoTransactionManager transactionManager;
    private CloudEventConverter<DomainEvent> converter;
    private SpringMongoEventStore eventStore;
    private SnapshotStore<List<String>> snapshots;
    private SynchronousSubscriptionModel subscriptions;
    private SnapshotDcbDecider<Add, List<String>, DomainEvent> decider;

    @BeforeEach
    void wire_a_snapshot_decider_with_a_synchronous_subscription_over_a_mongos() {
        String database = "sharded_" + UUID.randomUUID().toString().replace("-", "");
        mongoClient = MongoClients.create(new ConnectionString(mongos.getConnectionString()));
        assertThat(mongoClient.getDatabase(database).runCommand(new Document("hello", 1)).get("msg"))
                .as("The container must be a mongos, or nothing below tests the sharded case").isEqualTo("isdbgrid");
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, database);
        transactionManager = new MongoTransactionManager(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .transactionConfig(transactionManager)
                .build();
        eventStore = new SpringMongoEventStore(new MongoTemplate(databaseFactory), config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("names", event -> {
        });
        snapshots = SnapshotStore.inMemory();
        decider = SnapshotDcbDecider.from(DcbDecider.from(names(), command -> criteria(), event -> Set.of(tag())), snapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    @Test
    void a_transaction_the_executor_opens_saves_a_snapshot_at_marker_versions_and_the_next_execute_resumes_from_it() {
        SnapshotDcbDeciderApplicationService<DomainEvent> service = new SnapshotDcbDeciderApplicationService<>(applicationServiceWith(new SpringTransactionExecutor(transactionManager)));
        appendSeed();
        service.execute(new Add("first"), decider);

        assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria()))).as("the snapshot saved by the first execute").isPresent();
        Snapshot<List<String>> saved = snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).orElseThrow();
        snapshots.save(DcbSnapshotKeys.canonicalKey(criteria()), Snapshot.atMarkerVersions(List.of("snapshot-only"), saved.markerVersions().orElseThrow(), saved.highestFoldedPosition(), 1));

        List<String> state = service.executeAndReturnState(new Add("second"), decider);

        assertAll(
                () -> assertThat(state).as("the state, resumed from the snapshot so only the tail was read").containsExactly("snapshot-only", "second"),
                () -> assertThat(eventStore.read(criteria()).events()).as("events stored").hasSize(3),
                () -> assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).orElseThrow().state()).as("the snapshot saved by the second execute").containsExactly("snapshot-only", "second")
        );
    }

    private GenericDcbApplicationService<DomainEvent> applicationServiceWith(TransactionExecutor transactionExecutor) {
        return GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(subscriptions)
                .transactionExecutor(transactionExecutor)
                .build();
    }

    private void appendSeed() {
        eventStore.append(new ArrayList<>(converter.toCloudEvents(List.of(nameWasChanged("seed"))).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList()));
    }

    private static NameWasChanged nameWasChanged(String name) {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private record Add(String name) {
    }

    private static Decider<Add, List<String>, DomainEvent> names() {
        return new Decider<>() {
            @Override
            public List<String> initialState() {
                return List.of();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Add command, List<String> state) {
                return List.of(nameWasChanged(command.name()));
            }

            @Override
            public List<String> evolve(List<String> state, @NonNull DomainEvent event) {
                List<String> next = new ArrayList<>(state);
                next.add(event.name());
                return next;
            }
        };
    }
}
