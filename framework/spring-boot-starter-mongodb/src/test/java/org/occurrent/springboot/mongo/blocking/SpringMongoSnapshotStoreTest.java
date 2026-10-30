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

package org.occurrent.springboot.mongo.blocking;

import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Proves the MongoDB {@code SnapshotStore} round-trips both scalar and POJO state, and that an unreadable stored snapshot
 * (for example after a state-shape change) degrades to an empty result instead of throwing, so a snapshot never fails a
 * command that loads it.
 */
@DisplayName("SpringMongoSnapshotStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class SpringMongoSnapshotStoreTest {

    private static ConfigurableApplicationContext context;
    private static MongoOperations mongoOperations;

    @BeforeAll
    static void startContext() {
        context = SpringApplication.run(new Class<?>[]{Application.class}, new String[]{"--spring.main.web-application-type=none"});
        mongoOperations = context.getBean(MongoOperations.class);
    }

    @AfterAll
    static void stopContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void round_trips_scalar_state() {
        SpringMongoSnapshotStore<Long> store = new SpringMongoSnapshotStore<>(mongoOperations, Long.class, "snapshot-scalar");
        store.save("account-1", new Snapshot<>(4711L, 7, 2));

        Optional<Snapshot<Long>> found = store.findLatest("account-1");

        assertThat(found).isPresent();
        assertThat(found.get().state()).isEqualTo(4711L);
        assertThat(found.get().version()).isEqualTo(7L);
        assertThat(found.get().schemaVersion()).isEqualTo(2);
    }

    @Test
    void round_trips_pojo_state() {
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, "snapshot-pojo");
        store.save("account-2", new Snapshot<>(new Balance(250, true), 12, 1));

        Optional<Snapshot<Balance>> found = store.findLatest("account-2");

        assertThat(found).isPresent();
        assertThat(found.get().state()).isEqualTo(new Balance(250, true));
        assertThat(found.get().version()).isEqualTo(12L);
    }

    @Test
    void round_trips_marker_versions_including_a_marker_key_with_a_dot() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of(
                "tag:account:a.b", new MarkerVersion("epoch-1", 3, 1),
                "type:Deposited", new MarkerVersion("epoch-2", 8, 0)));
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, "snapshot-marker-versions");
        store.save("account-4", Snapshot.atMarkerVersions(new Balance(40, false), versions, 41, 3));

        Optional<Snapshot<Balance>> found = store.findLatest("account-4");

        assertThat(found).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.state()).isEqualTo(new Balance(40, false));
            assertThat(snapshot.markerVersions()).contains(versions);
            assertThat(snapshot.version()).isZero();
            assertThat(snapshot.schemaVersion()).isEqualTo(3);
        });
    }

    @Test
    void round_trips_the_highest_folded_position() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:account:6", new MarkerVersion("epoch-1", 3, 0)));
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, "snapshot-highest-folded-position");
        store.save("account-8", Snapshot.atMarkerVersions(new Balance(40, false), versions, 4_000_000_000L, 3));

        Optional<Snapshot<Balance>> found = store.findLatest("account-8");

        assertThat(found).hasValueSatisfying(snapshot -> assertThat(snapshot.highestFoldedPosition()).isEqualTo(4_000_000_000L));
    }

    @Test
    void degrades_to_empty_when_the_stored_marker_versions_have_no_highest_folded_position() {
        String collection = "snapshot-no-highest-folded-position";
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:account:9", new MarkerVersion("epoch-1", 3, 0)));
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);
        store.save("account-9", Snapshot.atMarkerVersions(new Balance(40, false), versions, 41, 3));
        Document stored = mongoOperations.findById("account-9", Document.class, collection);
        stored.remove("highestFoldedPosition");
        mongoOperations.save(stored, collection);

        assertThat(store.findLatest("account-9")).isEmpty();
    }

    @Test
    void stores_a_snapshot_at_marker_versions_without_a_version_field() {
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, "snapshot-no-version");
        store.save("account-5", Snapshot.atMarkerVersions(new Balance(1, false), DcbMarkerVersions.of(Map.of("tag:account:5", new MarkerVersion("epoch-1", 1, 0))), 1, 1));

        Document stored = mongoOperations.findById("account-5", Document.class, "snapshot-no-version");

        assertThat(stored).isNotNull();
        assertThat(stored.keySet()).as("a node that reads only the version must find none").doesNotContain("version").contains("markerVersions", "highestFoldedPosition");
    }

    @Test
    void reads_a_snapshot_stored_with_a_version_and_no_marker_versions_as_a_snapshot_without_marker_versions() {
        String collection = "snapshot-0-34";
        mongoOperations.save(new Document("_id", "account-6").append("version", 9L).append("schemaVersion", 1).append("state", new Document("amount", 5).append("closed", false)), collection);
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        Optional<Snapshot<Balance>> found = store.findLatest("account-6");

        assertThat(found).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.version()).isEqualTo(9L);
            assertThat(snapshot.markerVersions()).isEmpty();
        });
    }

    @Test
    void degrades_to_empty_when_the_stored_marker_versions_cannot_be_read() {
        String collection = "snapshot-bad-marker-versions";
        mongoOperations.save(new Document("_id", "account-7").append("markerVersions", "not-a-list").append("schemaVersion", 1).append("state", new Document("amount", 5).append("closed", false)), collection);
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        assertThatCode(() -> assertThat(store.findLatest("account-7")).isEmpty()).doesNotThrowAnyException();
    }

    @Test
    void degrades_to_empty_when_the_stored_state_cannot_be_read() {
        String collection = "snapshot-unreadable";
        // A state value that cannot be converted into the declared state type, standing in for a snapshot written under an
        // incompatible state shape. findLatest must return empty (falling back to a full replay), not throw.
        mongoOperations.save(new Document("_id", "account-3").append("version", 3L).append("schemaVersion", 1).append("state", "not-a-balance"), collection);
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        assertThatCode(() -> assertThat(store.findLatest("account-3")).isEmpty()).doesNotThrowAnyException();
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    record Balance(int amount, boolean closed) {
    }
}
