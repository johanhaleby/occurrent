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

package org.occurrent.springboot.mongo.reactor;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.mongodb.spring.reactor.ReactiveSpringMongoSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Proves the reactive MongoDB {@code ReactiveSnapshotStore} round-trips a snapshot at marker versions without storing a
 * {@code version}, reads a snapshot stored by 0.34 as one without marker versions, and degrades to empty when the stored
 * marker versions cannot be read.
 */
@DisplayName("ReactiveSpringMongoSnapshotStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveSpringMongoSnapshotStoreTest {

    private static ConfigurableApplicationContext context;
    private static ReactiveMongoOperations mongoOperations;

    @BeforeAll
    static void startContext() {
        context = SpringApplication.run(new Class<?>[]{Application.class}, new String[]{"--spring.main.web-application-type=none"});
        mongoOperations = context.getBean(ReactiveMongoOperations.class);
    }

    @AfterAll
    static void stopContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void round_trips_a_position_snapshot() {
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, "reactive-snapshot-position");
        store.save("account-1", new Snapshot<>(new Balance(250, true), 12, 1)).block();

        Optional<Snapshot<Balance>> found = store.findLatest("account-1").blockOptional();

        assertThat(found).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.state()).isEqualTo(new Balance(250, true));
            assertThat(snapshot.version()).isEqualTo(12L);
            assertThat(snapshot.markerVersions()).isEmpty();
        });
    }

    @Test
    void round_trips_marker_versions_including_a_marker_key_with_a_dot() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of(
                "tag:account:a.b", new MarkerVersion("epoch-1", 3, 1),
                "type:Deposited", new MarkerVersion("epoch-2", 8, 0)));
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, "reactive-snapshot-marker-versions");
        store.save("account-4", Snapshot.atMarkerVersions(new Balance(40, false), versions, 41, 3)).block();

        Optional<Snapshot<Balance>> found = store.findLatest("account-4").blockOptional();

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
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, "reactive-snapshot-highest-folded-position");
        store.save("account-8", Snapshot.atMarkerVersions(new Balance(40, false), versions, 4_000_000_000L, 3)).block();

        Optional<Snapshot<Balance>> found = store.findLatest("account-8").blockOptional();

        assertThat(found).hasValueSatisfying(snapshot -> assertThat(snapshot.highestFoldedPosition()).isEqualTo(4_000_000_000L));
    }

    @Test
    void degrades_to_empty_when_the_stored_marker_versions_have_no_highest_folded_position() {
        String collection = "reactive-snapshot-no-highest-folded-position";
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:account:9", new MarkerVersion("epoch-1", 3, 0)));
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);
        store.save("account-9", Snapshot.atMarkerVersions(new Balance(40, false), versions, 41, 3)).block();
        Document stored = mongoOperations.findById("account-9", Document.class, collection).block();
        stored.remove("highestFoldedPosition");
        mongoOperations.save(stored, collection).block();

        assertThat(store.findLatest("account-9").blockOptional()).isEmpty();
    }

    @Test
    void stores_a_snapshot_at_marker_versions_without_a_version_field() {
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, "reactive-snapshot-no-version");
        store.save("account-5", Snapshot.atMarkerVersions(new Balance(1, false), DcbMarkerVersions.of(Map.of("tag:account:5", new MarkerVersion("epoch-1", 1, 0))), 1, 1)).block();

        Document stored = mongoOperations.findById("account-5", Document.class, "reactive-snapshot-no-version").block();

        assertThat(stored).isNotNull();
        assertThat(stored.keySet()).as("a node that reads only the version must find none").doesNotContain("version").contains("markerVersions", "highestFoldedPosition");
    }

    @Test
    void reads_a_snapshot_stored_with_a_version_and_no_marker_versions_as_a_snapshot_without_marker_versions() {
        String collection = "reactive-snapshot-0-34";
        mongoOperations.save(new Document("_id", "account-6").append("version", 9L).append("schemaVersion", 1).append("state", new Document("amount", 5).append("closed", false)), collection).block();
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        Optional<Snapshot<Balance>> found = store.findLatest("account-6").blockOptional();

        assertThat(found).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.version()).isEqualTo(9L);
            assertThat(snapshot.markerVersions()).isEmpty();
        });
    }

    @Test
    void degrades_to_empty_when_the_stored_marker_versions_cannot_be_read() {
        String collection = "reactive-snapshot-bad-marker-versions";
        mongoOperations.save(new Document("_id", "account-7").append("markerVersions", "not-a-list").append("schemaVersion", 1).append("state", new Document("amount", 5).append("closed", false)), collection).block();
        ReactiveSpringMongoSnapshotStore<Balance> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        assertThatCode(() -> assertThat(store.findLatest("account-7").blockOptional()).isEmpty()).doesNotThrowAnyException();
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
