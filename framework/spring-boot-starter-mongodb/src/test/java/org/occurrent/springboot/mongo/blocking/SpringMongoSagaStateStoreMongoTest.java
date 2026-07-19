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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.Status;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the MongoDB-backed {@link SpringMongoSagaStateStore}: state round-trips, compare-and-set enforces the version,
 * and the due-timer query returns only active instances with a due timer. Docker-based.
 */
@DisplayName("SpringMongoSagaStateStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SpringMongoSagaStateStoreMongoTest.StoreApplication.class,
        properties = "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-store-test"
)
@Import(SpringMongoSagaStateStoreMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class SpringMongoSagaStateStoreMongoTest {

    @Autowired
    private MongoOperations mongoOperations;

    private SpringMongoSagaStateStore<Counter> store;

    @BeforeEach
    void setUp() {
        String collection = "saga-store-test-" + System.nanoTime();
        store = new SpringMongoSagaStateStore<>(mongoOperations, collection, Counter.class);
    }

    @Test
    void inserts_a_new_instance_at_version_1_and_rejects_a_second_insert() {
        SagaEnvelope<Counter> envelope = active("s1", new Counter(1), 1, List.of(new TimerEntry("t", 5_000)), 0);

        assertThat(store.compareAndSave("s1", envelope, 0)).isTrue();
        assertThat(store.compareAndSave("s1", active("s1", new Counter(9), 1, List.of(), 0), 0)).isFalse();
    }

    @Test
    void round_trips_state_timers_and_watermarks() {
        SagaEnvelope<Counter> envelope = new SagaEnvelope<>("s2", new Counter(42), Status.ACTIVE, 1,
                List.of(new TimerEntry("payment", 7_000)), Map.of("stream-a", 5L), 11L,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), null);
        store.compareAndSave("s2", envelope, 0);

        Optional<SagaEnvelope<Counter>> found = store.find("s2");

        assertThat(found).hasValueSatisfying(e -> {
            assertThat(e.sagaId()).isEqualTo("s2");
            assertThat(e.state()).isEqualTo(new Counter(42));
            assertThat(e.status()).isEqualTo(Status.ACTIVE);
            assertThat(e.version()).isEqualTo(1);
            assertThat(e.timers()).containsExactly(new TimerEntry("payment", 7_000));
            assertThat(e.streamWatermarks()).containsEntry("stream-a", 5L);
            assertThat(e.positionWatermark()).isEqualTo(11L);
        });
    }

    @Test
    void updates_only_when_the_expected_version_matches() {
        store.compareAndSave("s3", active("s3", new Counter(1), 1, List.of(), 0), 0);

        assertThat(store.compareAndSave("s3", active("s3", new Counter(2), 2, List.of(), 0), 1)).isTrue();
        // Version is now 2, so an update expecting version 1 loses.
        assertThat(store.compareAndSave("s3", active("s3", new Counter(3), 2, List.of(), 0), 1)).isFalse();
        assertThat(store.find("s3")).hasValueSatisfying(e -> assertThat(e.state()).isEqualTo(new Counter(2)));
    }

    @Test
    void finds_active_instances_with_a_due_timer_and_excludes_completed_and_not_yet_due() {
        store.compareAndSave("due", active("due", new Counter(1), 1, List.of(new TimerEntry("t", 1_000)), 0), 0);
        store.compareAndSave("later", active("later", new Counter(1), 1, List.of(new TimerEntry("t", 10_000)), 0), 0);
        store.compareAndSave("done", completed("done", new Counter(1), 1), 0);

        List<SagaEnvelope<Counter>> due = store.findWithDueTimers(Instant.ofEpochMilli(2_000), 10);

        assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("due");
    }

    @Test
    void deletes_an_instance() {
        store.compareAndSave("gone", active("gone", new Counter(1), 1, List.of(), 0), 0);

        store.delete("gone");

        assertThat(store.find("gone")).isEmpty();
    }

    private static SagaEnvelope<Counter> active(String id, Counter state, long version, List<TimerEntry> timers, long positionWatermark) {
        return new SagaEnvelope<>(id, state, Status.ACTIVE, version, timers, Map.of(),
                positionWatermark == 0 ? null : positionWatermark, Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), null);
    }

    private static SagaEnvelope<Counter> completed(String id, Counter state, long version) {
        return new SagaEnvelope<>(id, state, Status.COMPLETED, version, List.of(), Map.of(), null,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), Instant.ofEpochMilli(1));
    }

    record Counter(int value) {
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class StoreApplication {
    }
}
