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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaInstance;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. Checks that a quarantined instance survives a round trip through real MongoDB, that it is enumerable
 * without decoding its state, which is the whole point of storing the record as top-level fields, and that a document
 * written before 0.34.0 still reads back as an instance that has started.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreQuarantineTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-quarantine";

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-quarantine-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    private static SagaFailure failure() {
        return new SagaFailure("order-1@7", 7, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", null);
    }

    private static SagaEnvelope<String> quarantined(String sagaId) {
        return new SagaEnvelope<>(sagaId, "awaiting-payment", SagaStatus.QUARANTINED, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of("order-1", 6L), 6L, NOW.minusSeconds(600),
                NOW, null, null, true, failure());
    }

    @Test
    void a_quarantined_instance_round_trips_with_its_failure_record_intact() {
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        store.compareAndSave("order-1", quarantined("order-1"), 0);

        SagaEnvelope<String> read = store.find("order-1").orElseThrow();

        assertAll(
                () -> assertThat(read.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(read.started()).isTrue(),
                () -> assertThat(read.failure()).isEqualTo(failure()),
                () -> assertThat(read.streamWatermarks()).isEqualTo(Map.of("order-1", 6L)),
                () -> assertThat(read.positionWatermark()).isEqualTo(6L)
        );
    }

    @Test
    void a_released_instance_keeps_the_instant_it_was_released_at() {
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        SagaFailure released = failure().released(NOW);
        store.compareAndSave("order-2", new SagaEnvelope<>("order-2", "awaiting-payment", SagaStatus.QUARANTINED, 1,
                List.of(), Map.of(), null, NOW, NOW, null, null, true, released), 0);

        SagaFailure read = store.find("order-2").orElseThrow().failure();

        assertAll(
                () -> assertThat(read.isReleased()).isTrue(),
                () -> assertThat(read.releasedAt()).isEqualTo(NOW)
        );
    }

    @Test
    void an_instance_that_failed_before_it_started_round_trips_as_one_that_has_not_started() {
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        store.compareAndSave("order-3", new SagaEnvelope<>("order-3", null, SagaStatus.QUARANTINED, 1, List.of(),
                Map.of(), null, NOW, NOW, null, null, false, failure()), 0);

        assertThat(store.find("order-3").orElseThrow().started()).isFalse();
    }

    @Test
    void a_document_written_before_the_started_marker_existed_reads_back_as_started() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
        // Exactly the shape 0.33.0 wrote: no started field and no failure fields at all.
        mongoOperations.insert(new Document("_id", "order-4").append("status", "ACTIVE").append("version", 1L)
                .append("state", "awaiting-payment").append("timers", List.of())
                .append("createdAt", NOW.toEpochMilli()).append("updatedAt", NOW.toEpochMilli()), COLLECTION);

        SagaEnvelope<String> read = store.find("order-4").orElseThrow();

        assertAll(
                () -> assertThat(read.started()).isTrue(),
                () -> assertThat(read.failure()).isNull()
        );
    }

    @Test
    void a_quarantined_instance_is_enumerable_without_its_state_being_decoded() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
        store.compareAndSave("order-5", quarantined("order-5"), 0);
        // The state an operator is most likely looking at is the one that no longer decodes, so make it undecodable.
        mongoOperations.updateFirst(Query.query(where("_id").is("order-5")),
                new org.springframework.data.mongodb.core.query.Update().set("state", new Document("gone", true)), COLLECTION);

        List<SagaEnvelope<String>> found = store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(60), 10);

        assertAll(
                () -> assertThat(found).extracting(SagaInstance::sagaId).containsExactly("order-5"),
                () -> assertThat(found.getFirst().failure()).isEqualTo(failure()),
                () -> assertThat(found.getFirst().started()).isTrue(),
                () -> assertThat(found.getFirst().state()).isNull()
        );
    }

    @Test
    void a_quarantined_instance_with_a_due_timer_is_not_returned_by_the_due_timer_query() {
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        store.compareAndSave("order-6", quarantined("order-6"), 0);

        assertThat(store.findWithDueTimers(NOW.plusSeconds(60), 10)).isEmpty();
    }
}
