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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. An active instance that holds stream marks is stored with the status {@code ACTIVE_MARKED}, so that a
 * 0.34.0 node, which saves the whole document and knows nothing of the marks, throws on the status instead of saving
 * the marks away. Checks against real MongoDB which instances get that string, that every read and query treats it as
 * {@code ACTIVE}, and that a status string nothing here wrote is not read as {@code ACTIVE}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreActiveMarkedTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final long BASE = NOW.toEpochMilli();
    private static final String COLLECTION = "saga-active-marked";
    private static final Map<String, StreamMarks> MARKS = Map.of("order-1", new StreamMarks(6, Map.of()));

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-am-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    private static SpringMongoSagaStateStore<String> store(MongoOperations mongoOperations) {
        return new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    private static SagaFailure failure(@Nullable String quarantineId) {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", null, quarantineId);
    }

    private static SagaEnvelope<String> envelope(String sagaId, SagaStatus status, long version, List<TimerEntry> timers,
                                                 @Nullable SagaFailure failure, Map<String, StreamMarks> marks) {
        return new SagaEnvelope<>(sagaId, "awaiting-payment", status, version, timers, Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, marks);
    }

    private static SagaEnvelope<String> active(String sagaId, Map<String, StreamMarks> marks) {
        return envelope(sagaId, SagaStatus.ACTIVE, 1, List.of(), null, marks);
    }

    private static SagaEnvelope<String> activeWithTimer(String sagaId, Map<String, StreamMarks> marks, long firesAt) {
        return envelope(sagaId, SagaStatus.ACTIVE, 1, List.of(new TimerEntry("t", firesAt)), null, marks);
    }

    private static SagaEnvelope<String> activeUpdatedAt(String sagaId, Map<String, StreamMarks> marks, Instant updatedAt) {
        return new SagaEnvelope<>(sagaId, "awaiting-payment", SagaStatus.ACTIVE, 1, List.of(), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), updatedAt, null, null, true, null, marks);
    }

    private static String storedStatus(MongoOperations mongoOperations, String sagaId) {
        return requireNonNull(mongoOperations.findOne(Query.query(where("_id").is(sagaId)), Document.class, COLLECTION)).getString("status");
    }

    @Test
    void an_active_instance_with_stream_marks_is_stored_as_ACTIVE_MARKED_and_reads_back_ACTIVE_whichever_save_wrote_it() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("whole", active("whole", MARKS), 0);
        store.compareAndSave("without-state", active("without-state", Map.of()), 0);
        boolean saved = store.compareAndSaveWithoutState("without-state", envelope("without-state", SagaStatus.ACTIVE, 2, List.of(), null, MARKS), 1);

        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(storedStatus(mongoOperations, "whole")).as("stored by compareAndSave").isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(storedStatus(mongoOperations, "without-state")).as("stored by compareAndSaveWithoutState").isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(store.find("whole").orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(store.find("without-state").orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(store.find("whole").orElseThrow().streamMarks()).isEqualTo(MARKS)
        );
    }

    @Test
    void an_active_instance_without_stream_marks_is_stored_as_ACTIVE() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("unmarked", active("unmarked", Map.of()), 0);

        assertThat(storedStatus(mongoOperations, "unmarked")).isEqualTo("ACTIVE");
    }

    @Test
    void a_quarantined_instance_with_a_quarantine_id_and_stream_marks_is_stored_as_QUARANTINED_RECORDING() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("recording", envelope("recording", SagaStatus.QUARANTINED, 1, List.of(), failure("q-1"), MARKS), 0);

        assertAll(
                () -> assertThat(storedStatus(mongoOperations, "recording")).isEqualTo("QUARANTINED_RECORDING"),
                () -> assertThat(store.find("recording").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED)
        );
    }

    @Test
    void a_quarantined_instance_without_a_quarantine_id_and_a_completed_instance_keep_their_status_when_they_hold_stream_marks() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("quarantined", envelope("quarantined", SagaStatus.QUARANTINED, 1, List.of(), failure(null), MARKS), 0);
        store.compareAndSave("completed", envelope("completed", SagaStatus.COMPLETED, 1, List.of(), null, MARKS), 0);

        assertAll(
                () -> assertThat(storedStatus(mongoOperations, "quarantined")).isEqualTo("QUARANTINED"),
                () -> assertThat(storedStatus(mongoOperations, "completed")).isEqualTo("COMPLETED"),
                () -> assertThat(store.find("quarantined").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(store.find("completed").orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
        );
    }

    @Test
    void findByStatus_ACTIVE_returns_instances_stored_as_ACTIVE_and_as_ACTIVE_MARKED_and_findByStatus_QUARANTINED_returns_neither() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("plain", active("plain", Map.of()), 0);
        store.compareAndSave("marked", active("marked", MARKS), 0);
        store.compareAndSave("quarantined", envelope("quarantined", SagaStatus.QUARANTINED, 1, List.of(), failure(null), MARKS), 0);

        assertAll(
                () -> assertThat(storedStatus(mongoOperations, "marked")).as("the instance under test is stored as ACTIVE_MARKED").isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(60), 10)).extracting(SagaEnvelope::sagaId)
                        .containsExactlyInAnyOrder("plain", "marked"),
                () -> assertThat(store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(60), 10)).extracting(SagaEnvelope::sagaId)
                        .containsExactly("quarantined")
        );
    }

    @Test
    void findByStatus_ACTIVE_returns_ACTIVE_and_ACTIVE_MARKED_instances_interleaved_in_ascending_updatedAt_order_and_the_limit_cuts_that_ordered_list() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("marked-t5", activeUpdatedAt("marked-t5", MARKS, NOW.minusSeconds(50)), 0);
        store.compareAndSave("plain-t1", activeUpdatedAt("plain-t1", Map.of(), NOW.minusSeconds(90)), 0);
        store.compareAndSave("marked-t4", activeUpdatedAt("marked-t4", MARKS, NOW.minusSeconds(60)), 0);
        store.compareAndSave("marked-t2", activeUpdatedAt("marked-t2", MARKS, NOW.minusSeconds(80)), 0);
        store.compareAndSave("plain-t3", activeUpdatedAt("plain-t3", Map.of(), NOW.minusSeconds(70)), 0);

        List<SagaEnvelope<String>> firstThree = store.findByStatus(SagaStatus.ACTIVE, NOW, 3);
        List<SagaEnvelope<String>> all = store.findByStatus(SagaStatus.ACTIVE, NOW, 10);

        assertAll(
                () -> assertThat(List.of(storedStatus(mongoOperations, "plain-t1"), storedStatus(mongoOperations, "marked-t2"), storedStatus(mongoOperations, "plain-t3"),
                        storedStatus(mongoOperations, "marked-t4"), storedStatus(mongoOperations, "marked-t5")))
                        .as("the stored status strings interleave in updatedAt order")
                        .containsExactly("ACTIVE", "ACTIVE_MARKED", "ACTIVE", "ACTIVE_MARKED", "ACTIVE_MARKED"),
                () -> assertThat(firstThree).as("limit 3 cuts the ordered list across both stored strings").extracting(SagaEnvelope::sagaId)
                        .containsExactly("plain-t1", "marked-t2", "plain-t3"),
                () -> assertThat(all).as("limit above the total").extracting(SagaEnvelope::sagaId)
                        .containsExactly("plain-t1", "marked-t2", "plain-t3", "marked-t4", "marked-t5")
        );
    }

    @Test
    void findWithDueTimers_returns_an_ACTIVE_MARKED_instance_with_a_due_timer_in_due_order_with_the_ACTIVE_ones() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("marked-last", activeWithTimer("marked-last", MARKS, BASE - 1_000), 0);
        store.compareAndSave("plain", activeWithTimer("plain", Map.of(), BASE - 2_000), 0);
        store.compareAndSave("marked-first", activeWithTimer("marked-first", MARKS, BASE - 3_000), 0);
        store.compareAndSave("marked-not-due", activeWithTimer("marked-not-due", MARKS, BASE + 5_000), 0);

        assertAll(
                () -> assertThat(storedStatus(mongoOperations, "marked-first")).as("the instance under test is stored as ACTIVE_MARKED").isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(store.findWithDueTimers(NOW, 10)).extracting(SagaEnvelope::sagaId).containsExactly("marked-first", "plain", "marked-last"),
                () -> assertThat(store.findWithDueTimers(NOW, 2)).extracting(SagaEnvelope::sagaId).containsExactly("marked-first", "plain")
        );
    }

    @Test
    void find_findWithoutState_findByStatus_and_findWithDueTimers_return_an_ACTIVE_MARKED_instance_with_status_ACTIVE() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("marked", activeWithTimer("marked", MARKS, BASE - 1_000), 0);

        assertAll(
                () -> assertThat(storedStatus(mongoOperations, "marked")).as("the instance under test is stored as ACTIVE_MARKED").isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(store.find("marked").orElseThrow().status()).as("find").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(store.findWithoutState("marked").orElseThrow().status()).as("findWithoutState").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(60), 10)).as("findByStatus").extracting(SagaEnvelope::status)
                        .containsExactly(SagaStatus.ACTIVE),
                () -> assertThat(store.findWithDueTimers(NOW, 10)).as("findWithDueTimers").extracting(SagaEnvelope::status)
                        .containsExactly(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_failed_timer_attempt_saved_without_the_state_on_the_same_version_keeps_the_instance_ACTIVE_MARKED_and_due_again_after_the_attempt() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        store.compareAndSave("marked", activeWithTimer("marked", MARKS, BASE - 5_000), 0);
        SagaEnvelope<String> marked = envelope("marked", SagaStatus.ACTIVE, 1, List.of(new TimerEntry("t", BASE - 5_000, BASE - 500)), null, MARKS);

        boolean saved = store.compareAndSaveWithoutState("marked", marked, 1);

        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(storedStatus(mongoOperations, "marked")).isEqualTo("ACTIVE_MARKED"),
                () -> assertThat(store.findWithDueTimers(NOW.minusMillis(1_000), 10)).as("not due before the attempt time").isEmpty(),
                () -> assertThat(store.findWithDueTimers(NOW, 10)).extracting(SagaEnvelope::sagaId).containsExactly("marked")
        );
    }

    @Test
    void the_stored_string_ACTIVE_MARKED_is_not_a_SagaStatus_name() {
        assertThatThrownBy(() -> SagaStatus.valueOf(SpringMongoSagaStateStore.ACTIVE_MARKED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_document_stored_with_a_status_string_nobody_here_wrote_does_not_read_as_ACTIVE() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<String> store = store(mongoOperations);
        mongoOperations.insert(new Document("_id", "unknown").append("status", "ACTIVE_SOMETHING_NEW").append("version", 1L)
                .append("state", "awaiting-payment").append("timers", List.of()).append("streamWatermarks", new Document()), COLLECTION);

        assertThatThrownBy(() -> store.find("unknown")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ACTIVE_SOMETHING_NEW");
    }
}
