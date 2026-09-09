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
import org.springframework.data.mongodb.core.query.Update;
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
 * Docker-based. Checks that a quarantined instance survives a round trip through real MongoDB, that it can be read and
 * written without its state being decoded, which is the whole point of storing the record as top-level fields, and that
 * a document written before 0.34.0 still reads back as an instance that has started.
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

    /**
     * A state type a stored document can genuinely fail to decode into, which a {@code String} cannot be, because the
     * converter will hand back the stringified document instead of refusing it. Writing {@code amount} as text is what
     * a renamed class, a changed field type or a converter the application no longer has leaves in the document.
     */
    record Payment(long amount) {
    }

    private SpringMongoSagaStateStore<Payment> paymentStore(MongoOperations mongoOperations) {
        return new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
    }

    /** An active instance already carrying a failure record, which is what the executor reloads on the next failure. */
    private static SagaEnvelope<Payment> activePayment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of("order-1", 6L), 6L, NOW.minusSeconds(600),
                NOW, null, null, true, failure());
    }

    private static void makeTheStateUndecodable(MongoOperations mongoOperations, String sagaId) {
        mongoOperations.updateFirst(Query.query(where("_id").is(sagaId)),
                new Update().set("state", new Document("amount", "lots")), COLLECTION);
    }

    private static SagaFailure failure() {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom");
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
    void a_failure_record_from_a_store_with_no_global_position_round_trips_with_a_null_position() {
        // An event store built with withoutStreamPosition() gives its events a stream id and version and no position,
        // so the record that quarantines such an instance has none to hold either.
        SagaFailure withoutPosition = new SagaFailure("order-2@7", null, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom");
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        store.compareAndSave("order-2", new SagaEnvelope<>("order-2", "awaiting-payment", SagaStatus.QUARANTINED, 1,
                List.of(), Map.of("order-2", 6L), null, NOW.minusSeconds(600), NOW, null, null, true, withoutPosition), 0);

        SagaEnvelope<String> read = store.find("order-2").orElseThrow();

        assertAll(
                () -> assertThat(read.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(read.failure()).isEqualTo(withoutPosition),
                () -> assertThat(read.failure().position()).isNull()
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
        SpringMongoSagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-5", new SagaEnvelope<>("order-5", new Payment(12L), SagaStatus.QUARANTINED, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of("order-1", 6L), 6L, NOW.minusSeconds(600),
                NOW, null, null, true, failure()), 0);
        // The state an operator is most likely looking at is the one that no longer decodes, so make it undecodable.
        makeTheStateUndecodable(mongoOperations, "order-5");

        List<SagaEnvelope<Payment>> found = store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(60), 10);

        assertAll(
                () -> assertThat(found).extracting(SagaInstance::sagaId).containsExactly("order-5"),
                () -> assertThat(found.getFirst().failure()).isEqualTo(failure()),
                () -> assertThat(found.getFirst().started()).isTrue(),
                () -> assertThat(found.getFirst().state()).isNull()
        );
    }

    @Test
    void an_instance_whose_state_no_longer_decodes_is_still_read_without_it() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-7", activePayment("order-7"), 0);
        makeTheStateUndecodable(mongoOperations, "order-7");

        SagaEnvelope<Payment> read = store.findWithoutState("order-7").orElseThrow();

        assertAll(
                () -> assertThat(read.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(read.state()).isNull(),
                () -> assertThat(read.failure()).isEqualTo(failure()),
                () -> assertThat(read.version()).isEqualTo(1),
                // The watermarks come along, because the record the executor writes next carries them over and a
                // quarantine that dropped them would let a replay treat the failing event as already handled.
                () -> assertThat(read.streamWatermarks()).isEqualTo(Map.of("order-1", 6L)),
                () -> assertThat(read.positionWatermark()).isEqualTo(6L),
                () -> assertThat(read.timers()).extracting(TimerEntry::name).containsExactly("payment")
        );
    }

    @Test
    void reading_the_same_instance_with_its_state_still_fails() {
        // The companion to the case above, because findWithoutState is only worth having if find keeps its promise. A
        // caller that asked for the state is told it cannot be had rather than handed an instance with a null one.
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-8", activePayment("order-8"), 0);
        makeTheStateUndecodable(mongoOperations, "order-8");

        assertThatThrownBy(() -> store.find("order-8")).hasMessageContaining("lots");
    }

    @Test
    void quarantining_an_instance_whose_state_no_longer_decodes_leaves_that_state_where_it_is() {
        // The state is what somebody repairs the converter for, so the write that suspends the instance must not be what
        // destroys it. An ordinary save replaces the whole document, and the envelope saved here holds no state at all,
        // having been read without one.
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-9", activePayment("order-9"), 0);
        makeTheStateUndecodable(mongoOperations, "order-9");
        SagaEnvelope<Payment> read = store.findWithoutState("order-9").orElseThrow();

        boolean saved = store.compareAndSaveWithoutState("order-9", new SagaEnvelope<>("order-9", read.state(),
                SagaStatus.QUARANTINED, 2, read.timers(), read.streamWatermarks(), read.positionWatermark(),
                read.createdAt(), NOW, null, null, true, failure()), 1);

        Document stored = requireNonNull(mongoOperations.findOne(Query.query(where("_id").is("order-9")), Document.class, COLLECTION));
        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(stored.get("state")).isEqualTo(new Document("amount", "lots")),
                () -> assertThat(stored.getString("status")).isEqualTo("QUARANTINED"),
                () -> assertThat(stored.getLong("version")).isEqualTo(2L),
                () -> assertThat(stored.get("streamWatermarks")).isEqualTo(new Document("order-1", 6L))
        );
    }

    @Test
    void a_save_without_the_state_loses_to_a_version_that_has_moved_on() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-10", activePayment("order-10"), 0);

        assertThat(store.compareAndSaveWithoutState("order-10", activePayment("order-10"), 7)).isFalse();
    }

    @Test
    void a_save_without_the_state_drops_a_failure_field_the_new_record_does_not_carry() {
        // A failure on an event from a store that assigns no global position replaces a record that had one. Setting the
        // fields the new record carries and leaving the rest alone would keep the old position next to the new input.
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> store = paymentStore(mongoOperations);
        store.compareAndSave("order-11", activePayment("order-11"), 0);
        SagaFailure withoutPosition = new SagaFailure("order-1@8", null, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom");

        store.compareAndSaveWithoutState("order-11", new SagaEnvelope<>("order-11", null, SagaStatus.ACTIVE, 2,
                List.of(), Map.of("order-1", 6L), 6L, NOW.minusSeconds(600), NOW, null, null, true, withoutPosition), 1);

        Document stored = requireNonNull(mongoOperations.findOne(Query.query(where("_id").is("order-11")), Document.class, COLLECTION));
        assertAll(
                () -> assertThat(stored.containsKey("failurePosition")).isFalse(),
                () -> assertThat(stored.getString("failureInput")).isEqualTo("order-1@8")
        );
    }

    @Test
    void a_quarantined_instance_with_a_due_timer_is_not_returned_by_the_due_timer_query() {
        SagaStateStore<String> store = new SpringMongoSagaStateStore<>(mongoOperations(), COLLECTION, String.class);
        store.compareAndSave("order-6", quarantined("order-6"), 0);

        assertThat(store.findWithDueTimers(NOW.plusSeconds(60), 10)).isEmpty();
    }
}
