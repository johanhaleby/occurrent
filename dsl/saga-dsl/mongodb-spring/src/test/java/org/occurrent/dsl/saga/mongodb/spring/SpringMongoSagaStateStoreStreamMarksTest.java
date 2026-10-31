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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. Since 0.35.0 a saga stores, next to the highest version it has folded per stream, which versions it has
 * folded per stream and cloud event type, as {@code streamMarks}. A stream id and a cloud event type are free text and
 * can hold dots, so they are stored as values rather than as field names. A document written before 0.35.0 has no
 * {@code streamMarks} and has to keep loading.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore stream marks")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreStreamMarksTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-stream-marks";
    private static final String SAGA_ID = "order-1";
    private static final String STREAM_ID = "orders.2026.order-1";
    private static final String PLACED_TYPE = "com.example.orders.OrderPlaced";
    private static final String PAID_TYPE = "com.example.orders.Payment$Reserved";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Map<String, StreamMarks> MARKS = Map.of(
            STREAM_ID, new StreamMarks(3, Map.of(
                    PLACED_TYPE, new TypeMark(12, List.of(9L, 10L, 11L), 8),
                    PAID_TYPE, new TypeMark(5, List.of(), 1))),
            "other", new StreamMarks(Long.MIN_VALUE, Map.of(PLACED_TYPE, new TypeMark(1, List.of(), Long.MIN_VALUE))));
    // Stream "other" is what the failure of a first event inserts, a stream with a floor of nothing seen, no type marks and a pending version.
    private static final Map<String, StreamMarks> PENDING_MARKS = Map.of(
            STREAM_ID, new StreamMarks(3, Map.of(
                    PLACED_TYPE, new TypeMark(12, List.of(9L, 10L, 11L), 8),
                    PAID_TYPE, new TypeMark(5, List.of(), 1)),
                    Map.of(PLACED_TYPE, List.of(4L, 6L), PAID_TYPE, List.of(2L))),
            "other", new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(PLACED_TYPE, List.of(1L))));
    // Versions that failed before the event type was listed with them, next to versions that are pending and so have a type.
    private static final Map<String, StreamMarks> LEGACY_FAILED_MARKS = Map.of(
            STREAM_ID, new StreamMarks(3, Map.of(
                    PLACED_TYPE, new TypeMark(12, List.of(9L, 10L, 11L), 8)),
                    Map.of(PLACED_TYPE, List.of(4L)), List.of(5L, 7L)),
            "other", new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(), List.of(2L)));

    private MongoOperations mongoOperations;
    private SagaStateStore<String> stateStore;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-marks-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    private static SagaEnvelope<String> instanceWith(Map<String, StreamMarks> marks, long version) {
        return new SagaEnvelope<>(SAGA_ID, "awaiting-payment", SagaStatus.ACTIVE, version, List.of(), Map.of(STREAM_ID, 12L, "other", 1L), null,
                NOW, NOW, null, null, true, null, marks);
    }

    private void save(SagaEnvelope<String> envelope, long expectedVersion) {
        assertThat(stateStore.compareAndSave(SAGA_ID, envelope, expectedVersion)).isTrue();
    }

    private Document rawDocument() {
        return requireNonNull(mongoOperations.findOne(Query.query(where("_id").is(SAGA_ID)), Document.class, COLLECTION));
    }

    // Exactly the fields the store wrote in 0.34.0 for an active instance, which has no streamMarks.
    private static Document documentWrittenBy0_34() {
        return new Document("_id", SAGA_ID)
                .append("status", "ACTIVE")
                .append("version", 1L)
                .append("timers", List.of())
                .append("streamWatermarks", new Document("order-stream", 7L))
                .append("positionWatermark", 3L)
                .append("createdAt", NOW.toEpochMilli())
                .append("updatedAt", NOW.toEpochMilli())
                .append("state", "awaiting-payment");
    }

    @Nested
    @DisplayName("when an instance with marks is saved")
    class WhenAnInstanceWithMarksIsSaved {

        @Test
        void find_reads_back_the_marks_of_every_stream_and_type() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks()).isEqualTo(MARKS);
        }

        @Test
        void a_stream_id_and_a_type_with_dots_are_stored_as_values_and_not_as_field_names() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            Document raw = rawDocument();

            // Then
            List<Document> streams = raw.getList("streamMarks", Document.class);
            Document stream = streams.stream().filter(s -> STREAM_ID.equals(s.getString("streamId"))).findFirst().orElseThrow();
            List<Document> types = stream.getList("types", Document.class);
            assertAll(
                    () -> assertThat(streams).hasSize(2),
                    () -> assertThat(stream.getLong("floor")).isEqualTo(3L),
                    () -> assertThat(types).extracting(t -> t.getString("type")).containsExactlyInAnyOrder(PLACED_TYPE, PAID_TYPE),
                    () -> assertThat(types).filteredOn(t -> PLACED_TYPE.equals(t.getString("type"))).singleElement().satisfies(placed -> {
                        assertThat(placed.getLong("mark")).isEqualTo(12L);
                        assertThat(placed.getList("applied", Long.class)).containsExactly(9L, 10L, 11L);
                    })
            );
        }

        @Test
        void the_horizon_of_every_type_round_trips() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.streamMarks().get(STREAM_ID).byType().get(PLACED_TYPE).horizon()).isEqualTo(8L),
                    () -> assertThat(read.streamMarks().get(STREAM_ID).byType().get(PAID_TYPE).horizon()).isEqualTo(1L),
                    () -> assertThat(read.streamMarks().get("other").byType().get(PLACED_TYPE).horizon()).isEqualTo(Long.MIN_VALUE)
            );
        }

        @Test
        void the_horizon_is_stored_as_a_field_of_the_type_entry() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            Document raw = rawDocument();

            // Then
            Document stream = raw.getList("streamMarks", Document.class).stream().filter(s -> STREAM_ID.equals(s.getString("streamId"))).findFirst().orElseThrow();
            Document placed = stream.getList("types", Document.class).stream().filter(t -> PLACED_TYPE.equals(t.getString("type"))).findFirst().orElseThrow();
            assertThat(placed.getLong("horizon")).isEqualTo(8L);
        }

        @Test
        void the_lowest_possible_floor_round_trips() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get("other").floor()).isEqualTo(Long.MIN_VALUE);
        }

        @Test
        void an_update_replaces_the_marks_it_had_before() {
            // Given
            save(instanceWith(MARKS, 1), 0);
            Map<String, StreamMarks> later = Map.of(STREAM_ID, new StreamMarks(3, Map.of(PLACED_TYPE, new TypeMark(13, List.of(10L, 12L), 9))));

            // When
            save(instanceWith(later, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(later);
        }

        @Test
        void findWithoutState_returns_the_marks() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.state()).isNull(),
                    () -> assertThat(read.streamMarks()).isEqualTo(MARKS)
            );
        }
    }

    @Nested
    @DisplayName("when an instance without marks is saved")
    class WhenAnInstanceWithoutMarksIsSaved {

        @Test
        void no_streamMarks_field_is_written() {
            // Given
            save(instanceWith(Map.of(), 1), 0);

            // When
            Document raw = rawDocument();

            // Then
            assertThat(raw.containsKey("streamMarks")).isFalse();
        }

        @Test
        void a_save_with_empty_marks_takes_away_the_marks_the_document_had() {
            // Given
            save(instanceWith(MARKS, 1), 0);

            // When
            save(instanceWith(Map.of(), 2), 1);

            // Then
            assertAll(
                    () -> assertThat(rawDocument().containsKey("streamMarks")).isFalse(),
                    () -> assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the document was written before 0.35.0")
    class WhenTheDocumentWasWrittenBefore0_35_0 {

        @Test
        void find_loads_it_with_empty_marks_and_its_watermarks() {
            // Given
            mongoOperations.insert(documentWrittenBy0_34(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.streamMarks()).isEmpty(),
                    () -> assertThat(read.streamWatermarks()).isEqualTo(Map.of("order-stream", 7L)),
                    () -> assertThat(read.state()).isEqualTo("awaiting-payment")
            );
        }

        @Test
        void findWithoutState_loads_it_with_empty_marks() {
            // Given
            mongoOperations.insert(documentWrittenBy0_34(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks()).isEmpty();
        }

        @Test
        void the_first_save_after_it_writes_the_marks() {
            // Given
            mongoOperations.insert(documentWrittenBy0_34(), COLLECTION);

            // When
            save(instanceWith(MARKS, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(MARKS);
        }
    }

    @Nested
    @DisplayName("when a stored type entry has no horizon")
    class WhenAStoredTypeEntryHasNoHorizon {

        // A type entry as the store wrote it before it kept a horizon, with a mark and applied versions only.
        private Document documentWithATypeEntryWithoutAHorizon() {
            Document typeEntry = new Document("type", PLACED_TYPE).append("mark", 12L).append("applied", List.of(9L, 10L, 11L));
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(typeEntry));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream));
        }

        @Test
        void find_reads_the_horizon_as_the_lowest_possible_version() {
            // Given
            mongoOperations.insert(documentWithATypeEntryWithoutAHorizon(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).byType().get(PLACED_TYPE)).isEqualTo(new TypeMark(12, List.of(9L, 10L, 11L), Long.MIN_VALUE));
        }

        @Test
        void findWithoutState_reads_the_horizon_as_the_lowest_possible_version() {
            // Given
            mongoOperations.insert(documentWithATypeEntryWithoutAHorizon(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).byType().get(PLACED_TYPE).horizon()).isEqualTo(Long.MIN_VALUE);
        }
    }

    @Nested
    @DisplayName("when a failure is written without the state")
    class WhenAFailureIsWrittenWithoutTheState {

        private SagaEnvelope<String> failureRecordFor(SagaEnvelope<String> read) {
            return new SagaEnvelope<>(SAGA_ID, read.state(), SagaStatus.QUARANTINED, read.version() + 1, read.timers(), read.streamWatermarks(), null,
                    read.createdAt(), NOW.plusSeconds(60), null, read.currentStep(), read.started(),
                    new SagaFailure(STREAM_ID + "@13", null, NOW, IllegalStateException.class.getName(), "boom"), read.streamMarks());
        }

        @Test
        void the_marks_the_record_carries_over_are_kept() {
            // Given
            save(instanceWith(MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, failureRecordFor(read), read.version());

            // Then
            SagaEnvelope<String> after = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(after.streamMarks()).isEqualTo(MARKS),
                    () -> assertThat(after.state()).isEqualTo("awaiting-payment")
            );
        }

        @Test
        void the_marks_are_among_the_fields_the_write_replaces() {
            // Given
            save(instanceWith(MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();
            Map<String, StreamMarks> other = Map.of(STREAM_ID, new StreamMarks(3, Map.of(PLACED_TYPE, new TypeMark(99, List.of(), 95))));
            SagaEnvelope<String> record = failureRecordFor(read);
            SagaEnvelope<String> withOtherMarks = new SagaEnvelope<>(SAGA_ID, record.state(), record.status(), record.version(), record.timers(),
                    record.streamWatermarks(), null, record.createdAt(), record.updatedAt(), null, record.currentStep(), record.started(),
                    record.failure(), other);

            // When
            stateStore.compareAndSaveWithoutState(SAGA_ID, withOtherMarks, read.version());

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(other);
        }
    }

    @Nested
    @DisplayName("when an instance with pending versions is saved")
    class WhenAnInstanceWithPendingVersionsIsSaved {

        private Document streamDocument(Document raw, String streamId) {
            return raw.getList("streamMarks", Document.class).stream().filter(s -> streamId.equals(s.getString("streamId"))).findFirst().orElseThrow();
        }

        @Test
        void find_reads_back_the_pending_versions_of_every_stream_and_type() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks()).isEqualTo(PENDING_MARKS);
        }

        @Test
        void findWithoutState_reads_back_the_pending_versions() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.state()).isNull(),
                    () -> assertThat(read.streamMarks()).isEqualTo(PENDING_MARKS)
            );
        }

        @Test
        void the_pending_versions_are_stored_as_a_pending_array_of_type_and_versions_on_each_stream() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);

            // When
            Document raw = rawDocument();

            // Then
            List<Document> pending = streamDocument(raw, STREAM_ID).getList("pending", Document.class);
            assertThat(pending).isNotNull();
            assertAll(
                    () -> assertThat(pending).extracting(entry -> entry.getString("type")).containsExactlyInAnyOrder(PLACED_TYPE, PAID_TYPE),
                    () -> assertThat(pending).filteredOn(entry -> PLACED_TYPE.equals(entry.getString("type"))).singleElement()
                            .satisfies(entry -> assertThat(entry.getList("versions", Long.class)).containsExactly(4L, 6L)),
                    () -> assertThat(pending).filteredOn(entry -> PAID_TYPE.equals(entry.getString("type"))).singleElement()
                            .satisfies(entry -> assertThat(entry.getList("versions", Long.class)).containsExactly(2L))
            );
        }

        @Test
        void a_stream_that_holds_nothing_but_a_pending_version_is_stored_and_read_back() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);

            // When
            Document raw = rawDocument();
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            Document other = streamDocument(raw, "other");
            assertAll(
                    () -> assertThat(other.getList("pending", Document.class)).hasSize(1),
                    () -> assertThat(read.streamMarks().get("other").byType()).isEmpty(),
                    () -> assertThat(read.streamMarks().get("other").pending()).isEqualTo(Map.of(PLACED_TYPE, List.of(1L))),
                    () -> assertThat(read.streamMarks().get("other").floor()).isEqualTo(Long.MIN_VALUE)
            );
        }

        @Test
        void an_update_replaces_the_pending_versions_it_had_before() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);
            Map<String, StreamMarks> later = Map.of(STREAM_ID, new StreamMarks(3,
                    Map.of(PLACED_TYPE, new TypeMark(13, List.of(10L, 12L), 9)), Map.of(PLACED_TYPE, List.of(6L))));

            // When
            save(instanceWith(later, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(later);
        }

        @Test
        void a_save_with_nothing_pending_takes_away_the_pending_versions_the_document_had() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);
            SagaEnvelope<String> before = stateStore.find(SAGA_ID).orElseThrow();

            // When
            save(instanceWith(MARKS, 2), 1);

            // Then
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(before.streamMarks().get(STREAM_ID).pending()).isNotEmpty(),
                    () -> assertThat(read.streamMarks()).isEqualTo(MARKS),
                    () -> assertThat(read.streamMarks().get(STREAM_ID).pending()).isEmpty()
            );
        }

        @Test
        void a_later_write_that_changes_other_fields_and_carries_the_marks_over_keeps_the_pending_versions() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();
            SagaEnvelope<String> changed = new SagaEnvelope<>(SAGA_ID, "paid", SagaStatus.ACTIVE, 2, read.timers(), Map.of(STREAM_ID, 13L, "other", 1L),
                    null, NOW, NOW.plusSeconds(60), null, null, true, null, read.streamMarks());

            // When
            save(changed, 1);

            // Then
            SagaEnvelope<String> after = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(after.state()).isEqualTo("paid"),
                    () -> assertThat(after.streamMarks()).isEqualTo(PENDING_MARKS)
            );
        }
    }

    @Nested
    @DisplayName("when a stored stream entry has no pending field")
    class WhenAStoredStreamEntryHasNoPendingField {

        private Document typeEntry() {
            return new Document("type", PLACED_TYPE).append("mark", 12L).append("applied", List.of(9L, 10L, 11L)).append("horizon", 8L);
        }

        private Document documentWithAStreamEntryWithoutPending() {
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(typeEntry()));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream));
        }

        // A stream entry as the store writes it once a version is pending, written by hand so the shape is pinned from the reading side too.
        private Document documentWithAStreamEntryWithPending() {
            Document pending = new Document("type", PLACED_TYPE).append("versions", List.of(4L, 6L));
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(typeEntry())).append("pending", List.of(pending));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream));
        }

        @Test
        void find_reads_it_with_nothing_pending() {
            // Given
            mongoOperations.insert(documentWithAStreamEntryWithoutPending(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.streamMarks().get(STREAM_ID).pending()).isEmpty(),
                    () -> assertThat(read.streamMarks().get(STREAM_ID).byType().get(PLACED_TYPE)).isEqualTo(new TypeMark(12, List.of(9L, 10L, 11L), 8))
            );
        }

        @Test
        void findWithoutState_reads_it_with_nothing_pending() {
            // Given
            mongoOperations.insert(documentWithAStreamEntryWithoutPending(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).pending()).isEmpty();
        }

        @Test
        void a_stream_entry_written_by_hand_with_a_pending_field_reads_its_versions_per_type() {
            // Given
            mongoOperations.insert(documentWithAStreamEntryWithPending(), COLLECTION);

            // When
            SagaEnvelope<String> viaFind = stateStore.find(SAGA_ID).orElseThrow();
            SagaEnvelope<String> viaFindWithoutState = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(viaFind.streamMarks().get(STREAM_ID).pending()).isEqualTo(Map.of(PLACED_TYPE, List.of(4L, 6L))),
                    () -> assertThat(viaFindWithoutState.streamMarks().get(STREAM_ID).pending()).isEqualTo(Map.of(PLACED_TYPE, List.of(4L, 6L)))
            );
        }

        @Test
        void the_first_save_after_it_writes_the_pending_versions() {
            // Given
            mongoOperations.insert(documentWithAStreamEntryWithoutPending(), COLLECTION);

            // When
            save(instanceWith(PENDING_MARKS, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(PENDING_MARKS);
        }
    }

    @Nested
    @DisplayName("when a pending version is written without the state")
    class WhenAPendingVersionIsWrittenWithoutTheState {

        private SagaEnvelope<String> withMarks(SagaEnvelope<String> read, Map<String, StreamMarks> marks) {
            return new SagaEnvelope<>(SAGA_ID, read.state(), read.status(), read.version() + 1, read.timers(), read.streamWatermarks(), null,
                    read.createdAt(), NOW.plusSeconds(60), null, read.currentStep(), read.started(), read.failure(), marks);
        }

        @Test
        void the_pending_version_is_stored_and_the_state_stays_where_it_was() {
            // Given
            save(instanceWith(MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, withMarks(read, PENDING_MARKS), read.version());

            // Then
            SagaEnvelope<String> after = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(after.streamMarks()).isEqualTo(PENDING_MARKS),
                    () -> assertThat(after.state()).isEqualTo("awaiting-payment"),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(after.failure()).isNull()
            );
        }

        @Test
        void a_write_that_carries_the_pending_versions_over_keeps_them() {
            // Given
            save(instanceWith(PENDING_MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            stateStore.compareAndSaveWithoutState(SAGA_ID, withMarks(read, read.streamMarks()), read.version());

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(PENDING_MARKS);
        }

        @Test
        void an_insert_of_an_instance_whose_first_event_failed_stores_the_pending_version() {
            // Given
            Map<String, StreamMarks> insertedMarks = Map.of(STREAM_ID, new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(PLACED_TYPE, List.of(1L))));
            SagaEnvelope<String> inserted = new SagaEnvelope<>(SAGA_ID, "initial", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null,
                    NOW, NOW, null, null, false, null, insertedMarks);

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, inserted, 0);

            // Then
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(read.started()).isFalse(),
                    () -> assertThat(read.streamMarks()).isEqualTo(insertedMarks)
            );
        }
    }

    @Nested
    @DisplayName("when an instance with failed versions of unknown type is saved")
    class WhenAnInstanceWithFailedVersionsOfUnknownTypeIsSaved {

        private Document streamDocument(Document raw, String streamId) {
            return raw.getList("streamMarks", Document.class).stream().filter(s -> streamId.equals(s.getString("streamId"))).findFirst().orElseThrow();
        }

        @Test
        void find_reads_back_the_failed_versions_of_every_stream() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks()).isEqualTo(LEGACY_FAILED_MARKS);
        }

        @Test
        void findWithoutState_reads_back_the_failed_versions() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.state()).isNull(),
                    () -> assertThat(read.streamMarks()).isEqualTo(LEGACY_FAILED_MARKS)
            );
        }

        @Test
        void the_failed_versions_are_stored_as_a_legacyFailed_array_of_versions_on_each_stream() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);

            // When
            Document raw = rawDocument();

            // Then
            assertAll(
                    () -> assertThat(streamDocument(raw, STREAM_ID).getList("legacyFailed", Long.class)).containsExactly(5L, 7L),
                    () -> assertThat(streamDocument(raw, "other").getList("legacyFailed", Long.class)).containsExactly(2L)
            );
        }

        @Test
        void an_update_replaces_the_failed_versions_it_had_before() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);
            Map<String, StreamMarks> later = Map.of(STREAM_ID, new StreamMarks(3,
                    Map.of(PLACED_TYPE, new TypeMark(13, List.of(10L, 12L), 9)), Map.of(), List.of(7L)));

            // When
            save(instanceWith(later, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(later);
        }

        @Test
        void a_save_with_no_failed_versions_takes_away_the_ones_the_document_had() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);

            // When
            save(instanceWith(MARKS, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks().get(STREAM_ID).legacyFailed()).isEmpty();
        }

        @Test
        void a_write_without_the_state_that_carries_the_marks_over_keeps_the_failed_versions() {
            // Given
            save(instanceWith(LEGACY_FAILED_MARKS, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();
            SagaEnvelope<String> carried = new SagaEnvelope<>(SAGA_ID, read.state(), read.status(), read.version() + 1, read.timers(), read.streamWatermarks(),
                    null, read.createdAt(), NOW.plusSeconds(60), null, read.currentStep(), read.started(), read.failure(), read.streamMarks());

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, carried, read.version());

            // Then
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(LEGACY_FAILED_MARKS)
            );
        }
    }

    @Nested
    @DisplayName("when a stored stream entry has no legacyFailed field")
    class WhenAStoredStreamEntryHasNoLegacyFailedField {

        private Document typeEntry() {
            return new Document("type", PLACED_TYPE).append("mark", 12L).append("applied", List.of(9L, 10L, 11L)).append("horizon", 8L);
        }

        // A stream entry as an earlier 0.35.0 build wrote it, with pending versions and no legacyFailed.
        private Document documentWithPendingAndNoLegacyFailed() {
            Document pending = new Document("type", PLACED_TYPE).append("versions", List.of(4L, 6L));
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(typeEntry())).append("pending", List.of(pending));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream));
        }

        private Document documentWithLegacyFailedWrittenAsIntegers() {
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(typeEntry()))
                    .append("pending", List.of()).append("legacyFailed", List.of(5, 7));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream));
        }

        @Test
        void find_reads_it_with_no_failed_versions_and_keeps_its_pending_versions() {
            // Given
            mongoOperations.insert(documentWithPendingAndNoLegacyFailed(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            StreamMarks marks = read.streamMarks().get(STREAM_ID);
            assertAll(
                    () -> assertThat(marks.legacyFailed()).isEmpty(),
                    () -> assertThat(marks.pending()).isEqualTo(Map.of(PLACED_TYPE, List.of(4L, 6L)))
            );
        }

        @Test
        void findWithoutState_reads_it_with_no_failed_versions_and_keeps_its_pending_versions() {
            // Given
            mongoOperations.insert(documentWithPendingAndNoLegacyFailed(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            StreamMarks marks = read.streamMarks().get(STREAM_ID);
            assertAll(
                    () -> assertThat(marks.legacyFailed()).isEmpty(),
                    () -> assertThat(marks.pending()).isEqualTo(Map.of(PLACED_TYPE, List.of(4L, 6L)))
            );
        }

        @Test
        void a_stream_entry_written_by_hand_with_integer_versions_reads_them_as_longs() {
            // Given
            mongoOperations.insert(documentWithLegacyFailedWrittenAsIntegers(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).legacyFailed()).containsExactly(5L, 7L);
        }

        @Test
        void the_first_save_after_it_writes_the_failed_versions() {
            // Given
            mongoOperations.insert(documentWithPendingAndNoLegacyFailed(), COLLECTION);

            // When
            save(instanceWith(LEGACY_FAILED_MARKS, 2), 1);

            // Then
            assertThat(stateStore.find(SAGA_ID).orElseThrow().streamMarks()).isEqualTo(LEGACY_FAILED_MARKS);
        }
    }

    @Nested
    @DisplayName("when an instance with pending versions that first failed at different instants is saved")
    class WhenAnInstanceWithPendingVersionsThatFirstFailedAtDifferentInstantsIsSaved {

        private static final Instant OLDER = Instant.parse("2026-01-01T00:00:10Z");
        private static final Instant NEWER = Instant.parse("2026-01-01T00:05:00Z");
        private static final String KEY_OF_FOUR = STREAM_ID + "@4";
        private static final String KEY_OF_SIX = STREAM_ID + "@6";
        private final Map<Long, Instant> clocks = Map.of(4L, OLDER, 6L, NEWER);
        private final Map<String, StreamMarks> marks = Map.of(
                STREAM_ID, new StreamMarks(3, Map.of(
                        PLACED_TYPE, new TypeMark(12, List.of(9L, 10L, 11L), 8)),
                        Map.of(PLACED_TYPE, List.of(4L, 6L)), List.of(), clocks));

        private SagaEnvelope<String> instanceFailingOn(String input, Instant firstFailedAt) {
            return new SagaEnvelope<>(SAGA_ID, "awaiting-payment", SagaStatus.ACTIVE, 1, List.of(), Map.of(STREAM_ID, 12L), null,
                    NOW, NOW, null, null, true, failure(input, firstFailedAt), marks);
        }

        private SagaFailure failure(String input, Instant firstFailedAt) {
            return new SagaFailure(input, null, firstFailedAt, IllegalStateException.class.getName(), "boom");
        }

        private SagaEnvelope<String> writeOver(SagaEnvelope<String> read, SagaFailure failure) {
            return new SagaEnvelope<>(SAGA_ID, read.state(), read.status(), read.version() + 1, read.timers(), read.streamWatermarks(), null,
                    read.createdAt(), NOW.plusSeconds(60), null, read.currentStep(), read.started(), failure, read.streamMarks());
        }

        private Map<Long, Instant> clocksOf(SagaEnvelope<String> envelope) {
            return envelope.streamMarks().get(STREAM_ID).pendingSince();
        }

        @Test
        void find_reads_back_the_instant_each_version_first_failed() {
            // Given
            save(instanceWith(marks, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(clocksOf(read)).isEqualTo(clocks),
                    () -> assertThat(read.streamMarks()).isEqualTo(marks)
            );
        }

        @Test
        void findWithoutState_reads_back_the_instant_each_version_first_failed() {
            // Given
            save(instanceWith(marks, 1), 0);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(clocksOf(read)).isEqualTo(clocks);
        }

        @Test
        void a_write_without_the_state_that_carries_the_marks_over_keeps_the_instants() {
            // Given
            save(instanceWith(marks, 1), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, writeOver(read, failure(KEY_OF_FOUR, OLDER)), read.version());

            // Then
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(clocksOf(stateStore.find(SAGA_ID).orElseThrow())).isEqualTo(clocks)
            );
        }

        @Test
        void a_write_without_the_state_that_replaces_the_failure_record_keeps_the_instant_of_the_input_it_no_longer_names() {
            // Given
            save(instanceFailingOn(KEY_OF_FOUR, OLDER), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, writeOver(read, failure(KEY_OF_SIX, NEWER)), read.version());

            // Then
            SagaEnvelope<String> after = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(requireNonNull(after.failure()).input()).isEqualTo(KEY_OF_SIX),
                    () -> assertThat(clocksOf(after)).isEqualTo(clocks)
            );
        }

        @Test
        void a_write_without_the_state_that_clears_the_failure_record_keeps_the_instants() {
            // Given
            save(instanceFailingOn(KEY_OF_FOUR, OLDER), 0);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // When
            boolean written = stateStore.compareAndSaveWithoutState(SAGA_ID, writeOver(read, null), read.version());

            // Then
            SagaEnvelope<String> after = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(written).isTrue(),
                    () -> assertThat(after.failure()).isNull(),
                    () -> assertThat(clocksOf(after)).isEqualTo(clocks)
            );
        }
    }

    @Nested
    @DisplayName("when a stored stream entry has no pending since field")
    class WhenAStoredStreamEntryHasNoPendingSinceField {

        private static final Instant FIRST_FAILED_AT = Instant.parse("2026-01-01T00:00:10Z");
        private static final String KEY_OF_FOUR = STREAM_ID + "@4";

        // A stream entry and a failure record as 0.35.0 wrote them before the instant of a pending version was kept.
        private Document documentWithAPendingVersionAndAFailureRecordButNoInstants() {
            Document type = new Document("type", PLACED_TYPE).append("mark", 12L).append("applied", List.of(9L, 10L, 11L)).append("horizon", 8L);
            Document pending = new Document("type", PLACED_TYPE).append("versions", List.of(4L));
            Document stream = new Document("streamId", STREAM_ID).append("floor", 3L).append("types", List.of(type)).append("pending", List.of(pending));
            return documentWrittenBy0_34().append("streamMarks", List.of(stream))
                    .append("failureInput", KEY_OF_FOUR)
                    .append("failureFirstFailedAt", FIRST_FAILED_AT.toEpochMilli())
                    .append("failureType", IllegalStateException.class.getName());
        }

        @Test
        void find_reads_it_with_no_instants() {
            // Given
            mongoOperations.insert(documentWithAPendingVersionAndAFailureRecordButNoInstants(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).pendingSince()).isEmpty();
        }

        @Test
        void findWithoutState_reads_it_with_no_instants() {
            // Given
            mongoOperations.insert(documentWithAPendingVersionAndAFailureRecordButNoInstants(), COLLECTION);

            // When
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.streamMarks().get(STREAM_ID).pendingSince()).isEmpty();
        }

        @Test
        void the_budget_of_the_input_the_failure_record_names_runs_from_the_first_failure_of_the_record() {
            // Given
            mongoOperations.insert(documentWithAPendingVersionAndAFailureRecordButNoInstants(), COLLECTION);
            SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();
            Saga<String, String, String> saga = Saga.<String, String, String>builder("initial")
                    .correlateAll(event -> event)
                    .startsOn(String.class)
                    .evolve(String.class, (state, event) -> state)
                    .react(String.class, (state, event) -> List.of())
                    .build();
            Instant now = FIRST_FAILED_AT.plus(Duration.ofMinutes(10));

            // When
            FailureRecord<String> record = SagaExecutionSupport.onFailure(saga, SAGA_ID, read, new EventMeta(STREAM_ID, 4L, null, PLACED_TYPE),
                    new IllegalStateException("boom"), now, Duration.ofMinutes(5), 2);

            // Then
            assertAll(
                    () -> assertThat(requireNonNull(record).envelope().failure().firstFailedAt()).isEqualTo(FIRST_FAILED_AT),
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().streamMarks().get(STREAM_ID).pendingSince()).isEqualTo(Map.of(4L, FIRST_FAILED_AT))
            );
        }
    }
}
