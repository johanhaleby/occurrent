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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. A saga instance holds events beside its own document, in a collection named after the saga collection,
 * and each held event has a flag for whether it is a start event and a time for since when it has been failing. This is the contract of that
 * storage against MongoDB.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore held events")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaHeldEventsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-held";
    private static final String HELD_COLLECTION = COLLECTION + "-held-events";
    private static final String SAGA_ID = "order-1";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private ConnectionString connectionString;
    private MongoOperations mongoOperations;
    private SpringMongoSagaStateStore<String> store;

    @BeforeEach
    void createInstances() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-held-" + UUID.randomUUID()));
        mongoOperations = newMongoTemplate();
        store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    // A second client on the same database, for a store whose calls the test observes.
    private MongoTemplate newMongoTemplate() {
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    private static SagaEnvelope<String> envelope(String state, long version, SagaStatus status, boolean started, StartMark startMark) {
        return new SagaEnvelope<>(SAGA_ID, state, status, version, List.of(), Map.of("orders", 4L), null, NOW, NOW, null, null, started, null,
                Map.of(), startMark);
    }

    private void saveActiveStartedInstance() {
        SagaEnvelope<String> instance = envelope("state", 1, SagaStatus.ACTIVE, true, null);
        assertThat(store.compareAndSave(SAGA_ID, instance, 0)).isTrue();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test:source")).withType("test.Event")
                .withSubject("subject-" + id)
                .withData("application/json", "{\"name\":\"Rådhuset ✓\",\"amount\":12}".getBytes(StandardCharsets.UTF_8))
                .withExtension("tenant", "acme")
                .withExtension("retries", 3)
                .withExtension("urgent", true)
                .build();
    }

    private static HeldEvent heldEvent(String key) {
        return heldEvent(key, cloudEvent("id-" + key), false, null);
    }

    private static HeldEvent heldEvent(String key, CloudEvent cloudEvent, boolean startEvent, Instant failingSince) {
        return new HeldEvent(SAGA_ID, new HeldEventRef(key, "orders", 5L, 50L, "test.Event", NOW, startEvent, failingSince), cloudEvent);
    }

    // What a CloudEvent says, since two events with the same content are not equal when one carries its data as bytes and the other as parsed JSON.
    private record Content(HeldEventRef ref, String id, String type, URI source, String subject, String dataContentType, List<Byte> data, Map<String, Object> extensions) {
    }

    private static List<Content> contentOf(List<HeldEvent> events) {
        return events.stream().map(SpringMongoSagaHeldEventsTest::contentOf).toList();
    }

    private static Content contentOf(HeldEvent held) {
        CloudEvent event = held.cloudEvent();
        List<Byte> data = new ArrayList<>();
        for (byte b : requireNonNull(event.getData()).toBytes()) {
            data.add(b);
        }
        Map<String, Object> extensions = new HashMap<>();
        event.getExtensionNames().forEach(name -> extensions.put(name, event.getExtension(name)));
        return new Content(held.ref(), event.getId(), event.getType(), event.getSource(), event.getSubject(), event.getDataContentType(), data, extensions);
    }

    private Document rawInstance() {
        return requireNonNull(mongoOperations.findOne(Query.query(where("_id").is(SAGA_ID)), Document.class, COLLECTION));
    }

    @Nested
    @DisplayName("when an event is held")
    class WhenAnEventIsHeld {

        @Test
        void hold_answers_true_for_an_event_that_was_not_held() {
            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }

        @Test
        void hold_answers_false_for_a_second_event_with_the_same_key() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isFalse();
        }

        @Test
        void a_second_hold_of_the_same_key_leaves_the_first_copy_in_place() {
            // Given
            HeldEvent first = heldEvent("orders@5", cloudEvent("first"), true, null);
            store.hold(SAGA_ID, first);

            // When
            store.hold(SAGA_ID, heldEvent("orders@5", cloudEvent("second"), false, Instant.parse("2026-02-01T00:00:00Z")));

            // Then
            assertThat(contentOf(store.held(SAGA_ID, List.of("orders@5")))).containsExactly(contentOf(first));
        }

        @Test
        void the_same_key_can_be_held_for_two_instances() {
            // Given
            store.hold("order-1", heldEvent("orders@5"));

            // When
            boolean inserted = store.hold("order-2", heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }

        @Test
        void the_event_is_stored_in_the_held_events_collection_of_the_saga_collection_under_the_saga_id_and_key() {
            // When
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            Document stored = requireNonNull(mongoOperations.findOne(new Query(), Document.class, HELD_COLLECTION));
            assertThat(stored.get("_id")).isEqualTo(new Document("sagaId", SAGA_ID).append("key", "orders@5"));
        }

        @Test
        void the_cloud_event_is_stored_as_json_text() {
            // When
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            Document stored = requireNonNull(mongoOperations.findOne(new Query(), Document.class, HELD_COLLECTION));
            assertThat(stored.get("cloudEvent")).isInstanceOfSatisfying(String.class, json -> assertThat(json).contains("\"id\":\"id-orders@5\""));
        }

        @Test
        void the_store_indexes_the_held_events_by_saga_id_and_when_they_were_held() {
            // When
            List<Document> indexes = mongoOperations.getCollection(HELD_COLLECTION).listIndexes().into(new ArrayList<>());

            // Then
            assertThat(indexes).anySatisfy(index -> assertThat(index.get("key")).isEqualTo(new Document("sagaId", 1).append("heldAt", 1)));
        }

        @Test
        void heldIndex_lists_the_reference_of_every_event_held_for_the_instance() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            HeldEvent second = heldEvent("orders@6", cloudEvent("id-orders@6"), true, null);
            store.hold(SAGA_ID, first);
            store.hold(SAGA_ID, second);
            store.hold("another-instance", heldEvent("orders@7"));

            // When
            List<HeldEventRef> index = store.heldIndex(SAGA_ID);

            // Then
            assertThat(index).containsExactlyInAnyOrder(first.ref(), second.ref());
        }

        @Test
        void heldIndex_reads_a_reference_whose_event_carries_no_stream_key_or_position() {
            // Given
            HeldEventRef bare = new HeldEventRef("id-1 from urn:test:source", null, null, null, "test.Event", NOW, false, null);
            store.hold(SAGA_ID, new HeldEvent(SAGA_ID, bare, cloudEvent("id-1")));

            // When
            List<HeldEventRef> index = store.heldIndex(SAGA_ID);

            // Then
            assertThat(index).containsExactly(bare);
        }

        @Test
        void heldIndex_does_not_read_the_bodies() {
            // Given
            MongoTemplate observed = spy(newMongoTemplate());
            SpringMongoSagaStateStore<String> observingStore = new SpringMongoSagaStateStore<>(observed, COLLECTION, String.class);
            observingStore.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            observingStore.heldIndex(SAGA_ID);

            // Then
            then(observed).should().find(argThat((Query query) -> Integer.valueOf(0).equals(query.getFieldsObject().get("cloudEvent"))), eq(Document.class), eq(HELD_COLLECTION));
        }

        @Test
        void heldIndex_is_empty_for_an_instance_that_holds_nothing() {
            // When
            List<HeldEventRef> index = store.heldIndex(SAGA_ID);

            // Then
            assertThat(index).isEmpty();
        }

        @Test
        void held_reads_the_events_back_equal_to_the_cloud_events_that_were_held() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            HeldEvent second = heldEvent("orders@6");
            store.hold(SAGA_ID, first);
            store.hold(SAGA_ID, second);
            store.hold(SAGA_ID, heldEvent("orders@7"));

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5", "orders@6"));

            // Then
            assertThat(contentOf(held)).containsExactlyInAnyOrder(contentOf(first), contentOf(second));
        }

        @Test
        void held_keeps_the_id_type_source_data_and_extensions_of_the_cloud_event() {
            // Given
            CloudEvent original = cloudEvent("original");
            store.hold(SAGA_ID, heldEvent("orders@5", original, false, null));

            // When
            CloudEvent read = store.held(SAGA_ID, List.of("orders@5")).get(0).cloudEvent();

            // Then
            assertAll(
                    () -> assertThat(read.getId()).isEqualTo(original.getId()),
                    () -> assertThat(read.getType()).isEqualTo(original.getType()),
                    () -> assertThat(read.getSource()).isEqualTo(original.getSource()),
                    () -> assertThat(read.getSubject()).isEqualTo(original.getSubject()),
                    () -> assertThat(read.getData().toBytes()).isEqualTo(original.getData().toBytes()),
                    () -> assertThat(read.getExtension("tenant")).isEqualTo("acme"),
                    () -> assertThat(read.getExtension("retries")).isEqualTo(3),
                    () -> assertThat(read.getExtension("urgent")).isEqualTo(true)
            );
        }

        @Test
        void held_keeps_the_stream_key_the_event_store_wrote_on_the_cloud_event() {
            // Given
            CloudEvent streamKeyed = CloudEventBuilder.v1(cloudEvent("streamed")).withExtension(OccurrentCloudEventExtension.occurrent("orders", 7)).build();
            store.hold(SAGA_ID, heldEvent("orders@7", streamKeyed, false, null));

            // When
            CloudEvent read = store.held(SAGA_ID, List.of("orders@7")).get(0).cloudEvent();

            // Then
            assertAll(
                    () -> assertThat(OccurrentExtensionGetter.getStreamId(read)).isEqualTo("orders"),
                    () -> assertThat(OccurrentExtensionGetter.getStreamVersion(read)).isEqualTo(7L)
            );
        }

        @Test
        void held_returns_the_start_event_flag_and_the_failing_since_next_to_the_event() {
            // Given
            Instant failingSince = Instant.parse("2026-02-01T00:00:00Z");
            store.hold(SAGA_ID, heldEvent("orders@5", cloudEvent("id-orders@5"), true, failingSince));

            // When
            HeldEventRef ref = store.held(SAGA_ID, List.of("orders@5")).get(0).ref();

            // Then
            assertAll(
                    () -> assertThat(ref.startEvent()).isTrue(),
                    () -> assertThat(ref.failingSince()).isEqualTo(failingSince)
            );
        }

        @Test
        void held_leaves_out_a_key_that_is_not_held() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            store.hold(SAGA_ID, first);

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5", "orders@99"));

            // Then
            assertThat(contentOf(held)).containsExactly(contentOf(first));
        }

        @Test
        void held_does_not_return_an_event_held_for_another_instance() {
            // Given
            store.hold("another-instance", heldEvent("orders@5"));

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5"));

            // Then
            assertThat(held).isEmpty();
        }

        @Test
        void held_with_no_keys_is_empty() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of());

            // Then
            assertThat(held).isEmpty();
        }
    }

    @Nested
    @DisplayName("when a held event is released")
    class WhenAHeldEventIsReleased {

        @Test
        void release_removes_only_the_event_with_that_key() {
            // Given
            HeldEvent kept = heldEvent("orders@6");
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.hold(SAGA_ID, kept);

            // When
            store.release(SAGA_ID, "orders@5");

            // Then
            assertThat(store.heldIndex(SAGA_ID)).containsExactly(kept.ref());
        }

        @Test
        void release_does_not_remove_the_same_key_held_for_another_instance() {
            // Given
            store.hold("order-1", heldEvent("orders@5"));
            store.hold("order-2", heldEvent("orders@5"));

            // When
            store.release("order-1", "orders@5");

            // Then
            assertThat(store.heldIndex("order-2")).hasSize(1);
        }

        @Test
        void release_of_an_event_that_is_not_held_changes_nothing() {
            // Given
            HeldEvent held = heldEvent("orders@5");
            store.hold(SAGA_ID, held);

            // When
            store.release(SAGA_ID, "orders@99");

            // Then
            assertThat(store.heldIndex(SAGA_ID)).containsExactly(held.ref());
        }

        @Test
        void an_event_can_be_held_again_after_it_was_released() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.release(SAGA_ID, "orders@5");

            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }
    }

    @Nested
    @DisplayName("when a held event is recorded as failing")
    class WhenAHeldEventIsRecordedAsFailing {

        private static final Instant FIRST = Instant.parse("2026-02-01T00:00:00Z");
        private static final Instant LATER = Instant.parse("2026-03-01T00:00:00Z");

        @Test
        void failing_sets_the_failing_since_of_the_held_event() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.failing(SAGA_ID, "orders@5", FIRST);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(FIRST));
        }

        @Test
        void a_second_call_does_not_move_the_failing_since() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.failing(SAGA_ID, "orders@5", FIRST);

            // When
            store.failing(SAGA_ID, "orders@5", LATER);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(FIRST));
        }

        @Test
        void failing_changes_nothing_but_the_failing_since() {
            // Given
            HeldEvent held = heldEvent("orders@5", cloudEvent("id-orders@5"), true, null);
            store.hold(SAGA_ID, held);

            // When
            store.failing(SAGA_ID, "orders@5", FIRST);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().isEqualTo(new HeldEventRef(held.ref().key(), "orders", 5L, 50L, "test.Event", NOW, true, FIRST)),
                    () -> assertThat(contentOf(store.held(SAGA_ID, List.of("orders@5"))).get(0).id()).isEqualTo("id-orders@5")
            );
        }

        @Test
        void failing_does_not_insert_an_event_for_a_key_that_is_not_held() {
            // When
            store.failing(SAGA_ID, "orders@5", FIRST);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex(SAGA_ID)).isEmpty(),
                    () -> assertThat(mongoOperations.getCollection(HELD_COLLECTION).countDocuments()).isZero()
            );
        }

        @Test
        void failing_of_an_unknown_key_leaves_the_other_held_events_alone() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.failing(SAGA_ID, "orders@99", FIRST);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isNull());
        }

        @Test
        void failing_leaves_the_event_of_another_instance_with_the_same_key_alone() {
            // Given
            store.hold("order-1", heldEvent("orders@5"));
            store.hold("order-2", heldEvent("orders@5"));

            // When
            store.failing("order-1", "orders@5", FIRST);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex("order-1")).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(FIRST)),
                    () -> assertThat(store.heldIndex("order-2")).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isNull())
            );
        }

        @Test
        void a_held_event_that_is_held_again_after_failing_keeps_its_failing_since() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.failing(SAGA_ID, "orders@5", FIRST);

            // When
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(FIRST));
        }
    }

    @Nested
    @DisplayName("when a held event is read back")
    class WhenAHeldEventIsReadBack {

        @Test
        void heldIndex_and_held_read_back_the_start_event_flag_and_the_failing_since_that_were_held() {
            // Given
            Instant failingSince = Instant.parse("2026-02-01T00:00:00Z");
            HeldEvent held = heldEvent("orders@5", cloudEvent("id-orders@5"), true, failingSince);
            store.hold(SAGA_ID, held);

            // When
            HeldEventRef fromIndex = store.heldIndex(SAGA_ID).get(0);
            HeldEventRef fromHeld = store.held(SAGA_ID, List.of("orders@5")).get(0).ref();

            // Then
            assertAll(
                    () -> assertThat(fromIndex.startEvent()).isTrue(),
                    () -> assertThat(fromIndex.failingSince()).isEqualTo(failingSince),
                    () -> assertThat(fromHeld.startEvent()).isTrue(),
                    () -> assertThat(fromHeld.failingSince()).isEqualTo(failingSince)
            );
        }

        @Test
        void a_held_event_that_is_not_a_start_event_and_has_not_failed_reads_back_as_such() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            HeldEventRef ref = store.heldIndex(SAGA_ID).get(0);

            // Then
            assertAll(
                    () -> assertThat(ref.startEvent()).isFalse(),
                    () -> assertThat(ref.failingSince()).isNull()
            );
        }

        @Test
        void a_held_document_written_with_neither_field_reads_as_not_a_start_event_and_not_failing() {
            // Given
            mongoOperations.insert(new Document("_id", new Document("sagaId", SAGA_ID).append("key", "orders@5"))
                    .append("sagaId", SAGA_ID)
                    .append("key", "orders@5")
                    .append("streamId", "orders")
                    .append("version", 5L)
                    .append("position", 50L)
                    .append("type", "test.Event")
                    .append("heldAt", java.util.Date.from(NOW))
                    .append("cloudEvent", "{\"specversion\":\"1.0\",\"id\":\"legacy\",\"source\":\"urn:test:source\",\"type\":\"test.Event\"}"), HELD_COLLECTION);

            // When
            HeldEventRef fromIndex = store.heldIndex(SAGA_ID).get(0);
            HeldEventRef fromHeld = store.held(SAGA_ID, List.of("orders@5")).get(0).ref();

            // Then
            assertAll(
                    () -> assertThat(fromIndex.startEvent()).isFalse(),
                    () -> assertThat(fromIndex.failingSince()).isNull(),
                    () -> assertThat(fromHeld.startEvent()).isFalse(),
                    () -> assertThat(fromHeld.failingSince()).isNull()
            );
        }

        @Test
        void a_held_event_that_never_failed_stores_no_failing_since() {
            // When
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            Document stored = requireNonNull(mongoOperations.findOne(new Query(), Document.class, HELD_COLLECTION));
            assertThat(stored.keySet()).doesNotContain("failingSince");
        }
    }

    @Nested
    @DisplayName("when the instance is deleted")
    class WhenTheInstanceIsDeleted {

        @Test
        void delete_removes_the_events_held_for_the_instance() {
            // Given
            saveActiveStartedInstance();
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void delete_leaves_the_events_held_for_another_instance() {
            // Given
            saveActiveStartedInstance();
            store.hold("another-instance", heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex("another-instance")).hasSize(1);
        }
    }

    @Nested
    @DisplayName("when the envelope carries a start mark")
    class WhenTheEnvelopeCarriesAStartMark {

        private final SagaEnvelope<String> withStartMark = envelope("state", 1, SagaStatus.ACTIVE, true, new StartMark("orders", 3));

        @Test
        void find_reads_back_the_start_mark_a_compareAndSave_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, withStartMark, 0);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3));
        }

        @Test
        void the_stored_start_mark_has_only_the_stream_id_and_the_version() {
            // Given
            store.compareAndSave(SAGA_ID, withStartMark, 0);

            // When
            Document stored = rawInstance().get("startMark", Document.class);

            // Then
            assertThat(stored).isEqualTo(new Document("streamId", "orders").append("version", 3L));
        }

        @Test
        void find_reads_back_the_start_mark_a_compareAndSave_update_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, envelope("state", 1, SagaStatus.ACTIVE, true, null), 0);
            store.compareAndSave(SAGA_ID, envelope("state", 2, SagaStatus.ACTIVE, true, new StartMark("orders", 3)), 1);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3));
        }

        @Test
        void findWithoutState_reads_back_the_start_mark_a_compareAndSave_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, withStartMark, 0);

            // When
            SagaEnvelope<String> read = store.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3));
        }

        @Test
        void compareAndSaveWithoutState_writes_the_start_mark_and_leaves_the_stored_state_alone() {
            // Given
            store.compareAndSave(SAGA_ID, envelope("stored state", 1, SagaStatus.ACTIVE, true, null), 0);
            SagaEnvelope<String> update = envelope("ignored state", 2, SagaStatus.ACTIVE, true, new StartMark("orders", 3));

            // When
            boolean saved = store.compareAndSaveWithoutState(SAGA_ID, update, 1);

            // Then
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(read.state()).isEqualTo("stored state"),
                    () -> assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3))
            );
        }

        @Test
        void compareAndSaveWithoutState_removes_the_start_mark_the_new_envelope_does_not_carry() {
            // Given
            store.compareAndSave(SAGA_ID, withStartMark, 0);
            SagaEnvelope<String> update = envelope("state", 2, SagaStatus.ACTIVE, true, null);

            // When
            store.compareAndSaveWithoutState(SAGA_ID, update, 1);

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().startMark()).isNull();
        }

        @Test
        void a_default_envelope_stores_no_start_mark() {
            // When
            store.compareAndSave(SAGA_ID, envelope("state", 1, SagaStatus.ACTIVE, true, null), 0);

            // Then
            assertThat(rawInstance().keySet()).doesNotContain("startMark");
        }

        @Test
        void a_document_written_without_the_field_reads_as_having_no_start_mark() {
            // Given
            insertInstanceWithoutHeldEventFields();

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isNull();
        }

        @Test
        void findWithoutState_reads_a_document_written_without_the_field_as_having_no_start_mark() {
            // Given
            insertInstanceWithoutHeldEventFields();

            // When
            SagaEnvelope<String> read = store.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isNull();
        }

        private void insertInstanceWithoutHeldEventFields() {
            mongoOperations.insert(new Document("_id", SAGA_ID)
                    .append("status", "ACTIVE")
                    .append("version", 1L)
                    .append("timers", List.of())
                    .append("streamWatermarks", new Document("orders", 4L))
                    .append("createdAt", NOW.toEpochMilli())
                    .append("updatedAt", NOW.toEpochMilli())
                    .append("state", "state"), COLLECTION);
        }
    }

    @Nested
    @DisplayName("when the held events are listed without those waiting for a lower start")
    class WhenTheHeldEventsAreListedWithoutThoseWaitingForALowerStart {

        private final SagaEnvelope.StartMark start = new SagaEnvelope.StartMark("orders", 5);

        @BeforeEach
        void holdOneCopyOfEachKind() {
            store.hold(SAGA_ID, copy("orders@3", "orders", 3L, false));
            store.hold(SAGA_ID, copy("orders@4", "orders", 4L, false));
            store.hold(SAGA_ID, copy("orders@5", "orders", 5L, false));
            store.hold(SAGA_ID, copy("orders@7", "orders", 7L, false));
            store.hold(SAGA_ID, copy("orders@2", "orders", 2L, true));
            store.hold(SAGA_ID, copy("other@3", "other", 3L, false));
            store.hold(SAGA_ID, copy("keyless", null, null, false));
        }

        @Test
        void a_non_start_copy_on_the_starts_stream_below_the_start_is_left_out() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).doesNotContain("orders@3", "orders@4");
        }

        @Test
        void a_non_start_copy_at_the_starts_version_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@5");
        }

        @Test
        void a_non_start_copy_above_the_start_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@7");
        }

        @Test
        void a_copy_flagged_as_a_start_event_below_the_start_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@2");
        }

        @Test
        void a_copy_of_another_stream_below_the_starts_version_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("other@3");
        }

        @Test
        void a_copy_with_no_stream_key_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("keyless");
        }

        @Test
        void the_events_returned_are_those_of_heldIndex_less_the_waiting_ones() {
            // When
            List<HeldEventRef> listed = store.heldIndexWithoutWaiting(SAGA_ID, start);

            // Then
            assertThat(listed).containsExactlyInAnyOrderElementsOf(store.heldIndex(SAGA_ID).stream()
                    .filter(ref -> !ref.key().equals("orders@3") && !ref.key().equals("orders@4")).toList());
        }

        private List<String> keysWithoutWaiting() {
            return store.heldIndexWithoutWaiting(SAGA_ID, start).stream().map(HeldEventRef::key).toList();
        }

        private HeldEvent copy(String key, @Nullable String streamId, @Nullable Long version, boolean startEvent) {
            return new HeldEvent(SAGA_ID, new HeldEventRef(key, streamId, version, null, "test.Event", NOW, startEvent, null), cloudEvent("id-" + key));
        }

        @Test
        void the_query_agrees_with_the_rule_of_the_default_method_on_every_combination_of_stream_version_and_flag() {
            // Given
            store.delete(SAGA_ID);
            for (String streamId : List.of("orders", "other")) {
                for (long version = 1; version <= 8; version++) {
                    store.hold(SAGA_ID, copy(streamId + "@" + version, streamId, version, false));
                    store.hold(SAGA_ID, copy(streamId + "@" + version + "-start", streamId, version, true));
                }
            }
            store.hold(SAGA_ID, copy("keyless", null, null, false));
            store.hold(SAGA_ID, copy("keyless-start", null, null, true));
            SagaHeldEvents<String> byTheDefault = new HeldEventsWithTheDefault(store);

            // When
            List<HeldEventRef> byTheQuery = store.heldIndexWithoutWaiting(SAGA_ID, start);

            // Then
            assertAll(
                    () -> assertThat(byTheQuery).containsExactlyInAnyOrderElementsOf(byTheDefault.heldIndexWithoutWaiting(SAGA_ID, start)),
                    () -> assertThat(byTheQuery).hasSizeLessThan(store.heldIndex(SAGA_ID).size()));
        }
    }

    // Only heldIndex is the store's, so heldIndexWithoutWaiting is the default method of the interface.
    private record HeldEventsWithTheDefault(SpringMongoSagaStateStore<String> store) implements SagaHeldEvents<String> {
        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            return store.hold(sagaId, event);
        }

        @Override
        public void failing(String sagaId, String key, Instant at) {
            store.failing(sagaId, key, at);
        }

        @Override
        public List<HeldEventRef> heldIndex(String sagaId) {
            return store.heldIndex(sagaId);
        }

        @Override
        public List<HeldEvent> held(String sagaId, java.util.Collection<String> keys) {
            return store.held(sagaId, keys);
        }

        @Override
        public void release(String sagaId, String key) {
            store.release(sagaId, key);
        }
    }
}
