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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaHeldEvents.RefusedForSizeException;
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
 * and a handful of marks on the instance's document tell the executor that it does. This is the contract of that storage
 * against a real MongoDB, including the two writes MongoDB refuses for size, which the store must report as a
 * {@link RefusedForSizeException} without retrying them.
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
    // MongoDB's limit on one document, 16 MiB.
    private static final int DOCUMENT_LIMIT = 16 * 1024 * 1024;

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

    private static SagaEnvelope<String> envelope(String state, long version, SagaStatus status, boolean started, boolean heldEventsChecked,
                                                 Set<String> drainedHeldKeys, Set<String> selfHeldKeys, StartMark startMark) {
        return new SagaEnvelope<>(SAGA_ID, state, status, version, List.of(), Map.of("orders", 4L), null, NOW, NOW, null, null, started, null,
                Map.of(), heldEventsChecked, drainedHeldKeys, selfHeldKeys, startMark);
    }

    private void saveActiveStartedInstance(boolean heldEventsChecked, Set<String> selfHeldKeys) {
        SagaEnvelope<String> instance = envelope("state", 1, SagaStatus.ACTIVE, true, heldEventsChecked, Set.of(), selfHeldKeys, null);
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
        return heldEvent(key, cloudEvent("id-" + key), false, false);
    }

    private static HeldEvent heldEvent(String key, CloudEvent cloudEvent, boolean selfHeld, boolean outstanding) {
        return new HeldEvent(SAGA_ID, new HeldEventRef(key, "orders", 5L, 50L, "test.Event", NOW, selfHeld, outstanding), cloudEvent);
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
            HeldEvent first = heldEvent("orders@5", cloudEvent("first"), true, false);
            store.hold(SAGA_ID, first);

            // When
            store.hold(SAGA_ID, heldEvent("orders@5", cloudEvent("second"), false, true));

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
            HeldEvent second = heldEvent("orders@6", cloudEvent("id-orders@6"), true, false);
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
            HeldEventRef bare = new HeldEventRef("id-1 from urn:test:source", null, null, null, "test.Event", NOW, false, false);
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
            store.hold(SAGA_ID, heldEvent("orders@5", original, false, false));

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
            store.hold(SAGA_ID, heldEvent("orders@7", streamKeyed, false, false));

            // When
            CloudEvent read = store.held(SAGA_ID, List.of("orders@7")).get(0).cloudEvent();

            // Then
            assertAll(
                    () -> assertThat(OccurrentExtensionGetter.getStreamId(read)).isEqualTo("orders"),
                    () -> assertThat(OccurrentExtensionGetter.getStreamVersion(read)).isEqualTo(7L)
            );
        }

        @Test
        void held_returns_the_reference_flags_next_to_the_event() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5", cloudEvent("id-orders@5"), true, true));

            // When
            HeldEventRef ref = store.held(SAGA_ID, List.of("orders@5")).get(0).ref();

            // Then
            assertAll(
                    () -> assertThat(ref.selfHeld()).isTrue(),
                    () -> assertThat(ref.outstanding()).isTrue()
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
    @DisplayName("when an event is marked outstanding")
    class WhenAnEventIsMarkedOutstanding {

        @Test
        void markOutstanding_answers_true_and_sets_the_flag_on_a_held_event() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5", cloudEvent("id-orders@5"), true, false));

            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(marked).isTrue(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.outstanding()).isTrue())
            );
        }

        @Test
        void markOutstanding_changes_nothing_but_the_flag() {
            // Given
            HeldEvent held = heldEvent("orders@5", cloudEvent("id-orders@5"), true, false);
            store.hold(SAGA_ID, held);

            // When
            store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            HeldEventRef ref = held.ref();
            HeldEvent expected = new HeldEvent(SAGA_ID, new HeldEventRef(ref.key(), ref.streamId(), ref.version(), ref.position(), ref.type(), ref.heldAt(), true, true), held.cloudEvent());
            assertThat(contentOf(store.held(SAGA_ID, List.of("orders@5")))).containsExactly(contentOf(expected));
        }

        @Test
        void markOutstanding_answers_false_for_a_key_that_is_not_held() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@99");

            // Then
            assertAll(
                    () -> assertThat(marked).isFalse(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.outstanding()).isFalse())
            );
        }

        @Test
        void markOutstanding_does_not_insert_an_event_for_a_key_that_is_not_held() {
            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(marked).isFalse(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the instance is marked as holding events")
    class WhenTheInstanceIsMarkedAsHoldingEvents {

        @Test
        void markHeldEvents_returns_the_envelope_as_it_was_before_the_write() {
            // Given
            saveActiveStartedInstance(true, Set.of("orders@3"));

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isPresent(),
                    () -> assertThat(before.orElseThrow().version()).isEqualTo(1),
                    () -> assertThat(before.orElseThrow().heldEventsChecked()).isTrue(),
                    () -> assertThat(before.orElseThrow().selfHeldKeys()).containsExactly("orders@3"),
                    () -> assertThat(before.orElseThrow().streamWatermarks()).containsEntry("orders", 4L)
            );
        }

        @Test
        void markHeldEvents_returns_the_envelope_without_its_state() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(before.orElseThrow().state()).isNull();
        }

        @Test
        void markHeldEvents_leaves_the_stored_state_alone() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().state()).isEqualTo("state");
        }

        @Test
        void markHeldEvents_unsets_heldEventsChecked() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, null);

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().heldEventsChecked()).isFalse();
        }

        @Test
        void markHeldEvents_adds_the_key_to_the_selfHeldKeys_and_keeps_the_ones_already_there() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3"));

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactlyInAnyOrder("orders@3", "orders@5");
        }

        @Test
        void markHeldEvents_without_a_key_leaves_the_selfHeldKeys_as_they_were() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3"));

            // When
            store.markHeldEvents(SAGA_ID, null);

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactly("orders@3");
        }

        @Test
        void markHeldEvents_adding_a_key_already_there_leaves_it_once() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@5"));

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(rawInstance().getList("selfHeldKeys", String.class)).containsExactly("orders@5");
        }

        @Test
        void markHeldEvents_raises_the_version_by_one() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().version()).isEqualTo(2);
        }

        @Test
        void markHeldEvents_makes_a_compareAndSave_that_read_before_it_lose() {
            // Given
            saveActiveStartedInstance(true, Set.of());
            store.markHeldEvents(SAGA_ID, "orders@5");

            // When
            boolean saved = store.compareAndSave(SAGA_ID, envelope("later", 2, SagaStatus.ACTIVE, true, true, Set.of(), Set.of(), null), 1);

            // Then
            assertThat(saved).isFalse();
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_an_instance_that_does_not_exist() {
            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).isEmpty()
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_an_instance_that_has_not_started() {
            // Given
            SagaEnvelope<String> notStarted = envelope("state", 1, SagaStatus.ACTIVE, false, true, Set.of(), Set.of(), null);
            store.compareAndSave(SAGA_ID, notStarted, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(notStarted)
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_a_completed_instance() {
            // Given
            SagaEnvelope<String> completed = envelope("state", 1, SagaStatus.COMPLETED, true, true, Set.of(), Set.of(), null);
            store.compareAndSave(SAGA_ID, completed, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(completed)
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_a_quarantined_instance() {
            // Given
            SagaEnvelope<String> quarantined = envelope("state", 1, SagaStatus.QUARANTINED, true, true, Set.of(), Set.of(), null);
            store.compareAndSave(SAGA_ID, quarantined, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(quarantined)
            );
        }
    }

    @Nested
    @DisplayName("when a key is unmarked as self-held")
    class WhenAKeyIsUnmarkedAsSelfHeld {

        @Test
        void unmarkSelfHeld_removes_the_key_and_keeps_the_others() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3", "orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactly("orders@3");
        }

        @Test
        void unmarkSelfHeld_raises_the_version_by_one() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().version()).isEqualTo(2);
        }

        @Test
        void unmarkSelfHeld_leaves_heldEventsChecked_as_it_was() {
            // Given
            saveActiveStartedInstance(true, Set.of("orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().heldEventsChecked()).isTrue();
        }

        @Test
        void unmarkSelfHeld_of_an_instance_that_does_not_exist_writes_nothing() {
            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID)).isEmpty();
        }
    }

    @Nested
    @DisplayName("when the instance is deleted")
    class WhenTheInstanceIsDeleted {

        @Test
        void delete_removes_the_events_held_for_the_instance() {
            // Given
            saveActiveStartedInstance(false, Set.of());
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void delete_leaves_the_events_held_for_another_instance() {
            // Given
            saveActiveStartedInstance(false, Set.of());
            store.hold("another-instance", heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex("another-instance")).hasSize(1);
        }
    }

    @Nested
    @DisplayName("when the envelope carries held-event fields")
    class WhenTheEnvelopeCarriesHeldEventFields {

        private final SagaEnvelope<String> withHeldFields = envelope("state", 1, SagaStatus.ACTIVE, true, true,
                Set.of("orders@1", "orders@2"), Set.of("orders@9"), new StartMark("orders", 3, 30L));

        @Test
        void find_reads_back_the_four_fields_a_compareAndSave_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, withHeldFields, 0);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isTrue(),
                    () -> assertThat(read.drainedHeldKeys()).containsExactlyInAnyOrder("orders@1", "orders@2"),
                    () -> assertThat(read.selfHeldKeys()).containsExactly("orders@9"),
                    () -> assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3, 30L))
            );
        }

        @Test
        void find_reads_back_a_start_mark_without_a_position() {
            // Given
            store.compareAndSave(SAGA_ID, envelope("state", 1, SagaStatus.ACTIVE, true, false, Set.of(), Set.of(), new StartMark("orders", 3, null)), 0);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3, null));
        }

        @Test
        void find_reads_back_the_four_fields_a_compareAndSave_update_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, envelope("state", 1, SagaStatus.ACTIVE, true, false, Set.of(), Set.of(), null), 0);
            store.compareAndSave(SAGA_ID, envelope("state", 2, SagaStatus.ACTIVE, true, true, Set.of("orders@1"), Set.of("orders@9"), new StartMark("orders", 3, 30L)), 1);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isTrue(),
                    () -> assertThat(read.drainedHeldKeys()).containsExactly("orders@1"),
                    () -> assertThat(read.selfHeldKeys()).containsExactly("orders@9"),
                    () -> assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3, 30L))
            );
        }

        @Test
        void findWithoutState_reads_back_the_four_fields_a_compareAndSave_wrote() {
            // Given
            store.compareAndSave(SAGA_ID, withHeldFields, 0);

            // When
            SagaEnvelope<String> read = store.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isTrue(),
                    () -> assertThat(read.drainedHeldKeys()).containsExactlyInAnyOrder("orders@1", "orders@2"),
                    () -> assertThat(read.selfHeldKeys()).containsExactly("orders@9"),
                    () -> assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3, 30L))
            );
        }

        @Test
        void compareAndSaveWithoutState_writes_the_four_fields_and_leaves_the_stored_state_alone() {
            // Given
            store.compareAndSave(SAGA_ID, envelope("stored state", 1, SagaStatus.ACTIVE, true, false, Set.of(), Set.of(), null), 0);
            SagaEnvelope<String> update = envelope("ignored state", 2, SagaStatus.ACTIVE, true, true, Set.of("orders@1"), Set.of("orders@9"), new StartMark("orders", 3, 30L));

            // When
            boolean saved = store.compareAndSaveWithoutState(SAGA_ID, update, 1);

            // Then
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(read.state()).isEqualTo("stored state"),
                    () -> assertThat(read.heldEventsChecked()).isTrue(),
                    () -> assertThat(read.drainedHeldKeys()).containsExactly("orders@1"),
                    () -> assertThat(read.selfHeldKeys()).containsExactly("orders@9"),
                    () -> assertThat(read.startMark()).isEqualTo(new StartMark("orders", 3, 30L))
            );
        }

        @Test
        void compareAndSaveWithoutState_removes_the_fields_the_new_envelope_does_not_carry() {
            // Given
            store.compareAndSave(SAGA_ID, withHeldFields, 0);
            SagaEnvelope<String> update = envelope("state", 2, SagaStatus.ACTIVE, true, false, Set.of(), Set.of(), null);

            // When
            store.compareAndSaveWithoutState(SAGA_ID, update, 1);

            // Then
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isFalse(),
                    () -> assertThat(read.drainedHeldKeys()).isEmpty(),
                    () -> assertThat(read.selfHeldKeys()).isEmpty(),
                    () -> assertThat(read.startMark()).isNull()
            );
        }

        @Test
        void a_default_envelope_stores_none_of_the_four_fields() {
            // When
            store.compareAndSave(SAGA_ID, envelope("state", 1, SagaStatus.ACTIVE, true, false, Set.of(), Set.of(), null), 0);

            // Then
            Document stored = rawInstance();
            assertThat(stored.keySet()).doesNotContain("heldEventsChecked", "drainedHeldKeys", "selfHeldKeys", "startMark");
        }

        @Test
        void a_document_written_without_the_fields_reads_as_a_drain_due_with_nothing_drained_or_self_held() {
            // Given
            mongoOperations.insert(new Document("_id", SAGA_ID)
                    .append("status", "ACTIVE")
                    .append("version", 1L)
                    .append("timers", List.of())
                    .append("streamWatermarks", new Document("orders", 4L))
                    .append("createdAt", NOW.toEpochMilli())
                    .append("updatedAt", NOW.toEpochMilli())
                    .append("state", "state"), COLLECTION);

            // When
            SagaEnvelope<String> read = store.find(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isFalse(),
                    () -> assertThat(read.drainedHeldKeys()).isEmpty(),
                    () -> assertThat(read.selfHeldKeys()).isEmpty(),
                    () -> assertThat(read.startMark()).isNull()
            );
        }

        @Test
        void findWithoutState_reads_a_document_written_without_the_fields_as_defaults() {
            // Given
            mongoOperations.insert(new Document("_id", SAGA_ID)
                    .append("status", "ACTIVE")
                    .append("version", 1L)
                    .append("timers", List.of())
                    .append("streamWatermarks", new Document("orders", 4L))
                    .append("createdAt", NOW.toEpochMilli())
                    .append("updatedAt", NOW.toEpochMilli())
                    .append("state", "state"), COLLECTION);

            // When
            SagaEnvelope<String> read = store.findWithoutState(SAGA_ID).orElseThrow();

            // Then
            assertAll(
                    () -> assertThat(read.heldEventsChecked()).isFalse(),
                    () -> assertThat(read.drainedHeldKeys()).isEmpty(),
                    () -> assertThat(read.selfHeldKeys()).isEmpty(),
                    () -> assertThat(read.startMark()).isNull()
            );
        }
    }

    @Nested
    @DisplayName("when MongoDB refuses a write for size")
    class WhenMongoDbRefusesAWriteForSize {

        private final RetryStrategy retryThreeTimesWithoutWaiting = RetryStrategy.retry().backoff(Backoff.none()).maxAttempts(3);

        private MongoTemplate observed;
        private SpringMongoSagaStateStore<String> observingStore;

        @BeforeEach
        void createObservingStore() {
            observed = spy(newMongoTemplate());
            observingStore = new SpringMongoSagaStateStore<>(observed, COLLECTION, String.class, null, retryThreeTimesWithoutWaiting);
        }

        // An event whose body alone takes it past the document limit.
        private HeldEvent heldEventLargerThanTheDocumentLimit(String key) {
            String body = "a".repeat(DOCUMENT_LIMIT + 1024);
            CloudEvent huge = CloudEventBuilder.v1().withId("huge").withSource(URI.create("urn:test:source")).withType("test.Event")
                    .withData("text/plain", body.getBytes(StandardCharsets.UTF_8)).build();
            return heldEvent(key, huge, false, false);
        }

        // An instance whose document is a few hundred bytes short of the limit, so that adding a long key takes it past.
        private void saveInstanceCloseToTheDocumentLimit() {
            String state = "a".repeat(DOCUMENT_LIMIT - 1_500);
            assertThat(observingStore.compareAndSave(SAGA_ID, envelope(state, 1, SagaStatus.ACTIVE, true, true, Set.of(), Set.of(), null), 0)).isTrue();
        }

        private void verifyMarkHeldEventsWasAttemptedOnce() {
            then(observed).should(times(1)).findAndModify(any(Query.class), any(UpdateDefinition.class), any(FindAndModifyOptions.class), eq(Document.class), eq(COLLECTION));
        }

        @Test
        void hold_throws_RefusedForSizeException_for_an_event_that_alone_exceeds_the_document_limit() {
            // When
            Throwable thrown = catchThrowable(() -> observingStore.hold(SAGA_ID, heldEventLargerThanTheDocumentLimit("orders@5")));

            // Then
            assertThat(thrown).isInstanceOf(RefusedForSizeException.class);
        }

        @Test
        void hold_does_not_retry_an_event_that_exceeds_the_document_limit() {
            // When
            catchThrowable(() -> observingStore.hold(SAGA_ID, heldEventLargerThanTheDocumentLimit("orders@5")));

            // Then
            then(observed).should(times(1)).insert(any(Document.class), eq(HELD_COLLECTION));
        }

        @Test
        void hold_of_an_event_that_exceeds_the_document_limit_holds_nothing() {
            // When
            catchThrowable(() -> observingStore.hold(SAGA_ID, heldEventLargerThanTheDocumentLimit("orders@5")));

            // Then
            assertThat(observingStore.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void hold_throws_RefusedForSizeException_with_the_refusal_as_its_cause() {
            // When
            Throwable thrown = catchThrowable(() -> observingStore.hold(SAGA_ID, heldEventLargerThanTheDocumentLimit("orders@5")));

            // Then
            assertThat(thrown).hasCauseInstanceOf(RuntimeException.class);
        }

        @Test
        void markHeldEvents_throws_RefusedForSizeException_when_adding_the_key_takes_the_instance_past_the_document_limit() {
            // Given
            saveInstanceCloseToTheDocumentLimit();

            // When
            Throwable thrown = catchThrowable(() -> observingStore.markHeldEvents(SAGA_ID, "k".repeat(5_000)));

            // Then
            assertThat(thrown).isInstanceOf(RefusedForSizeException.class);
        }

        @Test
        void markHeldEvents_does_not_retry_a_key_that_takes_the_instance_past_the_document_limit() {
            // Given
            saveInstanceCloseToTheDocumentLimit();

            // When
            catchThrowable(() -> observingStore.markHeldEvents(SAGA_ID, "k".repeat(5_000)));

            // Then
            verifyMarkHeldEventsWasAttemptedOnce();
        }

        @Test
        void markHeldEvents_that_is_refused_for_size_changes_nothing_on_the_instance() {
            // Given
            saveInstanceCloseToTheDocumentLimit();

            // When
            catchThrowable(() -> observingStore.markHeldEvents(SAGA_ID, "k".repeat(5_000)));

            // Then
            SagaEnvelope<String> after = store.findWithoutState(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(after.version()).isEqualTo(1),
                    () -> assertThat(after.heldEventsChecked()).isTrue(),
                    () -> assertThat(after.selfHeldKeys()).isEmpty()
            );
        }

        @Test
        void markHeldEvents_without_a_key_is_accepted_on_an_instance_at_the_document_limit() {
            // Given
            saveInstanceCloseToTheDocumentLimit();

            // When
            Optional<SagaEnvelope<String>> before = observingStore.markHeldEvents(SAGA_ID, null);

            // Then
            assertAll(
                    () -> assertThat(before).isPresent(),
                    () -> assertThat(store.findWithoutState(SAGA_ID).orElseThrow().heldEventsChecked()).isFalse()
            );
        }

        @Test
        void markHeldEvents_retries_a_failure_that_is_not_a_size_refusal_up_to_the_attempt_limit() {
            // Given
            saveActiveStartedInstance(true, Set.of());
            DataAccessResourceFailureException transientFailure = new DataAccessResourceFailureException("MongoDB is down");
            willThrow(transientFailure).given(observed).findAndModify(any(Query.class), any(UpdateDefinition.class), any(FindAndModifyOptions.class), eq(Document.class), eq(COLLECTION));

            // When
            Throwable thrown = catchThrowable(() -> observingStore.markHeldEvents(SAGA_ID, "orders@5"));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(transientFailure),
                    () -> then(observed).should(times(3)).findAndModify(any(Query.class), any(UpdateDefinition.class), any(FindAndModifyOptions.class), eq(Document.class), eq(COLLECTION))
            );
        }
    }
}
