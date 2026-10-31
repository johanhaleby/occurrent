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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A saga instance holds an event that arrives before its start in a collection beside the instances.
 * Deleting an instance removes the events held for it as well, because an event left behind would be applied to
 * whatever instance next starts under the same id.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore delete and the events held for the instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreHeldEventsDeleteTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-held-events-delete";
    private static final String SAGA_ID = "order-1";
    private static final String OTHER_SAGA_ID = "order-2";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private SpringMongoSagaStateStore<String> store;
    private SagaHeldEvents<String> heldEvents;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-held-delete-" + UUID.randomUUID()));
        MongoTemplate mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
        heldEvents = store;
    }

    private static SagaEnvelope<String> startedInstance(String sagaId) {
        return new SagaEnvelope<>(sagaId, "awaiting-payment", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW, NOW, null, null);
    }

    private static HeldEvent heldEvent(String sagaId, String streamId, long version) {
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId(streamId + "@" + version).withSource(URI.create("urn:test")).withType("com.example.ItemAdded").build();
        HeldEventRef ref = new HeldEventRef(streamId + "@" + version, streamId, version, null, "com.example.ItemAdded", NOW, false, null);
        return new HeldEvent(sagaId, ref, cloudEvent);
    }

    private List<String> heldKeysOf(String sagaId) {
        return heldEvents.heldIndex(sagaId).stream().map(HeldEventRef::key).sorted().toList();
    }

    @Nested
    @DisplayName("when an instance is deleted")
    class WhenAnInstanceIsDeleted {

        @Test
        void the_events_held_for_the_instance_are_removed_with_it_and_their_bodies_can_no_longer_be_read() {
            // Given
            store.compareAndSave(SAGA_ID, startedInstance(SAGA_ID), 0);
            heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 2));
            heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 3));

            // When
            store.delete(SAGA_ID);

            // Then
            assertAll(
                    () -> assertThat(store.find(SAGA_ID)).isEmpty(),
                    () -> assertThat(heldKeysOf(SAGA_ID)).isEmpty(),
                    () -> assertThat(heldEvents.held(SAGA_ID, List.of("order-stream@2", "order-stream@3"))).isEmpty());
        }

        @Test
        void the_events_held_for_another_instance_are_kept() {
            // Given
            store.compareAndSave(SAGA_ID, startedInstance(SAGA_ID), 0);
            heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 2));
            heldEvents.hold(OTHER_SAGA_ID, heldEvent(OTHER_SAGA_ID, "order-stream", 2));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(heldKeysOf(OTHER_SAGA_ID)).containsExactly("order-stream@2");
        }

        @Test
        void the_events_held_for_an_instance_that_has_not_started_are_removed_although_no_instance_document_exists() {
            // Given
            heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 2));
            assertThat(store.find(SAGA_ID)).as("no instance document").isEmpty();

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(heldKeysOf(SAGA_ID)).isEmpty();
        }

        @Test
        void an_event_with_the_same_key_can_be_held_again_for_the_id_afterwards() {
            // Given
            heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 2));
            store.delete(SAGA_ID);

            // When
            boolean held = heldEvents.hold(SAGA_ID, heldEvent(SAGA_ID, "order-stream", 2));

            // Then
            assertThat(held).isTrue();
        }

        @Test
        void deleting_an_id_that_holds_nothing_does_nothing() {
            // Given
            heldEvents.hold(OTHER_SAGA_ID, heldEvent(OTHER_SAGA_ID, "order-stream", 2));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(heldKeysOf(OTHER_SAGA_ID)).containsExactly("order-stream@2");
        }
    }
}
