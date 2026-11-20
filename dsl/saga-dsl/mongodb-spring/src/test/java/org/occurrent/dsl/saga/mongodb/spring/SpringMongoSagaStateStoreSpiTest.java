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
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.mongodb.spring.SagaDocumentFixtures.OrderState;
import org.occurrent.dsl.saga.mongodb.spring.SagaDocumentFixtures.ReviewEvent;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Docker-based. {@link SpringMongoSagaStateStore} through the {@code SagaStateStore} SPI.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreSpiTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-spi";

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-spi-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    @Test
    void a_save_without_the_state_at_version_0_is_refused_naming_the_instance_and_stores_nothing() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<OrderState> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class);
        SagaEnvelope<OrderState> envelope = SagaDocumentFixtures.coreActiveWithTimers();
        StoredSaga insert = SagaCodec.of(store.stateCodec(), null).encodeWithoutState(envelope);

        assertThatThrownBy(() -> store.compareAndSaveWithoutState(insert, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(envelope.sagaId())
                .hasMessageContaining("compareAndSave");
        assertThat(mongoOperations.findAll(Document.class, COLLECTION)).isEmpty();
    }

    @Test
    void the_envelope_level_save_without_the_state_at_version_0_inserts_the_whole_instance() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<OrderState> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class);
        SagaEnvelope<OrderState> envelope = SagaDocumentFixtures.coreActiveWithTimers();

        boolean saved = SagaEnvelopes.of(store, null).compareAndSaveWithoutState(envelope.sagaId(), envelope, 0);

        assertThat(saved).isTrue();
        assertThat(SagaEnvelopes.of(store, null).find(envelope.sagaId())).contains(envelope);
    }

    @Test
    void a_store_given_a_converter_reads_a_flow_state_whatever_state_type_it_was_declared_with() {
        MongoOperations mongoOperations = mongoOperations();
        SagaEnvelope<FlowState<ReviewEvent>> expected = SagaDocumentFixtures.flowActive();
        SpringMongoSagaStateStore<FlowState<ReviewEvent>> writer = flowStore(mongoOperations);
        SagaEnvelopes.of(writer, SagaDocumentFixtures.reviewEventConverter()).compareAndSave(expected.sagaId(), expected, 0);
        SpringMongoSagaStateStore<Object> reader = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Object.class, SagaDocumentFixtures.reviewEventConverter());

        SagaEnvelope<Object> read = SagaEnvelopes.of(reader, SagaDocumentFixtures.reviewEventConverter()).find(expected.sagaId()).orElseThrow();

        assertThat(read.state()).isEqualTo(expected.state());
        assertThat(read.status()).isEqualTo(SagaStatus.ACTIVE);
    }

    @Test
    void a_store_without_a_converter_leaves_the_received_events_of_a_stored_state_as_they_are_stored() {
        MongoOperations mongoOperations = mongoOperations();
        SagaEnvelope<FlowState<ReviewEvent>> expected = SagaDocumentFixtures.flowActive();
        SagaEnvelopes.of(flowStore(mongoOperations), SagaDocumentFixtures.reviewEventConverter()).compareAndSave(expected.sagaId(), expected, 0);
        SpringMongoSagaStateStore<Object> reader = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Object.class);

        StoredSaga stored = reader.find(expected.sagaId()).orElseThrow();

        Object state = stored.encoded().fields().get(SagaCodec.STATE);
        assertThat(((Map<?, ?>) state).get(SagaCodec.FLOW_RECEIVED)).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .isNotEmpty().allSatisfy(event -> assertThat(event).isInstanceOf(String.class));
    }

    @Test
    void the_store_refuses_a_flow_state_type_without_a_converter() {
        MongoOperations mongoOperations = mongoOperations();

        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, rawFlowStateType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CloudEventConverter");
    }

    private static SpringMongoSagaStateStore<FlowState<ReviewEvent>> flowStore(MongoOperations mongoOperations) {
        return new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, rawFlowStateType(), SagaDocumentFixtures.reviewEventConverter());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<ReviewEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }
}
