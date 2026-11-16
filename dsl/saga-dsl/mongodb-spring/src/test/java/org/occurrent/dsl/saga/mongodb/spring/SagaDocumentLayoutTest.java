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
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. The documents in {@code src/test/resources/saga-document-fixtures} are what {@link SpringMongoSagaStateStore}
 * wrote before the saga codec took over the mapping, and one written by hand the way 0.34.0 wrote it. For each, the
 * committed document read through the store and the codec must give the expected envelope, and the expected envelope
 * written through the codec and the store must give the committed document again, with its field order and value types,
 * compared as canonical extended JSON.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SagaDocumentLayoutTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Path FIXTURE_DIRECTORY = Path.of("src/test/resources/saga-document-fixtures");
    private static final String COLLECTION = "saga-fixtures";
    private static final JsonWriterSettings CANONICAL_EXTENDED_JSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(true).build();

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-layout-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<ReviewEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    @Test
    void core_active_with_timers_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.CORE_ACTIVE_WITH_TIMERS, SagaDocumentFixtures.coreActiveWithTimers());
    }

    @Test
    void core_active_with_timers_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.CORE_ACTIVE_WITH_TIMERS, SagaDocumentFixtures.coreActiveWithTimers());
    }

    @Test
    void flow_active_document_decodes_to_the_expected_envelope() {
        verifyFlowDecodes(SagaDocumentFixtures.FLOW_ACTIVE, SagaDocumentFixtures.flowActive());
    }

    @Test
    void flow_active_envelope_encodes_to_the_committed_document() {
        verifyFlowEncodes(SagaDocumentFixtures.FLOW_ACTIVE, SagaDocumentFixtures.flowActive());
    }

    @Test
    void active_marked_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.ACTIVE_MARKED, SagaDocumentFixtures.activeMarked());
    }

    @Test
    void active_marked_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.ACTIVE_MARKED, SagaDocumentFixtures.activeMarked());
    }

    @Test
    void quarantined_recording_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.QUARANTINED_RECORDING, SagaDocumentFixtures.quarantinedRecording());
    }

    @Test
    void quarantined_recording_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.QUARANTINED_RECORDING, SagaDocumentFixtures.quarantinedRecording());
    }

    @Test
    void releasing_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.RELEASING, SagaDocumentFixtures.releasing());
    }

    @Test
    void releasing_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.RELEASING, SagaDocumentFixtures.releasing());
    }

    @Test
    void completed_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.COMPLETED, SagaDocumentFixtures.completed());
    }

    @Test
    void completed_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.COMPLETED, SagaDocumentFixtures.completed());
    }

    @Test
    void not_started_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.NOT_STARTED, SagaDocumentFixtures.notStarted());
    }

    @Test
    void not_started_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.NOT_STARTED, SagaDocumentFixtures.notStarted());
    }

    @Test
    void quarantined_0_34_0_document_decodes_to_the_expected_envelope() {
        verifyDecodes(SagaDocumentFixtures.QUARANTINED_0_34_0, SagaDocumentFixtures.quarantined034());
    }

    @Test
    void quarantined_0_34_0_envelope_encodes_to_the_committed_document() {
        verifyEncodes(SagaDocumentFixtures.QUARANTINED_0_34_0, SagaDocumentFixtures.quarantined034());
    }

    private void verifyDecodes(String name, SagaEnvelope<OrderState> expected) {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<OrderState> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class);
        mongoOperations.getCollection(COLLECTION).insertOne(Document.parse(read(name)));

        assertThat(SagaEnvelopes.of(store, null).find(expected.sagaId())).as("the envelope the committed document %s decodes to", name).contains(expected);
    }

    private void verifyEncodes(String name, SagaEnvelope<OrderState> expected) {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<OrderState> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class);

        assertThat(SagaEnvelopes.of(store, null).compareAndSave(expected.sagaId(), expected, 0)).as("the save of %s", name).isTrue();

        assertThat(storedJson(mongoOperations, expected.sagaId(), name)).as("the document written for %s against the committed one", name).isEqualTo(read(name));
    }

    private void verifyFlowDecodes(String name, SagaEnvelope<FlowState<ReviewEvent>> expected) {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<FlowState<ReviewEvent>> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, rawFlowStateType(), SagaDocumentFixtures.reviewEventConverter());
        mongoOperations.getCollection(COLLECTION).insertOne(Document.parse(read(name)));

        assertThat(SagaEnvelopes.of(store, SagaDocumentFixtures.reviewEventConverter()).find(expected.sagaId()))
                .as("the envelope the committed document %s decodes to", name).contains(expected);
    }

    private void verifyFlowEncodes(String name, SagaEnvelope<FlowState<ReviewEvent>> expected) {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<FlowState<ReviewEvent>> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, rawFlowStateType(), SagaDocumentFixtures.reviewEventConverter());

        assertThat(SagaEnvelopes.of(store, SagaDocumentFixtures.reviewEventConverter()).compareAndSave(expected.sagaId(), expected, 0)).as("the save of %s", name).isTrue();

        assertThat(storedJson(mongoOperations, expected.sagaId(), name)).as("the document written for %s against the committed one", name).isEqualTo(read(name));
    }

    private static String storedJson(MongoOperations mongoOperations, String sagaId, String name) {
        return requireNonNull(mongoOperations.findById(sagaId, Document.class, COLLECTION), "no document stored for " + name).toJson(CANONICAL_EXTENDED_JSON);
    }

    private static String read(String name) {
        try {
            return Files.readString(FIXTURE_DIRECTORY.resolve(name + ".json"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
