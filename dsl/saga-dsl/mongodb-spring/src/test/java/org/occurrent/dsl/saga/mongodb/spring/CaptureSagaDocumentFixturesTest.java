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
import java.util.TreeMap;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. Writes the documents {@link SpringMongoSagaStateStore} stores today to
 * {@code src/test/resources/saga-document-fixtures} as canonical extended JSON, and checks for each that the store reads
 * it back to the envelope {@link SagaDocumentFixtures} expects. The documents are the on-disk layout a later change of
 * the store's mapping must leave as it is. The one fixture not generated here, {@code quarantined-0.34.0}, is inserted
 * as it was written by hand and must decode to its expected envelope.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CaptureSagaDocumentFixturesTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Path FIXTURE_DIRECTORY = Path.of("src/test/resources/saga-document-fixtures");
    private static final String COLLECTION = "saga-fixtures";
    // A fixture file is written when it does not exist, or when this is set, and otherwise only compared with what the store writes now
    private static final boolean REGENERATE = Boolean.getBoolean("saga.fixtures.regenerate");
    private static final JsonWriterSettings CANONICAL_EXTENDED_JSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(true).build();

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-fixtures-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<ReviewEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    @Test
    void core_active_with_timers() {
        captureCore(SagaDocumentFixtures.CORE_ACTIVE_WITH_TIMERS, SagaDocumentFixtures.coreActiveWithTimers());
    }

    @Test
    void flow_active() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<FlowState<ReviewEvent>> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, rawFlowStateType(), SagaDocumentFixtures.reviewEventConverter());
        captureAndVerify(SagaDocumentFixtures.FLOW_ACTIVE, mongoOperations, store, SagaDocumentFixtures.flowActive());
    }

    @Test
    void active_marked() {
        captureCore(SagaDocumentFixtures.ACTIVE_MARKED, SagaDocumentFixtures.activeMarked());
    }

    @Test
    void quarantined_recording() {
        captureCore(SagaDocumentFixtures.QUARANTINED_RECORDING, SagaDocumentFixtures.quarantinedRecording());
    }

    @Test
    void releasing() {
        captureCore(SagaDocumentFixtures.RELEASING, SagaDocumentFixtures.releasing());
    }

    @Test
    void completed() {
        captureCore(SagaDocumentFixtures.COMPLETED, SagaDocumentFixtures.completed());
    }

    @Test
    void not_started() {
        captureCore(SagaDocumentFixtures.NOT_STARTED, SagaDocumentFixtures.notStarted());
    }

    @Test
    void the_hand_written_0_34_0_document_decodes_to_its_expected_envelope() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<OrderState> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class);
        SagaEnvelope<OrderState> expected = SagaDocumentFixtures.quarantined034();
        Document handWritten = Document.parse(read(SagaDocumentFixtures.QUARANTINED_0_34_0));
        mongoOperations.getCollection(COLLECTION).insertOne(handWritten);

        assertThat(store.find(expected.sagaId())).contains(expected);
    }

    private void captureCore(String name, SagaEnvelope<OrderState> expected) {
        MongoOperations mongoOperations = mongoOperations();
        captureAndVerify(name, mongoOperations, new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, OrderState.class), expected);
    }

    private static <S> void captureAndVerify(String name, MongoOperations mongoOperations, SpringMongoSagaStateStore<S> store, SagaEnvelope<S> expected) {
        String sagaId = expected.sagaId();
        assertThat(store.compareAndSave(sagaId, expected, 0)).as("the save of %s", name).isTrue();

        Document stored = requireNonNull(mongoOperations.findById(sagaId, Document.class, COLLECTION), "no document stored for " + name);
        String json = withStreamWatermarksInKeyOrder(stored).toJson(CANONICAL_EXTENDED_JSON);
        assertThat(store.find(sagaId)).as("the envelope read back for %s", name).contains(expected);

        if (REGENERATE || !Files.exists(FIXTURE_DIRECTORY.resolve(name + ".json"))) {
            write(name, json);
        }
        assertThat(read(name)).as("the committed fixture %s against what the store writes now", name).isEqualTo(json);

        // Replaces the document the store wrote with the committed file, so the check is on the file and not on the store's own output.
        mongoOperations.getCollection(COLLECTION).replaceOne(new Document("_id", sagaId), Document.parse(read(name)));
        assertThat(store.find(sagaId)).as("the envelope the committed fixture %s decodes to", name).contains(expected);
    }

    // Map.copyOf, which SagaEnvelope applies to the watermarks, iterates in an order that differs from one JVM to the next,
    // so the stored key order of a document with two or more watermarks would too. Sorting them keeps the file the same
    // on every run.
    private static Document withStreamWatermarksInKeyOrder(Document stored) {
        Document watermarks = stored.get("streamWatermarks", Document.class);
        stored.put("streamWatermarks", new Document(new TreeMap<>(watermarks)));
        return stored;
    }

    private static void write(String name, String json) {
        try {
            Files.createDirectories(FIXTURE_DIRECTORY);
            Files.writeString(FIXTURE_DIRECTORY.resolve(name + ".json"), json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(String name) {
        try {
            return Files.readString(FIXTURE_DIRECTORY.resolve(name + ".json"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
