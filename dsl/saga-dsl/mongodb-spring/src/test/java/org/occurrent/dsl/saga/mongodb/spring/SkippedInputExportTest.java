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
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. The downgrade guide has the operator export the records of the skipped inputs with {@code mongoexport}
 * and rebuild each event from that export alone. Checks that a record written by the store, exported with
 * {@code --jsonFormat=canonical} and read with {@code Document.parse}, gives back every extension and every value
 * inside the data with the class and value it was delivered with, and that the export without the flag loses the difference
 * between an {@code Integer} and a {@code Long}, which is what the guide warns about. The event is rebuilt here from
 * the export, with no call into the store.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class SkippedInputExportTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-export";
    private static final String SKIPPED_INPUTS = COLLECTION + "_skipped_inputs";
    private static final String ORDER = "order-1";
    private static final String QUARANTINE_ID = "q-1";
    private static final String KEY = "order-1@3";
    private static final Instant NOW = Instant.parse("2026-03-04T05:16:17.456Z");

    private static final Decimal128 DECIMAL = new Decimal128(new BigDecimal("1234567890.123456789"));
    private static final Date DATE = new Date(1772601377456L);
    private static final Binary RAW = new Binary(new byte[]{0, 1, 2, (byte) 0xff, (byte) 0x80, 'a'});

    private String databaseName;
    private SpringMongoSagaStateStore<String> stateStore;
    private MongoTemplate template;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-export-" + UUID.randomUUID()));
        databaseName = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
    }

    @Test
    void a_canonical_export_rebuilds_every_extension_and_every_leaf_of_the_data_with_the_class_and_value_delivered() throws Exception {
        CloudEvent delivered = deliveredEvent();
        record(delivered);

        List<Document> export = export(true);

        assertThat(export).hasSize(1);
        Document record = export.get(0);
        CloudEvent rebuilt = rebuild(record);
        Map<String, Object> deliveredData = deliveredMap();
        Map<?, ?> rebuiltData = (Map<?, ?>) ((PojoCloudEventData<?>) requireNonNull(rebuilt.getData())).getValue();
        assertAll(
                () -> assertThat(record.get("data")).as("the delivered bytes are the JSON of the map, so the record holds no bytes").isNull(),
                () -> assertThat(rebuilt.getId()).isEqualTo(delivered.getId()),
                () -> assertThat(rebuilt.getType()).isEqualTo(delivered.getType()),
                () -> assertThat(rebuilt.getSubject()).isEqualTo(delivered.getSubject()),
                extension(rebuilt, "intext", 7),
                extension(rebuilt, "longext", 8L),
                extension(rebuilt, "doubleext", 2.5d),
                extension(rebuilt, "integraldoubleext", 42.0d),
                leaf(rebuiltData, "int", deliveredData.get("int")),
                leaf(rebuiltData, "long", deliveredData.get("long")),
                leaf(rebuiltData, "double", deliveredData.get("double")),
                leaf(rebuiltData, "decimal", DECIMAL),
                leaf(rebuiltData, "date", DATE),
                leaf(rebuiltData, "raw", RAW),
                () -> assertThat(rebuiltData.keySet()).isEqualTo(deliveredData.keySet()),
                () -> assertThat(requireNonNull(rebuilt.getData()).toBytes()).as("data bytes").isEqualTo(requireNonNull(delivered.getData()).toBytes()),
                () -> {
                    List<?> lines = (List<?>) rebuiltData.get("lines");
                    assertThat(lines).hasSize(1);
                    Map<?, ?> line = (Map<?, ?>) lines.get(0);
                    assertThat(line.get("qty")).isInstanceOf(Long.class).isEqualTo(9L);
                }
        );
    }

    @Test
    void an_export_without_the_canonical_flag_reads_a_Long_extension_and_a_Long_in_the_data_back_as_an_Integer() throws Exception {
        record(deliveredEvent());

        List<Document> export = export(false);

        assertThat(export).hasSize(1);
        Document record = export.get(0);
        CloudEvent rebuilt = rebuild(record);
        Map<?, ?> rebuiltData = (Map<?, ?>) ((PojoCloudEventData<?>) requireNonNull(rebuilt.getData())).getValue();
        List<?> lines = (List<?>) rebuiltData.get("lines");
        assertAll(
                () -> assertThat(rebuilt.getExtension("longext")).as("the Long extension 8").isInstanceOf(Integer.class).isEqualTo(8),
                () -> assertThat(rebuiltData.get("long")).as("the Long in the data").isInstanceOf(Integer.class).isEqualTo(5),
                () -> assertThat(((Map<?, ?>) lines.get(0)).get("qty")).as("the Long in a map inside a list").isInstanceOf(Integer.class).isEqualTo(9)
        );
    }

    private static CloudEvent deliveredEvent() {
        Map<String, Object> data = deliveredMap();
        return CloudEventBuilder.v1().withId("held-1").withSource(URI.create("urn:test")).withType("test").withSubject(ORDER)
                .withExtension("intext", 7)
                .withExtension("longext", 8L)
                .withExtension("doubleext", 2.5d)
                .withExtension("integraldoubleext", 42.0d)
                .withDataContentType("application/json")
                .withData(PojoCloudEventData.wrap(data, value -> new Document(data).toJson().getBytes(UTF_8)))
                .build();
    }

    private static Map<String, Object> deliveredMap() {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("qty", 9L);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("int", 3);
        data.put("long", 5L);
        data.put("double", 7.0d);
        data.put("decimal", DECIMAL);
        data.put("date", DATE);
        data.put("raw", RAW);
        data.put("lines", List.of(line));
        return data;
    }

    private void record(CloudEvent event) {
        SagaEnvelope<String> envelope = new SagaEnvelope<>(ORDER, "placed", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
        assertThat(SagaEnvelopes.of(stateStore, null).compareAndSave(ORDER, envelope, 0)).isTrue();
        StoredSaga instance = stateStore.find(ORDER).orElseThrow();
        assertThat(stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event))).as("the record is written").isTrue();
    }

    // Runs the command of the downgrade guide inside the container, against this test's own database
    private List<Document> export(boolean canonical) throws IOException, InterruptedException {
        String uri = "mongodb://localhost:27017/" + databaseName + "?directConnection=true";
        List<String> command = new ArrayList<>(List.of("mongoexport", "--uri", uri, "--collection", SKIPPED_INPUTS));
        if (canonical) {
            command.add("--jsonFormat=canonical");
        }
        command.addAll(List.of("--sort", "{\"sagaId\": 1, \"quarantineId\": 1, \"seq\": 1, \"key\": 1}"));
        ExecResult result = mongoDBContainer.execInContainer(command.toArray(String[]::new));
        assertThat(result.getExitCode()).as("mongoexport exit code, stderr: " + result.getStderr()).isZero();
        return result.getStdout().lines().filter(line -> !line.isBlank()).map(Document::parse).toList();
    }

    // Rebuilds the event the way the downgrade guide describes, from the exported record alone
    private static CloudEvent rebuild(Document record) {
        CloudEvent attributes = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).deserialize(record.getString("event").getBytes(UTF_8));
        CloudEventBuilder builder = CloudEventBuilder.v1(attributes);
        Document extensions = record.get("extensions", Document.class);
        if (extensions != null) {
            extensions.forEach((name, value) -> {
                switch (value) {
                    case Integer number -> builder.withExtension(name, number);
                    case Long number -> builder.withExtension(name, number);
                    case Double number -> builder.withExtension(name, number);
                    default -> throw new AssertionError("Unexpected extension '" + name + "' of " + value.getClass());
                }
            });
        }
        Object data = record.get("data");
        Document dataDocument = record.get("dataDocument", Document.class);
        if (data != null) {
            byte[] bytes = data instanceof Binary binary ? binary.getData() : (byte[]) data;
            builder.withData(bytes);
        } else if (dataDocument != null) {
            byte[] bytes = dataDocument.toJson().getBytes(UTF_8);
            CloudEventData wrapped = PojoCloudEventData.wrap(dataDocument, value -> bytes);
            builder.withData(wrapped);
        }
        return builder.build();
    }

    private static Executable extension(CloudEvent event, String name, Object expected) {
        return () -> {
            Object actual = event.getExtension(name);
            assertThat(actual).as("extension '" + name + "'").isNotNull();
            assertThat(requireNonNull(actual).getClass()).as("class of extension '" + name + "'").isEqualTo(expected.getClass());
            assertThat(actual).as("value of extension '" + name + "'").isEqualTo(expected);
        };
    }

    private static Executable leaf(Map<?, ?> data, String name, Object expected) {
        return () -> {
            Object actual = data.get(name);
            assertThat(actual).as(name).isNotNull();
            assertThat(actual.getClass()).as(name + " class").isEqualTo(expected.getClass());
            assertThat(actual).as(name).isEqualTo(expected);
        };
    }
}
