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
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. The downgrade guide has the operator dump the skipped inputs with {@code mongodump}, restore the dump
 * into a database of its own, read the restored records with a driver and rebuild each event with a recipe. Checks that
 * the recipe, applied to records read from a dump restored with the guide's commands, gives exactly the event the store
 * returns for the same record, down to the class of every extension and of every value inside the data. Also checks
 * that a {@code mongoexport} JSON copy is not a copy to rebuild from, since it cannot tell a delivered map from a
 * delivered number.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class SkippedInputExportTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-export";
    private static final String SKIPPED_INPUTS = COLLECTION + "_skipped_inputs";
    private static final String OTHER_SAGAS_SKIPPED_INPUTS = "saga-other_skipped_inputs";
    private static final String ORDER = "order-1";
    private static final String QUARANTINE_ID = "q-1";
    private static final Instant NOW = Instant.parse("2026-03-04T05:16:17.456Z");
    private static final int LIMIT = 100;

    private static final Decimal128 DECIMAL = new Decimal128(new BigDecimal("1234567890.123456789"));
    private static final Date DATE = new Date(1772601377456L);
    private static final Binary RAW = new Binary(new byte[]{0, 1, 2, (byte) 0xff, (byte) 0x80, 'a'});

    private ConnectionString connectionString;
    private String databaseName;
    private SpringMongoSagaStateStore<String> stateStore;
    private MongoTemplate template;
    private int recorded;

    @BeforeEach
    void createInstances() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-export-" + UUID.randomUUID()));
        databaseName = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(MongoClients.create(connectionString), databaseName);
        stateStore = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
        recorded = 0;
    }

    @Test
    void a_map_whose_bytes_differ_from_its_json_is_rebuilt_with_those_bytes_and_the_map() throws Exception {
        record(mapEvent("held-1", deliveredMap(), "{\"other\":1}".getBytes(UTF_8)));
        record(mapEvent("held-2", deliveredMap(), "{\"line\": 1,  \"other\" : [2, 3]}\n".getBytes(UTF_8)));

        List<Document> restored = assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();

        assertThat(restored).as("both the document and the bytes are held").allSatisfy(record -> assertThat(record).containsKeys("dataDocument", "data"));
    }

    @Test
    void a_map_holding_literal_extended_json_keys_is_rebuilt_as_nested_documents_of_strings() throws Exception {
        record(mapEvent("held-1", Map.of("literal", Map.of("$numberLong", "42")), null));
        record(mapEvent("held-2", Map.of("literal", Map.of("$date", "2026-03-04T05:16:17.456Z")), null));
        record(mapEvent("held-3", Map.of("literal", Map.of("$binary", Map.of("base64", "AAEC/4Bh", "subType", "00"))), null));

        List<Document> restored = assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();

        Document first = restored.get(0).get("dataDocument", Document.class);
        assertThat(first.get("literal")).as("the literal map is held as a document and not as a number").isInstanceOf(Document.class);
    }

    @Test
    void a_map_with_every_kind_of_leaf_is_rebuilt_with_the_class_and_value_delivered() throws Exception {
        record(mapEvent("held-1", deliveredMap(), null));

        assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();
    }

    @Test
    void data_that_is_not_a_map_is_rebuilt_as_its_bytes() throws Exception {
        record(builder("held-1").withData("application/octet-stream", new byte[]{0, 1, 2, (byte) 0xff, (byte) 0x80, (byte) 0xc3, 'a'}).build());
        record(builder("held-2").withData("text/plain", "hello åäö".getBytes(UTF_8)).build());

        assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();
    }

    @Test
    void an_event_without_data_is_rebuilt_without_data() throws Exception {
        record(builder("held-1").build());

        List<Document> restored = assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();

        assertThat(rebuild(restored.get(0)).getData()).isNull();
    }

    @Test
    void every_class_of_extension_is_rebuilt_with_its_class_and_value() throws Exception {
        record(builder("held-1")
                .withExtension("uriext", URI.create("https://example.com/a?b=c"))
                .withExtension("timeext", OffsetDateTime.of(2026, 3, 4, 5, 16, 17, 456_000_000, ZoneOffset.ofHours(2)))
                .withExtension("byteext", (byte) -7)
                .withExtension("shortext", (short) 300)
                .withExtension("floatext", 1.5f)
                .withExtension("bigintext", new BigInteger("123456789012345678901234567890"))
                .withExtension("bigdecext", new BigDecimal("1234567890.123456789"))
                .withExtension("stringext", "text")
                .withExtension("intext", 7)
                .withExtension("longext", 8L)
                .withExtension("doubleext", 2.5d)
                .withExtension("integraldoubleext", 42.0d)
                .withExtension("boolext", true)
                .withExtension("bytesext", new byte[]{0, 1, (byte) 0xff})
                .build());

        List<Document> restored = assertEveryRestoredRecordRebuildsToWhatTheStoreReturns();

        assertThat(rebuild(restored.get(0)).getExtensionNames()).hasSize(14);
    }

    @Test
    void a_mongoexport_json_copy_rebuilds_a_literal_number_long_map_as_a_Long_and_so_is_not_a_copy_to_rebuild_from() throws Exception {
        record(mapEvent("held-1", Map.of("literal", Map.of("$numberLong", "42")), null));

        List<Document> exported = export("--jsonFormat=canonical");
        CloudEvent fromExport = rebuild(exported.get(0));
        CloudEvent fromStore = stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, LIMIT).get(0).event();

        Map<?, ?> exportedData = (Map<?, ?>) ((PojoCloudEventData<?>) requireNonNull(fromExport.getData())).getValue();
        Map<?, ?> storedData = (Map<?, ?>) ((PojoCloudEventData<?>) requireNonNull(fromStore.getData())).getValue();
        assertThat(storedData.get("literal")).as("what the store returns").isInstanceOf(Document.class).isEqualTo(new Document("$numberLong", "42"));
        assertThat(exportedData.get("literal")).as("what the export rebuilds").isInstanceOf(Long.class).isEqualTo(42L);
        assertThat(differences(fromExport, fromStore)).as("differences between the export's event and the store's").isNotEmpty();
    }

    @Test
    void an_extension_the_driver_reads_as_Binary_is_rebuilt_from_its_bytes() throws Exception {
        record(CloudEventBuilder.from(mapEvent("held-1", deliveredMap(), null)).withExtension("bytesext", new byte[]{0, 1, (byte) 0xff}).build());
        String restoredDatabase = restore(dump(SKIPPED_INPUTS));
        rewriteBinaryAsUserDefinedSubtype(restoredDatabase, true, false);

        List<Document> restored = readRestored(restoredDatabase);

        assertThat(restored.get(0).get("extensions", Document.class).get("bytesext")).as("the driver reads the extension as").isInstanceOf(Binary.class);
        assertThat(differences(rebuild(restored.get(0)), stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, LIMIT).get(0).event())).as("differences between the rebuilt event and the store's").isEmpty();
    }

    @Test
    void data_the_driver_reads_as_Binary_is_rebuilt_from_its_bytes() throws Exception {
        record(CloudEventBuilder.from(mapEvent("held-1", deliveredMap(), "{\"other\":1}".getBytes(UTF_8))).build());
        record(builder("held-2").withData("application/octet-stream", new byte[]{0, 1, (byte) 0xff, (byte) 0x80}).build());
        String restoredDatabase = restore(dump(SKIPPED_INPUTS));
        rewriteBinaryAsUserDefinedSubtype(restoredDatabase, false, true);

        List<Document> restored = readRestored(restoredDatabase);
        List<SkippedInput> fromStore = stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, LIMIT);

        assertThat(restored).as("records in the restored collection").hasSize(2);
        for (int i = 0; i < restored.size(); i++) {
            assertThat(restored.get(i).get("data")).as("the driver reads data of record " + i + " as").isInstanceOf(Binary.class);
            assertThat(differences(rebuild(restored.get(i)), fromStore.get(i).event())).as("differences between the rebuilt event and the store's, record " + i).isEmpty();
        }
    }

    @Test
    void only_the_named_collection_is_restored_when_the_dump_directory_holds_another_sagas_collection_too() throws Exception {
        record(mapEvent("held-1", deliveredMap(), null));
        template.getCollection(OTHER_SAGAS_SKIPPED_INPUTS).insertOne(new Document("sagaId", "other-1").append("quarantineId", "q-9").append("seq", 0L).append("key", "other"));
        String directory = dump(SKIPPED_INPUTS, OTHER_SAGAS_SKIPPED_INPUTS);
        template.getCollection(OTHER_SAGAS_SKIPPED_INPUTS).deleteMany(new Document());

        String restoredDatabase = restore(directory);

        assertThat(template.getCollection(OTHER_SAGAS_SKIPPED_INPUTS).countDocuments()).as("records written back into the other saga's collection in the source database").isZero();
        assertThat(collectionNames(restoredDatabase)).as("collections in the restored database").containsExactly(SKIPPED_INPUTS);
        assertThat(readRestored(restoredDatabase)).as("records restored").hasSize(1);
    }

    @Test
    void a_uuid_in_the_data_is_read_as_a_UUID_by_a_driver_with_the_application_clients_representation_and_as_Binary_by_one_with_the_default() {
        UUID uuid = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");
        MongoClientSettings standard = MongoClientSettings.builder().applyConnectionString(connectionString).uuidRepresentation(UuidRepresentation.STANDARD).build();
        try (MongoClient applicationClient = MongoClients.create(standard)) {
            SpringMongoSagaStateStore<String> store = new SpringMongoSagaStateStore<>(new MongoTemplate(applicationClient, databaseName), COLLECTION, String.class);
            SagaEnvelope<String> envelope = new SagaEnvelope<>(ORDER, "placed", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
            assertThat(SagaEnvelopes.of(store, null).compareAndSave(ORDER, envelope, 0)).isTrue();
            StoredSaga instance = store.find(ORDER).orElseThrow();
            CloudEvent event = mapEvent("held-1", Map.of("id", uuid), ("{\"id\":\"" + uuid + "\"}").getBytes(UTF_8));
            assertThat(store.compareAndSaveSkipped(instance, instance.version(), QUARANTINE_ID, new SkippedInput(0, "key-0", event))).as("the record with a UUID in its data is written").isTrue();
        }

        Object readWithTheSameRepresentation = firstRecordsDataValue(MongoClientSettings.builder().applyConnectionString(connectionString).uuidRepresentation(UuidRepresentation.STANDARD).build(), "id");
        Object readWithTheDefaultRepresentation = firstRecordsDataValue(MongoClientSettings.builder().applyConnectionString(connectionString).build(), "id");

        assertThat(readWithTheSameRepresentation).as("read with the application client's representation").isEqualTo(uuid);
        assertThat(readWithTheDefaultRepresentation).as("read with the default representation").isInstanceOf(Binary.class);
    }

    private Object firstRecordsDataValue(MongoClientSettings settings, String key) {
        try (MongoClient client = MongoClients.create(settings)) {
            Document record = requireNonNull(client.getDatabase(databaseName).getCollection(SKIPPED_INPUTS, Document.class).find().first());
            return record.get("dataDocument", Document.class).get(key);
        }
    }

    private CloudEventBuilder builder(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test").withSubject(ORDER)
                .withTime(OffsetDateTime.of(2026, 3, 4, 5, 16, 17, 456_000_000, ZoneOffset.UTC))
                .withDataSchema(URI.create("urn:schema"));
    }

    private CloudEvent mapEvent(String id, Map<String, Object> data, byte[] bytes) {
        return builder(id).withDataContentType("application/json")
                .withData(PojoCloudEventData.wrap(data, value -> bytes == null ? new Document(data).toJson().getBytes(UTF_8) : bytes))
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

    // Records the event as the next held input of the quarantine, so a test can hold several
    private void record(CloudEvent event) {
        if (recorded == 0) {
            SagaEnvelope<String> envelope = new SagaEnvelope<>(ORDER, "placed", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
            assertThat(SagaEnvelopes.of(stateStore, null).compareAndSave(ORDER, envelope, 0)).isTrue();
        }
        StoredSaga instance = stateStore.find(ORDER).orElseThrow();
        assertThat(stateStore.compareAndSaveSkipped(instance, instance.version(), QUARANTINE_ID, new SkippedInput(recorded, "key-" + recorded, event))).as("record " + recorded + " is written").isTrue();
        recorded++;
    }

    private List<Document> assertEveryRestoredRecordRebuildsToWhatTheStoreReturns() throws Exception {
        List<Document> restored = restoredRecords();
        List<SkippedInput> fromStore = stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, LIMIT);

        assertThat(restored).as("records in the restored collection").hasSize(recorded);
        assertThat(fromStore).as("inputs the store returns").hasSize(recorded);
        for (int i = 0; i < recorded; i++) {
            Document record = restored.get(i);
            SkippedInput expected = fromStore.get(i);
            assertThat(((Number) record.get("seq")).longValue()).as("seq of record " + i).isEqualTo(expected.seq());
            assertThat(record.getString("key")).as("key of record " + i).isEqualTo(expected.key());
            assertThat(differences(rebuild(record), expected.event())).as("differences between the rebuilt event and the store's, record " + i).isEmpty();
        }
        return restored;
    }

    // Runs the guide's dump and restore inside the container against this test's own database, and reads the restored
    // records with a driver
    private List<Document> restoredRecords() throws IOException, InterruptedException {
        return readRestored(restore(dump(SKIPPED_INPUTS)));
    }

    // The guide's mongodump, run for each saga's collection into the one directory. Returns the directory.
    private String dump(String... collections) throws IOException, InterruptedException {
        String directory = "/tmp/" + UUID.randomUUID();
        for (String collection : collections) {
            ExecResult dump = mongoDBContainer.execInContainer("mongodump", "--uri", "mongodb://localhost:27017/" + databaseName + "?directConnection=true",
                    "--collection", collection, "--out", directory);
            assertThat(dump.getExitCode()).as("mongodump exit code for " + collection + ", stderr: " + dump.getStderr()).isZero();
            ExecResult listing = mongoDBContainer.execInContainer("ls", directory + "/" + databaseName + "/" + collection + ".bson");
            assertThat(listing.getExitCode()).as("the dump holds the bson file of " + collection + ", stderr: " + listing.getStderr()).isZero();
        }
        return directory;
    }

    // The guide's mongorestore of this saga's skipped inputs collection, from a connection string that names no
    // database. Returns the name of the database restored into.
    private String restore(String directory) throws IOException, InterruptedException {
        String restoredDatabase = requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl("restored-" + UUID.randomUUID().toString().substring(0, 8))).getDatabase());
        assertThat(restoredDatabase.getBytes(UTF_8).length).as("restored database name fits MongoDB's limit").isLessThan(64);

        ExecResult restore = mongoDBContainer.execInContainer("mongorestore", "--uri", "mongodb://localhost:27017/?directConnection=true",
                "--nsInclude", databaseName + "." + SKIPPED_INPUTS,
                "--nsFrom", databaseName + "." + SKIPPED_INPUTS, "--nsTo", restoredDatabase + "." + SKIPPED_INPUTS, directory);
        assertThat(restore.getExitCode()).as("mongorestore exit code, stderr: " + restore.getStderr()).isZero();
        return restoredDatabase;
    }

    private List<Document> readRestored(String restoredDatabase) {
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(restoredDatabase))) {
            return client.getDatabase(restoredDatabase).getCollection(SKIPPED_INPUTS, Document.class).find()
                    .sort(new Document("sagaId", 1).append("quarantineId", 1).append("seq", 1).append("key", 1)).into(new ArrayList<>());
        }
    }

    private static byte[] bytesOf(Object binary) {
        return binary instanceof Binary value ? value.getData() : (byte[]) binary;
    }

    private List<String> collectionNames(String database) {
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(database))) {
            return client.getDatabase(database).listCollectionNames().into(new ArrayList<>());
        }
    }

    // Writes the extension and the data of every restored record back as binary of a user defined subtype, which the
    // driver reads as an org.bson.types.Binary, where it reads subtype 0 as a byte[]
    private void rewriteBinaryAsUserDefinedSubtype(String restoredDatabase, boolean extension, boolean data) {
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(restoredDatabase))) {
            var collection = client.getDatabase(restoredDatabase).getCollection(SKIPPED_INPUTS, Document.class);
            for (Document record : collection.find()) {
                Document set = new Document();
                if (extension) {
                    set.append("extensions.bytesext", new Binary(BsonBinarySubType.USER_DEFINED, bytesOf(record.get("extensions", Document.class).get("bytesext"))));
                }
                if (data) {
                    set.append("data", new Binary(BsonBinarySubType.USER_DEFINED, bytesOf(record.get("data"))));
                }
                assertThat(collection.updateOne(new Document("_id", record.get("_id")), new Document("$set", set)).getModifiedCount()).as("record rewritten").isEqualTo(1);
            }
        }
    }

    // What the guide warns against, a JSON copy of the records read with Document.parse
    private List<Document> export(String jsonFormat) throws IOException, InterruptedException {
        String uri = "mongodb://localhost:27017/" + databaseName + "?directConnection=true";
        ExecResult result = mongoDBContainer.execInContainer("mongoexport", "--uri", uri, "--collection", SKIPPED_INPUTS, jsonFormat,
                "--sort", "{\"sagaId\": 1, \"quarantineId\": 1, \"seq\": 1, \"key\": 1}");
        assertThat(result.getExitCode()).as("mongoexport exit code, stderr: " + result.getStderr()).isZero();
        return result.getStdout().lines().filter(line -> !line.isBlank()).map(Document::parse).toList();
    }

    // SpringMongoSagaStateStore.toSkippedInput as the downgrade in the upgrade guide and the runbook writes it down, so a
    // change to one of the three is made in all of them
    private static CloudEvent rebuild(Document record) {
        CloudEventBuilder builder = CloudEventBuilder.from(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).deserialize(record.getString("event").getBytes(UTF_8)));
        Document extensions = record.get("extensions", Document.class);
        if (extensions != null) {
            extensions.forEach((name, value) -> {
                switch (value) {
                    case String string -> builder.withExtension(name, string);
                    case Integer number -> builder.withExtension(name, number);
                    case Long number -> builder.withExtension(name, number);
                    case Double number -> builder.withExtension(name, number);
                    case Boolean bool -> builder.withExtension(name, bool);
                    // The recipe: binary data is its bytes, read as a byte[] or an org.bson.types.Binary depending on the subtype
                    case Binary binary -> builder.withExtension(name, binary.getData());
                    case byte[] bytes -> builder.withExtension(name, bytes);
                    case Document tagged -> {
                        String tag = tagged.getString("value");
                        switch (tagged.getString("type")) {
                            case "uri" -> builder.withExtension(name, URI.create(tag));
                            case "timestamp" -> builder.withExtension(name, OffsetDateTime.parse(tag));
                            case "byte" -> builder.withExtension(name, Byte.valueOf(tag));
                            case "short" -> builder.withExtension(name, Short.valueOf(tag));
                            case "float" -> builder.withExtension(name, Float.valueOf(tag));
                            case "bigInteger" -> builder.withExtension(name, new BigInteger(tag));
                            case "bigDecimal" -> builder.withExtension(name, new BigDecimal(tag));
                            default -> throw new IllegalStateException("Unknown extension type for '" + name + "': " + tagged.getString("type"));
                        }
                    }
                    default -> throw new IllegalStateException("Unknown extension value for '" + name + "': " + value.getClass());
                }
            });
        }
        Object data = record.get("data");
        // The recipe: `data` is read as bytes the way binary data in an extension is
        byte[] bytes = data == null ? null : data instanceof Binary binary ? binary.getData() : (byte[]) data;
        Document dataDocument = record.get("dataDocument", Document.class);
        if (dataDocument != null) {
            byte[] delivered = bytes == null ? dataDocument.toJson().getBytes(UTF_8) : bytes;
            builder.withData(PojoCloudEventData.wrap(dataDocument, value -> delivered));
        } else if (bytes != null) {
            builder.withData(bytes);
        }
        return builder.build();
    }

    // Compares every attribute, every extension with its class, the data's class and bytes, and a wrapped value deeply
    // with the class of every map, list and value in it, which CloudEvent.equals does not
    private static List<String> differences(CloudEvent actual, CloudEvent expected) {
        List<String> differences = new ArrayList<>();
        compare(differences, "specversion", actual.getSpecVersion(), expected.getSpecVersion());
        compare(differences, "id", actual.getId(), expected.getId());
        compare(differences, "source", actual.getSource(), expected.getSource());
        compare(differences, "type", actual.getType(), expected.getType());
        compare(differences, "subject", actual.getSubject(), expected.getSubject());
        compare(differences, "time", actual.getTime(), expected.getTime());
        compare(differences, "datacontenttype", actual.getDataContentType(), expected.getDataContentType());
        compare(differences, "dataschema", actual.getDataSchema(), expected.getDataSchema());
        compare(differences, "extension names", actual.getExtensionNames(), expected.getExtensionNames());
        for (String name : expected.getExtensionNames()) {
            deepCompare(differences, "extension " + name, actual.getExtension(name), expected.getExtension(name));
        }
        CloudEventData actualData = actual.getData();
        CloudEventData expectedData = expected.getData();
        if (actualData == null || expectedData == null) {
            compare(differences, "data presence", actualData == null, expectedData == null);
        } else {
            compare(differences, "data class", actualData.getClass(), expectedData.getClass());
            compare(differences, "data bytes", Arrays.toString(actualData.toBytes()), Arrays.toString(expectedData.toBytes()));
            if (actualData instanceof PojoCloudEventData<?> actualPojo && expectedData instanceof PojoCloudEventData<?> expectedPojo) {
                deepCompare(differences, "data value", actualPojo.getValue(), expectedPojo.getValue());
            }
        }
        return differences;
    }

    private static void compare(List<String> differences, String what, Object actual, Object expected) {
        if (!Objects.equals(actual, expected)) {
            differences.add(what + ": " + actual + " but expected " + expected);
        }
    }

    private static void deepCompare(List<String> differences, String path, Object actual, Object expected) {
        if (actual == null || expected == null) {
            if (actual != expected) {
                differences.add(path + ": " + actual + " but expected " + expected);
            }
        } else if (actual.getClass() != expected.getClass()) {
            differences.add(path + ": class " + actual.getClass().getName() + " but expected " + expected.getClass().getName());
        } else if (expected instanceof Map<?, ?> map) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            compare(differences, path + " keys", new ArrayList<>(actualMap.keySet()), new ArrayList<>(map.keySet()));
            map.forEach((key, value) -> deepCompare(differences, path + "." + key, actualMap.get(key), value));
        } else if (expected instanceof List<?> list) {
            List<?> actualList = (List<?>) actual;
            compare(differences, path + " size", actualList.size(), list.size());
            for (int i = 0; i < Math.min(list.size(), actualList.size()); i++) {
                deepCompare(differences, path + "[" + i + "]", actualList.get(i), list.get(i));
            }
        } else if (expected instanceof byte[] bytes) {
            compare(differences, path, Arrays.toString((byte[]) actual), Arrays.toString(bytes));
        } else {
            compare(differences, path, actual, expected);
        }
    }
}
