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
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. The record of a skipped input is checked before the transaction that writes it, and is sent only when
 * the transaction sends its upsert. The record the store writes must be the one that was checked, so a byte array, a
 * map, a list or an extension the caller changes after the check and before the write changes nothing that is stored. The change here is made by a command listener the moment the write of
 * the instance, which comes before the write of the record in the same transaction, is sent.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreDetachedRecordTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-detached";
    private static final String ORDER = "order-1";
    private static final String QUARANTINE_ID = "q-1";
    private static final Instant NOW = Instant.parse("2026-03-04T05:16:17.456Z");

    private final List<String> commands = new CopyOnWriteArrayList<>();
    private final AtomicReference<Runnable> mutation = new AtomicReference<>();
    private final List<String> mutatedBefore = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private SpringMongoSagaStateStore<String> store;

    @BeforeEach
    void createStoreWhoseClientMutatesTheEventWhenTheInstanceIsWritten() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-detached-" + UUID.randomUUID()));
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (!event.getCommandName().equals("update")) {
                    return;
                }
                String collection = event.getCommand().getString("update").getValue();
                commands.add(collection);
                // The write of the instance is the first command of the transition. The record is sent after it
                if (collection.equals(COLLECTION)) {
                    Runnable mutate = mutation.getAndSet(null);
                    if (mutate != null) {
                        mutate.run();
                        mutatedBefore.add(collection);
                    }
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(listener).build();
        client = MongoClients.create(settings);
        MongoTemplate template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
        SagaEnvelope<String> envelope = new SagaEnvelope<>(ORDER, "placed", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(ORDER, envelope, 0)).isTrue();
        commands.clear();
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    void the_bytes_of_BytesCloudEventData_changed_before_the_record_is_sent_are_stored_as_they_were_checked() {
        CloudEvent delivered = event().withDataContentType("application/octet-stream").withData(BytesCloudEventData.wrap(new byte[]{1, 2, 3, 4})).build();
        byte[] theDeliveredArray = requireNonNull(delivered.getData()).toBytes();

        record(delivered, () -> theDeliveredArray[0] = 99);

        assertThat(theDeliveredArray).as("the delivered array was changed in the window").containsExactly(99, 2, 3, 4);
        assertThat(requireNonNull(released().getData()).toBytes()).as("the bytes stored").containsExactly(1, 2, 3, 4);
    }

    @Test
    void a_nested_map_and_a_list_in_a_map_changed_before_the_record_is_sent_are_stored_as_they_were_checked() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("k", "v");
        List<Object> list = new ArrayList<>(List.of("x", "y"));
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("nested", nested);
        map.put("list", list);
        CloudEvent delivered = event().withDataContentType("application/json").withData(PojoCloudEventData.wrap(map, value -> new Document(value).toJson().getBytes(UTF_8))).build();

        record(delivered, () -> {
            nested.put("k", "changed");
            list.set(0, "changed");
        });

        Document stored = (Document) ((PojoCloudEventData<?>) requireNonNull(released().getData())).getValue();
        assertThat(nested).as("the delivered map was changed in the window").containsEntry("k", "changed");
        assertThat(stored.get("nested")).as("the nested map stored").isEqualTo(new Document("k", "v"));
        assertThat(stored.get("list")).as("the list stored").isEqualTo(List.of("x", "y"));
    }

    @Test
    void the_bytes_a_map_that_does_not_round_trip_hands_out_changed_before_the_record_is_sent_are_stored_as_they_were_checked() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("price", new BigDecimal("1.10"));
        byte[] bytes = "{\"price\":1.10}".getBytes(UTF_8);
        CloudEvent delivered = event().withDataContentType("application/json").withData(PojoCloudEventData.wrap(map, value -> bytes)).build();

        record(delivered, () -> bytes[2] = 'X');

        assertThat(new String(bytes, UTF_8)).as("the delivered array was changed in the window").isEqualTo("{\"Xrice\":1.10}");
        assertThat(new String(requireNonNull(released().getData()).toBytes(), UTF_8)).as("the bytes stored").isEqualTo("{\"price\":1.10}");
    }

    @Test
    void a_byte_array_extension_changed_before_the_record_is_sent_is_stored_as_it_was_checked() {
        CloudEvent delivered = event().withExtension("bytesext", new byte[]{1, 2, 3}).build();
        byte[] theDeliveredArray = (byte[]) requireNonNull(delivered.getExtension("bytesext"));

        record(delivered, () -> theDeliveredArray[0] = 99);

        assertThat(theDeliveredArray).as("the delivered array was changed in the window").containsExactly(99, 2, 3);
        assertThat((byte[]) requireNonNull(released().getExtension("bytesext"))).as("the extension stored").containsExactly(1, 2, 3);
    }

    private static CloudEventBuilder event() {
        return CloudEventBuilder.v1().withId("held-1").withSource(URI.create("urn:test")).withType("test").withSubject(ORDER);
    }

    // Records the skip with the change made when the write of the instance is sent, which is after the record was
    // checked and before the record is sent
    private void record(CloudEvent event, Runnable change) {
        mutation.set(change);
        StoredSaga instance = store.find(ORDER).orElseThrow();

        boolean written = store.compareAndSaveSkipped(instance, instance.version(), QUARANTINE_ID, new SkippedInput(0, "key-0", event));

        assertThat(written).as("the record is written").isTrue();
        assertThat(mutatedBefore).as("the change was made when the instance was written").containsExactly(COLLECTION);
        assertThat(commands).as("commands in the order they were sent, the instance and then the record").containsExactly(COLLECTION, COLLECTION + "_skipped_inputs");
    }

    private CloudEvent released() {
        List<SkippedInput> inputs = store.findSkippedInputs(ORDER, QUARANTINE_ID, 10);
        assertThat(inputs).as("records stored").hasSize(1);
        return inputs.getFirst().event();
    }
}
