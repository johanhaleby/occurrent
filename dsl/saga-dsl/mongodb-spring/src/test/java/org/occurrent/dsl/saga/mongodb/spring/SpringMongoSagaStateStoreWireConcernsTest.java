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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. The concerns the store asks for are read off the commands it sends, since a single-node replica set
 * answers the same whatever concern a command sends. A read returns only what a majority has, and an instance that a
 * majority read does not find is read again at read concern {@code linearizable}, bounded by {@code maxTimeMS}. The
 * template and the client here write at {@code w: 1}, so a write that sends {@code majority} and {@code j: true} was
 * made to.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreWireConcernsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-wire-concerns";
    private static final String SKIPPED_INPUTS = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";

    record Payment(long amount) {
    }

    record Sent(String command, String collection, @org.jspecify.annotations.Nullable String readConcernLevel, boolean hasMaxTimeMs, BsonDocument writeConcern) {
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private SpringMongoSagaStateStore<Payment> store;
    private SagaEnvelopes<Payment> envelopes;

    @BeforeEach
    void createTheStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-wire-" + UUID.randomUUID().toString().substring(0, 8)));
        String database = requireNonNull(connectionString.getDatabase());
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.W1)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonDocument command = event.getCommand();
                        BsonValue target = command.get(event.getCommandName());
                        BsonDocument readConcern = command.getDocument("readConcern", new BsonDocument());
                        sent.add(new Sent(event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : "",
                                readConcern.containsKey("level") ? readConcern.getString("level").getValue() : null, command.containsKey("maxTimeMS"),
                                BsonDocument.parse(command.getDocument("writeConcern", new BsonDocument()).toJson())));
                    }
                }).build());
        MongoTemplate template = new MongoTemplate(client, database);
        template.setWriteConcern(WriteConcern.W1);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class);
        envelopes = SagaEnvelopes.of(store, null);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<Payment> envelope(String sagaId, SagaStatus status, List<TimerEntry> timers, SagaFailure failure, long version) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), status, version, timers, Map.of("order-1", 6L), null, NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static SagaFailure releasingFailure() {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
    }

    private List<Sent> sentBy(Runnable action) {
        sent.clear();
        action.run();
        return List.copyOf(sent);
    }

    private static List<Sent> finds(List<Sent> sent, String collection) {
        return sent.stream().filter(s -> s.command().equals("find") && s.collection().equals(collection)).toList();
    }

    private static List<String> readConcernLevels(List<Sent> finds) {
        return finds.stream().map(s -> String.valueOf(s.readConcernLevel())).toList();
    }

    @Test
    void find_of_an_absent_instance_reads_again_at_read_concern_linearizable_bounded_by_max_time() {
        List<Sent> sentByTheRead = sentBy(() -> assertThat(store.find("absent")).isEmpty());

        List<Sent> finds = finds(sentByTheRead, COLLECTION);
        assertAll(
                () -> assertThat(readConcernLevels(finds)).as("read concern of each find, in order").containsExactly("majority", "linearizable"),
                () -> assertThat(finds.get(1).hasMaxTimeMs()).as("maxTimeMS of the linearizable find").isTrue()
        );
    }

    @Test
    void find_without_state_of_an_absent_instance_reads_again_at_read_concern_linearizable_bounded_by_max_time() {
        List<Sent> sentByTheRead = sentBy(() -> assertThat(store.findWithoutState("absent")).isEmpty());

        List<Sent> finds = finds(sentByTheRead, COLLECTION);
        assertAll(
                () -> assertThat(readConcernLevels(finds)).as("read concern of each find, in order").containsExactly("majority", "linearizable"),
                () -> assertThat(finds.get(1).hasMaxTimeMs()).as("maxTimeMS of the linearizable find").isTrue()
        );
    }

    @Test
    void find_of_a_present_instance_sends_exactly_one_find_at_read_concern_majority() {
        envelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(), null, 1), 0);

        List<Sent> sentByTheRead = sentBy(() -> assertThat(store.find("order-1")).isPresent());

        assertThat(readConcernLevels(finds(sentByTheRead, COLLECTION))).as("read concern of each find").containsExactly("majority");
    }

    @Test
    void find_without_state_of_a_present_instance_sends_exactly_one_find_at_read_concern_majority() {
        envelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(), null, 1), 0);

        List<Sent> sentByTheRead = sentBy(() -> assertThat(store.findWithoutState("order-1")).isPresent());

        assertThat(readConcernLevels(finds(sentByTheRead, COLLECTION))).as("read concern of each find").containsExactly("majority");
    }

    @Test
    void every_read_of_the_store_sends_read_concern_majority() {
        envelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(new TimerEntry("payment", NOW.toEpochMilli())), null, 1), 0);
        envelopes.compareAndSave("order-2", envelope("order-2", SagaStatus.QUARANTINED, List.of(), releasingFailure(), 1), 0);
        envelopes.compareAndSaveSkipped(store, envelope("order-2", SagaStatus.QUARANTINED, List.of(), releasingFailure(), 2), 1, QUARANTINE_ID, new SkippedInput(0, "order-2@8", heldEvent("order-2@8")));

        List<Sent> found = sentBy(() -> assertThat(store.find("order-1")).isPresent());
        List<Sent> foundWithoutState = sentBy(() -> assertThat(store.findWithoutState("order-1")).isPresent());
        List<Sent> withDueTimers = sentBy(() -> assertThat(store.findWithDueTimers(NOW.plusSeconds(1), 10)).hasSize(1));
        List<Sent> byStatus = sentBy(() -> assertThat(store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(1), 10)).hasSize(1));
        List<Sent> releasing = sentBy(() -> assertThat(store.findReleasingSagaIds(null, 10)).containsExactly("order-2"));
        List<Sent> skippedInputs = sentBy(() -> assertThat(store.findSkippedInputs("order-2", QUARANTINE_ID, 10)).hasSize(1));

        assertAll(
                () -> assertThat(readConcernLevels(finds(found, COLLECTION))).as("find").isNotEmpty().containsOnly("majority"),
                () -> assertThat(readConcernLevels(finds(foundWithoutState, COLLECTION))).as("findWithoutState").isNotEmpty().containsOnly("majority"),
                () -> assertThat(readConcernLevels(finds(withDueTimers, COLLECTION))).as("findWithDueTimers").isNotEmpty().containsOnly("majority"),
                () -> assertThat(readConcernLevels(finds(byStatus, COLLECTION))).as("findByStatus").isNotEmpty().containsOnly("majority"),
                () -> assertThat(readConcernLevels(finds(releasing, COLLECTION))).as("findReleasingSagaIds").isNotEmpty().containsOnly("majority"),
                () -> assertThat(readConcernLevels(finds(skippedInputs, SKIPPED_INPUTS))).as("findSkippedInputs").isNotEmpty().containsOnly("majority")
        );
    }

    @Test
    void the_delete_of_an_instance_that_is_not_quarantined_is_majority_and_journaled_whatever_the_write_concern_of_the_template() {
        envelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(), null, 1), 0);

        List<Sent> sentByTheDelete = sentBy(() -> store.delete("order-1"));

        List<Sent> deletes = sentByTheDelete.stream().filter(s -> s.command().equals("delete") && s.collection().equals(COLLECTION)).toList();
        assertAll(
                () -> assertThat(deletes).as("delete commands on the instances").hasSize(1),
                () -> assertThat(deletes.getFirst().writeConcern().get("w")).as("w").isEqualTo(new BsonString("majority")),
                () -> assertThat(deletes.getFirst().writeConcern().get("j")).as("j").isEqualTo(org.bson.BsonBoolean.TRUE),
                () -> assertThat(store.find("order-1")).as("the instance is gone").isEmpty()
        );
    }

    private static io.cloudevents.CloudEvent heldEvent(String key) {
        return io.cloudevents.core.builder.CloudEventBuilder.v1().withId(key).withSource(java.net.URI.create("urn:test")).withType("test").build();
    }
}
