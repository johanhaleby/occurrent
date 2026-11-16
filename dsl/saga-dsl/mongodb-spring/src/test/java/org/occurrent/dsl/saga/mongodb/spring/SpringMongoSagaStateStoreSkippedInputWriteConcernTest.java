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
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A skip is acknowledged once its record is written, so a record that a failover rolls back leaves an
 * input that no release replays. Every write and delete on the skipped-inputs collection is therefore sent with a
 * journaled majority, whatever write concern the {@code MongoOperations} has, while the writes to the saga instance
 * collection keep the caller's.
 * <p>
 * The container is a single-node replica set, where any write concern is acknowledged at once, so a stronger one does
 * not behave differently here. What differs is the {@code writeConcern} the driver puts on the {@code update} and
 * {@code delete} commands, which a command listener reads off the wire. The client and the template here both use
 * {@code w: 1}, so the collection the store is handed has it too.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreSkippedInputWriteConcernTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-skipped-input-write-concern";
    private static final String SKIPPED_INPUTS_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";
    private static final BsonDocument W1_ON_THE_WIRE = new BsonDocument("w", new BsonInt32(1));
    private static final BsonDocument MAJORITY_ON_THE_WIRE = new BsonDocument("w", new BsonString("majority")).append("j", BsonBoolean.TRUE).append("wtimeout", new BsonInt32(10_000));
    private static final Set<String> WRITE_COMMANDS = Set.of("insert", "update", "delete", "findAndModify");

    record Payment(long amount) {
    }

    // One entry per write command the server was sent, as the collection it targets and the write concern it carries
    record SentWrite(String command, String collection, BsonDocument writeConcern) {
    }

    private SpringMongoSagaStateStore<Payment> store;
    private final List<SentWrite> sentWrites = new CopyOnWriteArrayList<>();

    @BeforeEach
    void createTheStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-write-concern-" + UUID.randomUUID().toString().substring(0, 8)));
        String database = requireNonNull(connectionString.getDatabase());
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .writeConcern(WriteConcern.W1)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if (WRITE_COMMANDS.contains(event.getCommandName())) {
                            BsonDocument command = event.getCommand();
                            sentWrites.add(new SentWrite(event.getCommandName(), command.getString(event.getCommandName()).getValue(), command.getDocument("writeConcern", new BsonDocument())));
                        }
                    }
                }).build();
        MongoTemplate template = new MongoTemplate(MongoClients.create(settings), database);
        template.setWriteConcern(WriteConcern.W1);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class);
    }

    private static SagaEnvelope<Payment> envelope(String sagaId, SagaStatus status, SagaFailure failure) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), status, 1, List.<TimerEntry>of(), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static SagaFailure failure() {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
    }

    private List<SentWrite> writesSentBy(Runnable operation) {
        sentWrites.clear();
        operation.run();
        return List.copyOf(sentWrites);
    }

    private static List<SentWrite> to(List<SentWrite> writes, String collection) {
        return writes.stream().filter(write -> write.collection().equals(collection)).toList();
    }

    private static void assertSentWithAJournaledMajority(List<SentWrite> writes, String command) {
        assertAll(
                () -> assertThat(writes).as("%s commands sent to the skipped-inputs collection", command).extracting(SentWrite::command).containsExactly(command),
                () -> assertThat(writes).extracting(SentWrite::writeConcern).containsExactly(MAJORITY_ON_THE_WIRE)
        );
    }

    @Test
    void recordSkippedInput_is_sent_with_a_journaled_majority_whatever_the_write_concern_of_the_template() {
        List<SentWrite> sent = writesSentBy(() -> store.recordSkippedInput("order-1", QUARANTINE_ID, new SkippedInput(7L, "order-1@7")));

        assertSentWithAJournaledMajority(to(sent, SKIPPED_INPUTS_COLLECTION), "update");
    }

    @Test
    void recordSkippedInputAtHead_is_sent_with_a_journaled_majority_whatever_the_write_concern_of_the_template() {
        List<SentWrite> sent = writesSentBy(() -> store.recordSkippedInputAtHead("order-1", QUARANTINE_ID, new SkippedInput(7L, "order-1@7")));

        assertSentWithAJournaledMajority(to(sent, SKIPPED_INPUTS_COLLECTION), "update");
    }

    @Test
    void deleteSkippedInput_is_sent_with_a_journaled_majority_whatever_the_write_concern_of_the_template() {
        SkippedInput input = new SkippedInput(7L, "order-1@7");
        store.recordSkippedInput("order-1", QUARANTINE_ID, input);

        List<SentWrite> sent = writesSentBy(() -> store.deleteSkippedInput("order-1", QUARANTINE_ID, input));

        assertSentWithAJournaledMajority(to(sent, SKIPPED_INPUTS_COLLECTION), "delete");
    }

    @Test
    void deleteSkippedInputs_is_sent_with_a_journaled_majority_whatever_the_write_concern_of_the_template() {
        store.recordSkippedInput("order-1", QUARANTINE_ID, new SkippedInput(7L, "order-1@7"));

        List<SentWrite> sent = writesSentBy(() -> store.deleteSkippedInputs("order-1", QUARANTINE_ID));

        assertSentWithAJournaledMajority(to(sent, SKIPPED_INPUTS_COLLECTION), "delete");
    }

    @Test
    void deleting_a_quarantined_instance_sends_the_delete_of_its_skipped_inputs_with_a_journaled_majority() {
        store.compareAndSave("order-1", envelope("order-1", SagaStatus.QUARANTINED, failure()), 0);
        store.recordSkippedInput("order-1", QUARANTINE_ID, new SkippedInput(7L, "order-1@7"));

        List<SentWrite> sent = writesSentBy(() -> store.delete("order-1"));

        assertSentWithAJournaledMajority(to(sent, SKIPPED_INPUTS_COLLECTION), "delete");
    }

    @Test
    void a_write_to_the_saga_instance_collection_keeps_the_write_concern_of_the_template() {
        List<SentWrite> sent = writesSentBy(() -> store.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, null), 0));

        List<SentWrite> instanceWrites = to(sent, COLLECTION);
        assertAll(
                () -> assertThat(instanceWrites).as("writes sent to the saga instance collection").isNotEmpty(),
                () -> assertThat(instanceWrites).extracting(SentWrite::writeConcern).containsOnly(W1_ON_THE_WIRE),
                () -> assertThat(to(sent, SKIPPED_INPUTS_COLLECTION)).as("writes sent to the skipped-inputs collection").isEmpty()
        );
    }
}
