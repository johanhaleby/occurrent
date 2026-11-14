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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * The conformance suites append through the fixture, so they only exercise the fixture's transaction manager if its store
 * writes inside the transaction that manager opened.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreConformanceFixtureTransactionTest {

    private static final String EVENTS = "events";
    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_K1 = "concert:k1";
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));

    private static final TransactionOptions MAJORITY_READS = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build();

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void the_fixtures_stream_append_writes_its_events_inside_the_managers_committed_transaction() {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        List<BsonDocument> commands;
        ReactorMongoEventStoreConformanceFixture fixture = ReactorMongoEventStoreConformanceFixture.listeningToCommands(connectionString(), recorder(recorded), MAJORITY_READS);
        try {
            recorded.clear();

            fixture.eventStore().write("stream-" + UUID.randomUUID(), List.of(event(UUID.randomUUID().toString(), TICKET_SOLD)));

            commands = new ArrayList<>(recorded);
        } finally {
            fixture.close();
        }

        assertWritesRanInACommittedTransaction(commands);
    }

    @Test
    void the_reactive_fixtures_stream_append_writes_its_events_inside_the_managers_committed_transaction() {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        List<BsonDocument> commands;
        ReactorMongoReactiveConformanceFixture fixture = ReactorMongoReactiveConformanceFixture.listeningToCommands(connectionString(), recorder(recorded), MAJORITY_READS);
        try {
            recorded.clear();

            fixture.eventStore().write("stream-" + UUID.randomUUID(), Flux.just(event(UUID.randomUUID().toString(), TICKET_SOLD))).block();

            commands = new ArrayList<>(recorded);
        } finally {
            fixture.close();
        }

        assertWritesRanInACommittedTransaction(commands);
    }

    @Test
    void the_fixtures_conditional_dcb_append_writes_its_events_inside_the_managers_committed_transaction() {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        List<BsonDocument> commands;
        ReactorMongoEventStoreConformanceFixture fixture = ReactorMongoEventStoreConformanceFixture.listeningToCommands(connectionString(), recorder(recorded), MAJORITY_READS);
        try {
            DcbConsistencyToken token = fixture.dcbEventStore().read(TICKETS_OF_K1).consistencyToken();
            recorded.clear();

            fixture.dcbEventStore().append(List.of(taggedEventWithId(UUID.randomUUID().toString(), TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));

            commands = new ArrayList<>(recorded);
        } finally {
            fixture.close();
        }

        assertWritesRanInACommittedTransaction(commands);
    }

    private static void assertWritesRanInACommittedTransaction(List<BsonDocument> commands) {
        List<Integer> writeIndexes = IntStream.range(0, commands.size()).filter(i -> isWriteToEvents(commands.get(i))).boxed().toList();
        List<BsonDocument> writes = writeIndexes.stream().map(commands::get).toList();
        assertThat(writes).as("the writes of the append to the event collection").isNotEmpty();
        assertThat(writes).as("the writes of the append to the event collection, which a transaction tags with a transaction number and no autocommit")
                .allSatisfy(write -> {
                    assertThat(write.containsKey("txnNumber")).as("txnNumber of %s", write.getFirstKey()).isTrue();
                    assertThat(write.getBoolean("autocommit", null)).as("autocommit of %s", write.getFirstKey()).isEqualTo(BsonBoolean.FALSE);
                });
        assertThat(writes.stream().map(ReactorMongoEventStoreConformanceFixtureTransactionTest::transactionOf).distinct())
                .as("the transactions the writes of the append ran in, of which there should be one").hasSize(1);
        BsonDocument transaction = transactionOf(writes.get(0));

        Optional<BsonDocument> start = commands.stream()
                .filter(command -> transaction.equals(transactionOf(command)) && command.getBoolean("startTransaction", BsonBoolean.FALSE).getValue())
                .findFirst();
        assertThat(start).as("the command that started the transaction the writes of the append ran in").isPresent();
        assertThat(start.orElseThrow().getDocument("readConcern", new BsonDocument()).getString("level", new BsonString("none")).getValue())
                .as("the read concern of the first command of the append's transaction, which the fixture's transaction manager was configured with")
                .isEqualTo("majority");

        int lastWrite = writeIndexes.get(writeIndexes.size() - 1);
        OptionalInt commit = IntStream.range(lastWrite + 1, commands.size())
                .filter(i -> "commitTransaction".equals(commands.get(i).getFirstKey()) && transaction.equals(transactionOf(commands.get(i))))
                .findFirst();
        assertThat(commit).as("the commit of the append's transaction after its last write").isPresent();
    }

    private static boolean isWriteToEvents(BsonDocument command) {
        return Set.of("insert", "update").contains(command.getFirstKey()) && EVENTS.equals(command.getString(command.getFirstKey()).getValue());
    }

    private static BsonDocument transactionOf(BsonDocument command) {
        return new BsonDocument("lsid", command.getDocument("lsid", new BsonDocument()).get("id", BsonNull.VALUE))
                .append("txnNumber", command.get("txnNumber", BsonNull.VALUE));
    }

    private static ConnectionString connectionString() {
        return new ConnectionString(mongoDBContainer.getReplicaSetUrl() + "." + EVENTS);
    }

    private static CommandListener recorder(List<BsonDocument> recorded) {
        return new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                recorded.add(event.getCommand().clone());
            }
        };
    }
}
