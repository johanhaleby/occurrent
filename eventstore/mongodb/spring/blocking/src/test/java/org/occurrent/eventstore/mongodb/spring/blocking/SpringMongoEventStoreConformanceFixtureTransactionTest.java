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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

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
class SpringMongoEventStoreConformanceFixtureTransactionTest {

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
        SpringMongoEventStoreConformanceFixture fixture = SpringMongoEventStoreConformanceFixture.listeningToCommands(connectionString(), recorder(recorded), MAJORITY_READS);
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
    void the_fixtures_conditional_dcb_append_writes_its_events_inside_the_managers_committed_transaction() {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        List<BsonDocument> commands;
        SpringMongoEventStoreConformanceFixture fixture = SpringMongoEventStoreConformanceFixture.listeningToCommands(connectionString(), recorder(recorded), MAJORITY_READS);
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
        BsonDocument first = commands.stream().filter(command -> command.containsKey("autocommit")).findFirst().orElseThrow();
        assertThat(first.getDocument("readConcern", new BsonDocument()).getString("level", new BsonString("none")).getValue())
                .as("the read concern of the first command of the append's transaction, which the fixture's transaction manager was configured with")
                .isEqualTo("majority");
        List<BsonDocument> writes = commands.stream()
                .filter(command -> Set.of("insert", "update").contains(command.getFirstKey()) && EVENTS.equals(command.getString(command.getFirstKey()).getValue()))
                .toList();
        assertThat(writes).as("the writes of the append to the event collection").isNotEmpty();
        assertThat(writes).as("the writes of the append to the event collection, which a transaction tags with a transaction number and no autocommit")
                .allSatisfy(write -> {
                    assertThat(write.containsKey("txnNumber")).as("txnNumber of %s", write.getFirstKey()).isTrue();
                    assertThat(write.getBoolean("autocommit", null)).as("autocommit of %s", write.getFirstKey()).isEqualTo(BsonBoolean.FALSE);
                });
        int lastWrite = commands.lastIndexOf(writes.get(writes.size() - 1));
        assertThat(commands.subList(lastWrite + 1, commands.size())).as("the commands after the last write of the append")
                .extracting(BsonDocument::getFirstKey).contains("commitTransaction");
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
