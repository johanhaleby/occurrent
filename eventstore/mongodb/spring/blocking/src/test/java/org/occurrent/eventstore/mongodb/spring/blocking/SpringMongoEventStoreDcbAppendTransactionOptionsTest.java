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
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * The transaction options the Spring blocking store runs a conditional append with on a replica set. Only a mongos needs
 * the snapshot read concern and the majority write concern that ADR 175 gives the store's own conditional append, so a
 * replica set keeps what the transaction manager was configured with.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbAppendTransactionOptionsTest {

    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_RENAMED = "ConcertRenamed";
    private static final String CONCERT_K1 = "concert:k1";
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));
    private static final int WTIMEOUT_MS = 12_345;

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void keeps_the_configured_read_and_write_concern_for_a_conditional_append_on_a_replica_set() {
        TransactionOptions options = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.W1.withWTimeout(WTIMEOUT_MS, MILLISECONDS).withJournal(true)).build();
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        CommandListener recorder = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                recorded.add(event.getCommand().clone());
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(mongoDBContainer.getReplicaSetUrl())).addCommandListener(recorder).build();
        List<BsonDocument> commands;
        try (MongoClient client = MongoClients.create(settings)) {
            SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(client, "dcb-" + UUID.randomUUID());
            SpringMongoEventStore store = new SpringMongoEventStore(new MongoTemplate(factory), new EventStoreConfig.Builder()
                    .eventStoreCollectionName("events")
                    .transactionConfig(new MongoTransactionManager(factory, options))
                    .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                    .eventStoreCapabilities(STREAM, DCB)
                    .build());
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).consistencyToken();
            store.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));
            recorded.clear();

            store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));

            assertThat(store.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
            commands = new ArrayList<>(recorded);
        }

        BsonDocument first = commands.stream().filter(command -> command.containsKey("autocommit")).findFirst().orElseThrow();
        assertThat(first.getDocument("readConcern").getString("level").getValue()).as("the first command of the transaction of a conditional append on a replica set").isEqualTo("majority");
        List<BsonDocument> commits = commands.stream().filter(command -> "commitTransaction".equals(command.getFirstKey())).toList();
        assertThat(commits).as("the commitTransaction commands of the append").hasSize(1);
        assertThat(commits.get(0).getDocument("writeConcern"))
                .as("the commitTransaction of a conditional append on a replica set, which isn't raised to majority")
                .isEqualTo(BsonDocument.parse("{\"w\": 1, \"wtimeout\": " + WTIMEOUT_MS + ", \"j\": true}"));
    }
}
