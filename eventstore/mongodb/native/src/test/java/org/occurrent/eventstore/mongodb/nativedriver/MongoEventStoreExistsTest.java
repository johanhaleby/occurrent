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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.tck.ConformanceEvents.event;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreExistsTest {

    private static final String COLLECTION = "events";
    private static final String STREAM_ID = "exists-stream";
    private static final int MATCHING_EVENTS = 20;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final List<BsonDocument> commandsSentToEventCollection = new CopyOnWriteArrayList<>();
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoEventStore eventStore;

    @BeforeEach
    void create_event_store_that_records_the_commands_it_sends_to_the_event_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".exists");
        CommandListener recordEventCollectionCommands = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonDocument command = event.getCommand();
                if (command.containsKey(event.getCommandName()) && command.get(event.getCommandName()).equals(new BsonString(COLLECTION))) {
                    commandsSentToEventCollection.add(command.clone());
                }
            }
        };
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(recordEventCollectionCommands).build());
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, database.getName(), COLLECTION, config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void exists_with_dcb_criteria_examines_one_document_when_many_events_match() {
        eventStore.append(IntStream.range(0, MATCHING_EVENTS).mapToObj(__ -> taggedEvent("exists:tag")).toList());
        commandsSentToEventCollection.clear();

        boolean exists = eventStore.exists(DcbCriteria.all());

        assertThat(exists).isTrue();
        assertThat(documentsExaminedBy(theOnlyFindLimitedToOneDocument())).isEqualTo(1);
    }

    @Test
    void exists_with_stream_id_examines_one_document_when_the_stream_has_many_events() {
        eventStore.write(STREAM_ID, IntStream.range(0, MATCHING_EVENTS).mapToObj(i -> event("event-" + i, "SomethingHappened")).toList());
        commandsSentToEventCollection.clear();

        boolean exists = eventStore.exists(STREAM_ID);

        assertThat(exists).isTrue();
        assertThat(documentsExaminedBy(theOnlyFindLimitedToOneDocument())).isEqualTo(1);
    }

    @Test
    void exists_with_filter_examines_one_document_when_many_events_match() {
        eventStore.write(STREAM_ID, IntStream.range(0, MATCHING_EVENTS).mapToObj(i -> event("event-" + i, "SomethingHappened")).toList());
        commandsSentToEventCollection.clear();

        boolean exists = eventStore.exists(Filter.type("SomethingHappened"));

        assertThat(exists).isTrue();
        assertThat(documentsExaminedBy(theOnlyFindLimitedToOneDocument())).isEqualTo(1);
    }

    @Test
    void exists_inside_an_ambient_session_sees_events_its_transaction_has_not_committed() {
        try (ClientSession session = mongoClient.startSession()) {
            session.startTransaction();
            ClientSessionHolder.set(session);
            try {
                eventStore.append(List.of(taggedEvent("exists:tag")));
                eventStore.write(STREAM_ID, List.of(event("event-1", "SomethingHappened")));

                assertThat(eventStore.exists(DcbCriteria.tags(Tag.parse("exists:tag")))).as("exists(DcbCriteria) inside the session").isTrue();
                assertThat(eventStore.exists(STREAM_ID)).as("exists(streamId) inside the session").isTrue();
                assertThat(eventStore.exists(Filter.all())).as("exists(Filter.all()) inside the session").isTrue();
            } finally {
                ClientSessionHolder.remove();
            }

            assertThat(eventStore.exists(DcbCriteria.tags(Tag.parse("exists:tag")))).as("exists(DcbCriteria) outside the session").isFalse();
            assertThat(eventStore.exists(STREAM_ID)).as("exists(streamId) outside the session").isFalse();
            assertThat(eventStore.exists(Filter.all())).as("exists(Filter.all()) outside the session").isFalse();
            session.abortTransaction();
        }
    }

    private BsonDocument theOnlyFindLimitedToOneDocument() {
        assertThat(commandsSentToEventCollection).as("commands exists sent to the event collection").hasSize(1);
        BsonDocument command = commandsSentToEventCollection.getFirst();
        assertThat(command.containsKey("find")).as("exists sends a find, not a count. Command: %s", command.toJson()).isTrue();
        assertThat(command.getNumber("limit").intValue()).isEqualTo(1);
        return command;
    }

    private long documentsExaminedBy(BsonDocument findCommand) {
        Document find = new Document("find", COLLECTION)
                .append("filter", findCommand.getDocument("filter"))
                .append("projection", findCommand.getDocument("projection"))
                .append("limit", 1);
        Document explain = database.runCommand(new Document("explain", find).append("verbosity", "executionStats"));
        return explain.get("executionStats", Document.class).get("totalDocsExamined", Number.class).longValue();
    }

    private static CloudEvent taggedEvent(String tag) {
        return DcbCloudEvents.withTags(event(UUID.randomUUID().toString(), "SomethingHappened"), List.of(Tag.parse(tag)));
    }
}
