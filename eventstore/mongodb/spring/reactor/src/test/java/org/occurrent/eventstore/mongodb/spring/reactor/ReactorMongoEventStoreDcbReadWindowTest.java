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
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What a DCB read returns when an append commits while the read is in flight. The consistency token comes from the markers
 * of the criteria and the events are capped at the position counter, so a token that covers an append has to come with
 * the event of that append, or a decision made on the events would be appended with a token that hides the event it
 * missed. A fail point holds the marker read of the reader while the position read is free to return, which puts the
 * append between the two reads in the order that the position was read first. The fail point blocks the connection
 * before the server runs the command, so the marker read sees the append.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbReadWindowTest {

    private static final String READER_APPLICATION_NAME = "dcb-read-window-reader";
    private static final String ACCOUNT = "account:a1";
    private static final DcbCriteria BOUNDARY = DcbCriteria.tags(tag(ACCOUNT));
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int MARKER_READ_HELD_MILLIS = 4_000;
    private static final Duration POSITION_READ_GRACE = Duration.ofSeconds(1);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private final ReaderCommands readerCommands = new ReaderCommands();

    private MongoClient writerClient;
    private MongoClient readerClient;
    private ReactorMongoEventStore writer;
    private ReactorMongoEventStore reader;
    private String markerNamespace;
    private String positionNamespace;

    @BeforeEach
    void create_a_writer_and_a_reader_on_clients_of_their_own() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        String database = requireNonNull(connectionString.getDatabase());
        String collection = "events-" + UUID.randomUUID();
        markerNamespace = database + "." + DcbMarkerModel.checkpointCollectionName(collection);
        positionNamespace = database + "." + DcbMarkerModel.positionCollectionName(collection);
        writerClient = MongoClients.create(connectionString);
        readerClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(READER_APPLICATION_NAME).addCommandListener(readerCommands).build());
        writer = storeOver(writerClient, database, collection);
        reader = storeOver(readerClient, database, collection);
    }

    @AfterEach
    void release_the_fail_point_and_close_the_clients() {
        Mono.from(writerClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"))).block(TIMEOUT);
        readerClient.close();
        writerClient.close();
    }

    @Test
    void a_token_that_covers_an_append_made_while_the_read_was_in_flight_comes_with_the_event_of_that_append() {
        holdTheNextReadOf(markerNamespace);
        CompletableFuture<DcbEventStream> read = reader.read(BOUNDARY).toFuture();
        await().atMost(TIMEOUT).until(() -> readerCommands.hasStartedFindOn(markerNamespace));
        awaitThePositionReadForAtMost(POSITION_READ_GRACE);

        CloudEvent appendedWhileReading = taggedEventWithId("appended-while-reading", "Deposited", ACCOUNT);
        writer.append(List.of(appendedWhileReading)).block(TIMEOUT);
        assertThat(read).as("The append has to commit while the marker read is still held").isNotDone();
        DcbEventStream stream = read.join();

        assertAll(
                () -> assertThat(stream.events()).as("Events of the read").extracting(CloudEvent::getId).containsExactly("appended-while-reading"),
                () -> assertThatCode(() -> writer.append(List.of(taggedEventWithId("next", "Deposited", ACCOUNT)), failIfEventsMatch(stream.consistencyToken())).block(TIMEOUT))
                        .as("The token covers the append, or the events above prove nothing about it")
                        .doesNotThrowAnyException());
    }

    private void holdTheNextReadOf(String namespace) {
        Document data = new Document("failCommands", List.of("find")).append("appName", READER_APPLICATION_NAME).append("namespace", namespace)
                .append("blockConnection", true).append("blockTimeMS", MARKER_READ_HELD_MILLIS);
        Document command = new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1)).append("data", data);
        Mono.from(writerClient.getDatabase("admin").runCommand(command)).block(TIMEOUT);
    }

    // A read that sequences the position after the token doesn't send it while the token is held, so this waits for the
    // position read for a while and goes on without it.
    private void awaitThePositionReadForAtMost(Duration grace) {
        try {
            await().atMost(grace).until(() -> readerCommands.hasSucceededFindOn(positionNamespace));
        } catch (ConditionTimeoutException positionReadIsSequencedAfterTheToken) {
            // Nothing to wait for
        }
    }

    private static ReactorMongoEventStore storeOver(MongoClient client, String database, String collection) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(collection)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        return new ReactorMongoEventStore(new ReactiveMongoTemplate(client, database), config);
    }

    private static final class ReaderCommands implements CommandListener {
        private final Map<Integer, String> findNamespaceByRequest = new ConcurrentHashMap<>();
        private final Set<String> startedFinds = ConcurrentHashMap.newKeySet();
        private final Set<String> succeededFinds = ConcurrentHashMap.newKeySet();

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (event.getCommandName().equals("find")) {
                String namespace = event.getDatabaseName() + "." + event.getCommand().getString("find").getValue();
                findNamespaceByRequest.put(event.getRequestId(), namespace);
                startedFinds.add(namespace);
            }
        }

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            String namespace = findNamespaceByRequest.get(event.getRequestId());
            if (namespace != null) {
                succeededFinds.add(namespace);
            }
        }

        boolean hasStartedFindOn(String namespace) {
            return startedFinds.contains(namespace);
        }

        boolean hasSucceededFindOn(String namespace) {
            return succeededFinds.contains(namespace);
        }
    }
}
