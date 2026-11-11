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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What the Spring blocking store answers to a conditional append when it runs against a mongos.
 * A read there can tear across shards, so a token can hold one marker from after an event and another from before
 * it. The store refuses a conditional append when any marker of its criteria moved, and doesn't try to settle it
 * (ADR 149).
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbShardedClusterTest {

    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_RENAMED = "ConcertRenamed";
    private static final String ENROLLED = "StudentEnrolledInCourse";
    private static final String CONCERT_K1 = "concert:k1";
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));

    @Container
    private static final MongoDBContainer mongos =
            new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                    .withSharding()
                    .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");

    private SpringMongoEventStoreConformanceFixture fixture;
    private DcbEventStore eventStore;
    private MongoClient transactionalClient;
    private SimpleMongoClientDatabaseFactory databaseFactory;
    private MongoTransactionManager transactionManager;
    private SpringMongoEventStore transactionalStore;
    private final AtomicBoolean recordingCommands = new AtomicBoolean();
    private final Queue<BsonDocument> commands = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void create_the_event_store() {
        String database = "sharded_" + UUID.randomUUID().toString().replace("-", "");
        assertThat(hello(database).get("msg")).as("The container must be a mongos, or nothing below tests the sharded case").isEqualTo("isdbgrid");
        fixture = new SpringMongoEventStoreConformanceFixture(new ConnectionString(mongos.getConnectionString() + "/" + database + ".events"));
        eventStore = fixture.dcbEventStore();
        CommandListener commandRecorder = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (recordingCommands.get()) {
                    commands.add(event.getCommand().clone());
                }
            }
        };
        transactionalClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongos.getConnectionString()))
                .addCommandListener(commandRecorder)
                .build());
        databaseFactory = new SimpleMongoClientDatabaseFactory(transactionalClient, database);
        transactionManager = new MongoTransactionManager(databaseFactory);
        transactionalStore = new SpringMongoEventStore(new MongoTemplate(databaseFactory), new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build());
    }

    @AfterEach
    void close_the_event_store() {
        transactionalClient.close();
        fixture.close();
    }

    @Test
    void refuses_an_append_after_an_event_of_another_type_with_the_tag_because_it_doesn_t_ask_the_events() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The marker of the tag moved. A token read through a mongos can hold a marker from after a matching event and miss "
                        + "the event, so the store doesn't ask the events and the check refuses. A store that queried here would let this append through")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_an_append_after_another_conditional_append_on_the_same_criteria_whatever_it_appended() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        DcbConsistencyToken otherToken = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)), failIfEventsMatch(otherToken));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The first append incremented the marker of the criteria without matching it, and on a mongos the check doesn't look further than the marker")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_an_append_after_an_event_with_only_some_of_the_tags_of_the_criteria() {
        DcbCriteria criteria = DcbCriteria.type(ENROLLED).tags(tag("course:c1"), tag("student:s1"));
        DcbConsistencyToken token = eventStore.read(criteria).consistencyToken();
        eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s2")));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s1")), failIfEventsMatch(token)))
                .as("The marker of student s1 didn't move, but a token read through a mongos can hold it from before a matching event, so it doesn't rule the event out")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void accepts_an_append_after_an_event_that_shares_no_tag_with_the_criteria() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, "concert:k2")));

        assertThatCode(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("The event moved no marker of the criteria, so the refusals above are not the store refusing every condition")
                .doesNotThrowAnyException();
    }

    @Test
    void refuses_an_append_after_an_event_that_matches_the_criteria() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("A matching event is refused too, as it is whenever a marker moved")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void reports_marker_versions_for_a_read_that_is_the_first_operation_of_a_transaction_and_rolls_back_an_append_in_it_with_the_transaction() {
        transactionalStore.append(List.of(taggedEventWithId("before", TICKET_SOLD, CONCERT_K1)));
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        inTransactionThatRollsBack(() -> {
            read.set(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction()));
            transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1)));
        });

        assertThat(read.get().markerVersions()).as("a mongos reads the versions in a snapshot transaction of the store's own").isPresent();
        assertThat(transactionalStore.read(TICKETS_OF_K1).events()).as("the append ran in the transaction, which rolled back").extracting(CloudEvent::getId).containsExactly("before");
        transactionalStore.append(List.of(taggedEventWithId("later", TICKET_SOLD, CONCERT_K1)));
        assertThat(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.afterVersions(read.get().markerVersions().orElseThrow())).events())
                .extracting(CloudEvent::getId).containsExactly("later");
    }

    @Test
    void reads_the_versions_for_a_read_that_is_the_first_operation_of_a_transaction_in_a_session_beside_the_one_of_the_transaction() {
        transactionalStore.append(List.of(taggedEventWithId("before", TICKET_SOLD, CONCERT_K1)));

        recordingCommands.set(true);
        inTransactionThatRollsBack(() -> {
            transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction());
            transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1)));
        });
        recordingCommands.set(false);

        BsonDocument sessionOfTheTransaction = commands.stream().filter(command -> command.containsKey("abortTransaction"))
                .map(command -> command.getDocument("lsid")).findFirst().orElseThrow();
        assertThat(commands.stream().filter(command -> command.containsKey("readConcern")
                        && command.getDocument("readConcern").getString("level", new BsonString("")).getValue().equals("snapshot"))
                .map(command -> command.getDocument("lsid")))
                .as("the snapshot transaction of the read runs in a session of its own")
                .isNotEmpty()
                .doesNotContain(sessionOfTheTransaction);
    }

    @Test
    void reports_marker_versions_for_a_read_that_confirms_an_append_before_saving_in_a_scope_without_a_transaction_and_rolls_back_an_append_in_the_scope() {
        transactionalStore.append(List.of(taggedEventWithId("before", TICKET_SOLD, CONCERT_K1)));
        AtomicReference<DcbEventStream> read = new AtomicReference<>();
        AtomicBoolean sessionBoundAfterRead = new AtomicBoolean();

        Throwable failure = catchThrowable(() -> inScopeWithoutTransaction(() -> {
            read.set(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving()));
            sessionBoundAfterRead.set(TransactionSynchronizationManager.getResource(databaseFactory) != null);
            transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1)));
            throw new IllegalStateException("scope fails after the append");
        }));

        assertThat(failure).hasMessage("scope fails after the append");
        assertThat(read.get().markerVersions()).isPresent();
        assertThat(sessionBoundAfterRead).as("the read binds a session to the scope").isTrue();
        assertThat(transactionalStore.read(TICKETS_OF_K1).events()).as("the append joined the session transaction the read bound, which rolled back").extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void reads_after_saved_marker_versions_as_the_first_operation_of_a_transaction() {
        transactionalStore.append(List.of(taggedEventWithId("before", TICKET_SOLD, CONCERT_K1)));
        DcbMarkerVersions saved = transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        transactionalStore.append(List.of(taggedEventWithId("later", TICKET_SOLD, CONCERT_K1)));
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        Throwable failure = catchThrowable(() -> inTransactionThatRollsBack(() -> read.set(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.afterVersions(saved).asFirstOperationOfTransaction()))));

        assertThat(failure).as("the store refuses no read after saved versions that is the first operation of the transaction").isNull();
        assertThat(read.get().events()).as("a mongos resumes after the saved versions in a snapshot transaction of the store's own").extracting(CloudEvent::getId).containsExactly("later");
        assertThat(read.get().markerVersions()).isPresent();
    }

    private void inTransactionThatRollsBack(Runnable body) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            try {
                body.run();
            } finally {
                status.setRollbackOnly();
            }
        });
    }

    private void inScopeWithoutTransaction(Runnable body) {
        TransactionTemplate supports = new TransactionTemplate(transactionManager);
        supports.setPropagationBehavior(TransactionDefinition.PROPAGATION_SUPPORTS);
        supports.executeWithoutResult(status -> body.run());
    }

    private static Document hello(String database) {
        try (MongoClient client = MongoClients.create(mongos.getConnectionString())) {
            return client.getDatabase(database).runCommand(new Document("hello", 1));
        }
    }
}
