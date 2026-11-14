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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
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
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
 * What the reactor store answers to a conditional append through a mongos with one shard.
 * When a marker of the criteria moved and the markers can't settle the condition, the store reads the markers again in a read-only snapshot transaction on a second session and queries the events there (ADR 175).
 * The tests check which appends pass and which are refused, and the marker versions of a read that is the first operation of a transaction and of a read that confirms an append before saving.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbShardedClusterTest {

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

    private ReactorMongoEventStoreConformanceFixture fixture;
    private DcbEventStore eventStore;
    private MongoClient transactionalClient;
    private SimpleReactiveMongoDatabaseFactory databaseFactory;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore transactionalStore;
    private final AtomicBoolean recordingCommands = new AtomicBoolean();
    private final Queue<BsonDocument> commands = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void create_the_event_store() {
        String database = "sharded_" + UUID.randomUUID().toString().replace("-", "");
        assertThat(hello(database).get("msg")).as("The container must be a mongos, or nothing below tests the sharded case").isEqualTo("isdbgrid");
        fixture = new ReactorMongoEventStoreConformanceFixture(new ConnectionString(mongos.getConnectionString() + "/" + database + ".events"));
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
        databaseFactory = new SimpleReactiveMongoDatabaseFactory(transactionalClient, database);
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        transactionalStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(databaseFactory), new EventStoreConfig.Builder()
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
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_because_it_asks_the_events_at_one_point_in_time() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatCode(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it")
                .doesNotThrowAnyException();
    }

    @Test
    void accepts_an_append_after_another_conditional_append_on_the_same_criteria_that_appended_no_matching_event() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        DcbConsistencyToken otherToken = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)), failIfEventsMatch(otherToken));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatCode(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The first append moved the marker of the criteria without a matching event, and the events above it settle that")
                .doesNotThrowAnyException();
    }

    @Test
    void accepts_an_append_after_an_event_with_only_some_of_the_tags_of_the_criteria() {
        DcbCriteria criteria = DcbCriteria.type(ENROLLED).tags(tag("course:c1"), tag("student:s1"));
        DcbConsistencyToken token = eventStore.read(criteria).consistencyToken();
        eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s2")));

        assertThatCode(() -> eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s1")), failIfEventsMatch(token)))
                .as("Only the marker of course c1 moved, and no event above it has student s1 as well")
                .doesNotThrowAnyException();
    }

    @Test
    void accepts_an_append_after_an_event_that_shares_no_tag_with_the_criteria() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, "concert:k2")));

        assertThatCode(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("The event moved no marker of the criteria, so the markers settle the condition without the events")
                .doesNotThrowAnyException();
    }

    @Test
    void refuses_an_append_after_an_event_that_matches_the_criteria() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("The marker of the tag moved, and the store finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void reports_marker_versions_for_a_read_that_is_the_first_operation_of_a_transaction_and_rolls_back_an_append_in_it_with_the_transaction() {
        append("before");
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        inTransactionThatRollsBack(() -> transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction())
                .doOnNext(read::set)
                .then(transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1)))));

        assertThat(read.get().markerVersions()).as("a mongos reads the versions in a snapshot transaction of the store's own").isPresent();
        assertThat(transactionalStore.read(TICKETS_OF_K1).block().events()).as("the append ran in the transaction, which rolled back").extracting(CloudEvent::getId).containsExactly("before");
        append("later");
        assertThat(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.afterVersions(read.get().markerVersions().orElseThrow())).block().events())
                .extracting(CloudEvent::getId).containsExactly("later");
    }

    @Test
    void reads_the_versions_for_a_read_that_is_the_first_operation_of_a_transaction_in_a_session_beside_the_one_of_the_transaction() {
        append("before");

        recordingCommands.set(true);
        inTransactionThatRollsBack(() -> transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction())
                .then(transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1)))));
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
        append("before");
        AtomicReference<DcbEventStream> read = new AtomicReference<>();
        AtomicBoolean sessionBoundAfterRead = new AtomicBoolean();

        Throwable failure = catchThrowable(() -> inScopeWithoutTransaction(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .doOnNext(read::set)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(synchronization -> synchronization.getResource(databaseFactory) != null))
                .doOnNext(sessionBoundAfterRead::set)
                .then(transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1))))
                .then(Mono.error(new IllegalStateException("scope fails after the append")))).block());

        assertThat(failure).hasMessage("scope fails after the append");
        assertThat(read.get().markerVersions()).isPresent();
        assertThat(sessionBoundAfterRead).as("the read binds a session to the scope").isTrue();
        assertThat(transactionalStore.read(TICKETS_OF_K1).block().events()).as("the append joined the session transaction the read bound, which rolled back").extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void reads_the_versions_for_a_read_that_confirms_an_append_before_saving_in_a_transaction_beside_the_one_it_binds() {
        append("before");

        recordingCommands.set(true);
        catchThrowable(() -> inScopeWithoutTransaction(transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(transactionalStore.append(List.of(taggedEventWithId("own", TICKET_SOLD, CONCERT_K1))))
                .then(Mono.error(new IllegalStateException("scope fails after the append")))).block());
        recordingCommands.set(false);

        // The driver pools server sessions, so the bound session can reuse the lsid of the read's closed one
        List<BsonValue> boundTransaction = commands.stream().filter(command -> command.containsKey("abortTransaction"))
                .map(command -> List.<BsonValue>of(command.getDocument("lsid"), command.get("txnNumber"))).findFirst().orElseThrow();
        assertThat(commands.stream().filter(command -> command.containsKey("readConcern")
                        && command.getDocument("readConcern").getString("level", new BsonString("")).getValue().equals("snapshot"))
                .map(command -> List.<BsonValue>of(command.getDocument("lsid"), command.get("txnNumber"))))
                .as("the read runs in a snapshot transaction beside the one it binds")
                .isNotEmpty()
                .doesNotContain(boundTransaction);
    }

    @Test
    void reads_after_saved_marker_versions_as_the_first_operation_of_a_transaction() {
        append("before");
        DcbMarkerVersions saved = transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        append("later");
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        Throwable failure = catchThrowable(() -> inTransactionThatRollsBack(() -> transactionalStore.read(TICKETS_OF_K1, DcbReadOptions.afterVersions(saved).asFirstOperationOfTransaction()).doOnNext(read::set)));

        assertThat(failure).as("the store refuses no read after saved versions that is the first operation of the transaction").isNull();
        assertThat(read.get().events()).as("a mongos resumes after the saved versions in a snapshot transaction of the store's own").extracting(CloudEvent::getId).containsExactly("later");
        assertThat(read.get().markerVersions()).isPresent();
    }

    private void append(String id) {
        transactionalStore.append(List.of(taggedEventWithId(id, TICKET_SOLD, CONCERT_K1))).block();
    }

    private void inTransactionThatRollsBack(Supplier<Mono<?>> body) {
        TransactionalOperator.create(transactionManager).execute(status -> {
            status.setRollbackOnly();
            return body.get().then();
        }).then().block();
    }

    private <T> Mono<T> inScopeWithoutTransaction(Mono<T> body) {
        return TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_SUPPORTS)).transactional(body);
    }

    private static Document hello(String database) {
        try (MongoClient client = MongoClients.create(mongos.getConnectionString())) {
            return Mono.from(client.getDatabase(database).runCommand(new Document("hello", 1))).block();
        }
    }
}
