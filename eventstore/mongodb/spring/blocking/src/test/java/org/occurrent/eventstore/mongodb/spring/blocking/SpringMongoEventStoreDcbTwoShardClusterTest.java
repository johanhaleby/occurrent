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
import org.assertj.core.api.SoftAssertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster.Hold;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What the Spring blocking store answers to a conditional append through a mongos in front of two shards (ADR 175). The events
 * live on the second shard, the marker of tag course:c1 on the first and the marker of tag student:s1 on the second, so
 * a read can see one of an event's markers and not the other. The store asks for the events a moved marker may stand for in
 * a snapshot transaction of its own, whatever transaction the append runs in.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbTwoShardClusterTest {

    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_RENAMED = "ConcertRenamed";
    private static final String ENROLLED = "StudentEnrolledInCourse";
    private static final String CONCERT_K1 = "concert:k1";
    private static final String COURSE_C1 = "course:c1";
    private static final String STUDENT_S1 = "student:s1";
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));
    private static final DcbCriteria ENROLLMENTS_OF_S1_IN_C1 = DcbCriteria.type(ENROLLED).tags(tag(COURSE_C1), tag(STUDENT_S1));
    private static final String COURSE_C1_KEY = DcbMarkerVersionsSupport.tagKey(tag(COURSE_C1));
    private static final String STUDENT_S1_KEY = DcbMarkerVersionsSupport.tagKey(tag(STUDENT_S1));

    private static final int WTIMEOUT_MS = 12_345;
    private static final long MARKER_VERSIONS_COMMIT_TIMEOUT_MS = 6_543;
    private static final WriteConcern CONFIGURED_WRITE_CONCERN = WriteConcern.W1.withWTimeout(WTIMEOUT_MS, MILLISECONDS).withJournal(true);

    private static final List<BsonDocument> COMMANDS = new CopyOnWriteArrayList<>();

    private static TwoShardMongoCluster cluster;
    private static MongoClient client;

    private String database;
    private SimpleMongoClientDatabaseFactory databaseFactory;
    private MongoTransactionManager transactionManager;
    private SpringMongoEventStore eventStore;

    @BeforeAll
    static void start_the_cluster() {
        cluster = TwoShardMongoCluster.start();
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString()))
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        COMMANDS.add(event.getCommand().clone());
                    }
                }).build());
    }

    @AfterAll
    static void stop_the_cluster() {
        if (client != null) {
            client.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void create_the_event_store_with_its_markers_split_across_the_shards() {
        database = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
        databaseFactory = new SimpleMongoClientDatabaseFactory(client, database);
        transactionManager = new MongoTransactionManager(databaseFactory);
        eventStore = new SpringMongoEventStore(new MongoTemplate(databaseFactory), new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build());
        cluster.shardCheckpointCollection(database, "events_dcb_checkpoints");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_because_it_asks_the_events_at_one_point_in_time() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)));

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_another_conditional_append_on_the_same_criteria_that_appended_no_matching_event() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        DcbConsistencyToken otherToken = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)), failIfEventsMatch(otherToken));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)));

        assertThat(thrown).as("The first append moved the marker of the criteria without a matching event, and the events above it settle that").isNull();
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_an_event_with_only_some_of_the_tags_of_the_criteria_whose_markers_sit_on_different_shards() {
        DcbConsistencyToken token = eventStore.read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("other student", ENROLLED, COURSE_C1, "student:s2")));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("enrolled", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(token)));

        assertThat(thrown).as("Only the marker of course c1 moved, and no event above it has student s1 as well").isNull();
        assertThat(eventStore.read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("enrolled");
    }

    @Test
    void refuses_an_append_with_a_token_that_holds_one_tag_marker_from_after_a_matching_event_and_the_other_from_before_it() {
        DcbConsistencyToken before = eventStore.read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1)));
        DcbConsistencyToken after = eventStore.read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        Map<String, MarkerObservation> tornMarkers = new HashMap<>(before.markers());
        tornMarkers.put(COURSE_C1_KEY, after.markers().get(COURSE_C1_KEY));
        DcbConsistencyToken torn = DcbConsistencyToken.of(ENROLLMENTS_OF_S1_IN_C1, before.version(), tornMarkers);

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(torn)));

        assertThat(thrown)
                .as("The marker of course c1 in the token is from after the matching event, so it didn't move. The marker of student s1 moved, "
                        + "and the store asks the events above it, where the matching event is")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_with_a_token_read_while_a_matching_event_had_committed_on_the_first_shard_and_not_on_the_second() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DcbEventStream readWhileHalfCommitted;
        try {
            Future<?> matching = null;
            try (Hold ignored = cluster.holdCommitsOnSecondShard()) {
                matching = executor.submit(() -> eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1))));
                awaitMarkerOnTheFirstShard(COURSE_C1_KEY);
                // A read that waited for the second shard would hang the test, so it gets a bound
                readWhileHalfCommitted = CompletableFuture.supplyAsync(() -> eventStore.read(ENROLLMENTS_OF_S1_IN_C1)).get(10, TimeUnit.SECONDS);
            } finally {
                awaitSettled(matching);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(readWhileHalfCommitted.events()).as("the read must miss the matching event, or the token isn't torn").isEmpty();
        assertThat(readWhileHalfCommitted.consistencyToken().markers().get(COURSE_C1_KEY).version()).as("the read saw the marker of course c1 the matching event created").isEqualTo(1);
        assertThat(readWhileHalfCommitted.consistencyToken().markers().get(STUDENT_S1_KEY).version()).as("the read saw no marker of student s1 yet").isZero();

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(readWhileHalfCommitted.consistencyToken())));

        assertThat(thrown)
                .as("The read saw the marker of course c1 after the matching event and the marker of student s1 before it, and missed the event. "
                        + "Only the marker of student s1 moved, and the store finds the event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_after_a_matching_event_in_the_stores_own_transaction() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1)));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)));

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_a_plain_read_in_the_executors_transaction_comes_first() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));

        Throwable thrown = appendInTheExecutorsTransaction(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1), token);

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void refuses_an_append_after_a_matching_event_when_a_plain_read_in_the_executors_transaction_comes_first() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1)));

        Throwable thrown = appendInTheExecutorsTransaction(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1), token);

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_it_joins_a_callers_transaction_at_the_default_read_concern() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))));

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it, whatever the caller's transaction reads").isNull();
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("commitTransaction");
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_it_joins_a_callers_transaction_at_read_concern_snapshot_and_write_concern_majority() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));
        TransactionOptions snapshotMajority = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build();
        MongoTransactionManager callersTransactionManager = new MongoTransactionManager(databaseFactory, snapshotMajority);

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> new TransactionTemplate(callersTransactionManager).executeWithoutResult(status ->
                eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))));

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("commitTransaction");
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void refuses_an_append_after_a_matching_event_when_it_joins_a_callers_transaction_at_the_default_read_concern() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1)));

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> new TransactionTemplate(new MongoTransactionManager(databaseFactory)).executeWithoutResult(status ->
                eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))));

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_after_a_matching_event_that_committed_after_the_callers_transaction_read_the_events_and_before_it_read_the_marker() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        MongoTemplate plainTemplate = new MongoTemplate(databaseFactory);

        Throwable thrown = catchThrowable(() -> new TransactionTemplate(new MongoTransactionManager(databaseFactory)).executeWithoutResult(status -> {
            plainTemplate.exists(new Query(Criteria.where("id").is("none")), "events");
            // Another thread, so the matching event commits on a transaction of its own and not the caller's
            CompletableFuture.runAsync(() -> eventStore.append(List.of(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1)))).join();
            eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));
        }));

        assertThat(thrown)
                .as("The caller's transaction read the events before the matching event committed and the marker after it, so the marker moved while its own read of the events misses "
                        + "the matching event. The store's own snapshot transaction finds it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void keeps_a_write_a_transaction_hook_made_before_the_stores_callback_and_sends_no_abortTransaction_when_a_conditional_append_runs_through_a_mongos() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));
        MongoTemplate hookTemplate = new MongoTemplate(databaseFactory);
        // A collection can't be created inside a distributed transaction
        hookTemplate.createCollection("hook");
        List<String> hookIds = new CopyOnWriteArrayList<>();
        MongoTransactionManager hookedTransactionManager = new MongoTransactionManager(databaseFactory);
        hookedTransactionManager.addListener(new TransactionExecutionListener() {
            @Override
            public void afterBegin(TransactionExecution transaction, @Nullable Throwable beginFailure) {
                String id = UUID.randomUUID().toString();
                hookTemplate.insert(new Document("_id", id), "hook");
                hookIds.add(id);
            }
        });
        SpringMongoEventStore hookedEventStore = new SpringMongoEventStore(new MongoTemplate(databaseFactory), new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(hookedTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build());

        COMMANDS.clear();
        hookedEventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));

        assertThat(hookIds).as("the transactions the hook ran in").isNotEmpty();
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(client.getDatabase(database).getCollection("hook").find().map(document -> document.getString("_id")).into(new ArrayList<>()))
                    .as("the documents the hook wrote in its transaction, which the store's check neither aborted nor restarted").containsExactlyInAnyOrderElementsOf(hookIds);
            softly.assertThat(COMMANDS).filteredOn(command -> "insert".equals(command.getFirstKey()) && "hook".equals(command.getString("insert").getValue()))
                    .as("the hook's insert, which was sent in the transaction").isNotEmpty().allMatch(command -> command.containsKey("autocommit"));
            softly.assertThat(COMMANDS).extracting(BsonDocument::getFirstKey).as("the commands of the conditional append").doesNotContain("abortTransaction");
            softly.assertThat(eventStore.read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
        });
    }

    @Test
    void refuses_an_append_that_would_pass_when_the_commit_of_the_snapshot_transaction_fails() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));

        Throwable thrown;
        try (Hold ignored = cluster.failNextCommitOnSecondShard()) {
            thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)));
        }

        assertThat(thrown)
                .as("Nothing matching was appended, so the append passes unless the snapshot transaction fails. The first commit after the fail point is the snapshot transaction's")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).as("the append's event wasn't stored").isEmpty();
    }

    @Test
    void refuses_the_second_of_two_conditional_appends_in_a_callers_transaction_when_the_first_one_moved_their_shared_marker() {
        eventStore.append(List.of(taggedEventWithId("seed", TICKET_SOLD, CONCERT_K1)));
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        Throwable[] second = new Throwable[1];

        new TransactionTemplate(new MongoTransactionManager(databaseFactory)).executeWithoutResult(status -> {
            eventStore.append(List.of(taggedEventWithId("first", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));
            second[0] = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)));
            status.setRollbackOnly();
        });

        assertThat(second[0])
                .as("The second append reads the marker at the first one's uncommitted increment, so it moved, while the store's own snapshot shows the version the token saw. "
                        + "The first event matches the criteria and isn't committed, so the snapshot can't find it, and the store refuses an append whose snapshot is behind its own read")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(eventStore.read(TICKETS_OF_K1).events()).as("neither of the caller's events was stored").extracting(CloudEvent::getId).containsExactly("seed");
    }

    @Test
    void commits_the_stores_own_conditional_append_with_the_configured_write_concern_through_a_mongos() {
        Recorded recorded = recordedConditionalAppendWithConfiguredConcerns();

        BsonDocument commit = single(theAppendsTransaction(recorded.commands()), "commitTransaction");
        assertThat(commit.getDocument("writeConcern"))
                .as("the commitTransaction of the append, which keeps the write concern the transaction manager configured, not majority")
                .isEqualTo(BsonDocument.parse("{\"w\": 1, \"wtimeout\": " + WTIMEOUT_MS + ", \"j\": true}"));
    }

    @Test
    void sends_no_abortTransaction_when_the_stores_own_conditional_append_commits_through_a_mongos() {
        Recorded recorded = recordedConditionalAppendWithConfiguredConcerns();
        List<BsonDocument> commands = recorded.commands();

        BsonDocument commit = single(theAppendsTransaction(commands), "commitTransaction");
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(commands).extracting(BsonDocument::getFirstKey).as("nothing aborts the append's transaction or the store's snapshot transaction").doesNotContain("abortTransaction");
            softly.assertThat(commit).isNotNull();
            softly.assertThat(recorded.eventIds()).containsExactly("sold");
        });
    }

    @Test
    void runs_every_command_of_the_stores_own_conditional_append_in_one_session_and_transaction_that_starts_at_the_configured_read_concern() {
        List<BsonDocument> inTransaction = theAppendsTransaction(recordedConditionalAppendWithConfiguredConcerns().commands());

        assertThat(inTransaction).as("the commands of the append's transaction").isNotEmpty();
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(inTransaction).extracting(command -> command.getFirstKey()).as("the finds, the writes and the commit of the append").contains("find", "insert", "commitTransaction");
            softly.assertThat(inTransaction).extracting(command -> command.get("txnNumber")).as("the transaction of every command").containsOnly(inTransaction.get(0).get("txnNumber"));
            softly.assertThat(inTransaction.get(0).getDocument("readConcern").getString("level").getValue()).as("the first command of the transaction, which isn't raised to snapshot").isEqualTo("majority");
        });
    }

    @Test
    void asks_the_events_of_a_moved_marker_in_a_snapshot_transaction_on_another_session_than_the_appends() {
        List<BsonDocument> commands = recordedConditionalAppendWithConfiguredConcerns().commands();

        List<BsonDocument> append = theAppendsTransaction(commands);
        List<BsonDocument> snapshot = theOtherTransactions(commands);
        assertThat(snapshot).as("the commands of a transaction on another session than the append's").isNotEmpty();
        BsonDocument commit = snapshot.get(snapshot.size() - 1);
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(snapshot).extracting(command -> command.get("lsid")).as("the session of every command of that transaction").containsOnly(snapshot.get(0).get("lsid"));
            softly.assertThat(snapshot).extracting(command -> command.get("txnNumber")).as("the transaction of every command").containsOnly(snapshot.get(0).get("txnNumber"));
            softly.assertThat(snapshot.get(0).getDocument("readConcern").getString("level").getValue()).as("the first command of the transaction that asks for the events").isEqualTo("snapshot");
            softly.assertThat(snapshot).filteredOn(command -> asksForTheEventsOfAMovedMarker(command)).as("the events query of the moved marker in that transaction").hasSize(1);
            softly.assertThat(commit.getFirstKey()).as("that transaction ends with a commit, since it only reads").isEqualTo("commitTransaction");
            softly.assertThat(commit.getNumber("maxTimeMS", new BsonInt32(0)).longValue())
                    .as("the max time of that commit, bounded by the configured marker versions commit timeout. Under a client timeout the driver sends what remains of it")
                    .isBetween(1L, MARKER_VERSIONS_COMMIT_TIMEOUT_MS);
            softly.assertThat(commit.getDocument("writeConcern", new BsonDocument()).get("w")).as("the write concern of that commit").isEqualTo(new BsonString("majority"));
            softly.assertThat(append).filteredOn(command -> asksForTheEventsOfAMovedMarker(command)).as("the events query of the moved marker in the append's own transaction").isEmpty();
        });
    }

    @Test
    void keeps_the_configured_read_and_write_concern_for_an_unconditional_append_because_it_reads_nothing_at_one_point_in_time() {
        TransactionOptions configured = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(CONFIGURED_WRITE_CONCERN).build();

        Recorded recorded = recordedAppend(configured, false);
        List<BsonDocument> commands = recorded.commands();

        BsonDocument first = commands.stream().filter(command -> command.containsKey("autocommit")).findFirst().orElseThrow();
        BsonDocument commit = single(commands, "commitTransaction");
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(first.getDocument("readConcern").getString("level").getValue()).as("the first command of the transaction of an append without a condition").isEqualTo("majority");
            softly.assertThat(commit.getDocument("writeConcern"))
                    .as("the commitTransaction of an append without a condition, which isn't raised to majority")
                    .isEqualTo(BsonDocument.parse("{\"w\": 1, \"wtimeout\": " + WTIMEOUT_MS + ", \"j\": true}"));
            softly.assertThat(recorded.eventIds()).containsExactly("sold");
        });
    }

    private Recorded recordedConditionalAppendWithConfiguredConcerns() {
        TransactionOptions configured = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(CONFIGURED_WRITE_CONCERN).build();
        return recordedAppend(configured, true);
    }

    private Recorded recordedAppend(TransactionOptions configured, boolean conditional) {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        CommandListener recorder = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                recorded.add(event.getCommand().clone());
            }
        };
        try (MongoClient recordingClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString())).addCommandListener(recorder).build())) {
            String recordingDatabase = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
            SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(recordingClient, recordingDatabase);
            SpringMongoEventStore store = new SpringMongoEventStore(new MongoTemplate(factory), new EventStoreConfig.Builder()
                    .eventStoreCollectionName("events")
                    .transactionConfig(new CommitRetryingMongoTransactionManager(factory, configured))
                    .markerVersionsCommitTimeout(Duration.ofMillis(MARKER_VERSIONS_COMMIT_TIMEOUT_MS))
                    .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                    .eventStoreCapabilities(STREAM, DCB)
                    .build());
            cluster.shardCheckpointCollection(recordingDatabase, "events_dcb_checkpoints");
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).consistencyToken();
            store.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)));
            recorded.clear();

            if (conditional) {
                store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token));
            } else {
                store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)));
            }

            return new Recorded(new ArrayList<>(recorded), store.read(TICKETS_OF_K1).events().stream().map(CloudEvent::getId).toList());
        }
    }

    private record Recorded(List<BsonDocument> commands, List<String> eventIds) {
    }

    private static BsonDocument single(List<BsonDocument> commands, String name) {
        List<BsonDocument> matching = commands.stream().filter(command -> name.equals(command.getFirstKey())).toList();
        assertThat(matching).as("the " + name + " commands of the append").hasSize(1);
        return matching.get(0);
    }

    // The starters' path, where a SpringTransactionExecutor runs a plain command and then the append in a transaction of the store's own transaction manager
    private Throwable appendInTheExecutorsTransaction(CloudEvent event, DcbConsistencyToken token) {
        MongoTemplate plainTemplate = new MongoTemplate(databaseFactory);
        return catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            plainTemplate.exists(new Query(Criteria.where("id").is("none")), "events");
            eventStore.append(List.of(event), failIfEventsMatch(token));
        }));
    }

    // The commands of the transaction that inserts the append's events
    private static List<BsonDocument> theAppendsTransaction(List<BsonDocument> commands) {
        BsonDocument insert = commands.stream().filter(command -> command.containsKey("autocommit") && "insert".equals(command.getFirstKey())).findFirst().orElseThrow();
        return commands.stream().filter(command -> command.containsKey("autocommit") && insert.get("lsid").equals(command.get("lsid"))).toList();
    }

    private static List<BsonDocument> theOtherTransactions(List<BsonDocument> commands) {
        BsonDocument insert = commands.stream().filter(command -> command.containsKey("autocommit") && "insert".equals(command.getFirstKey())).findFirst().orElseThrow();
        return commands.stream().filter(command -> command.containsKey("autocommit") && !insert.get("lsid").equals(command.get("lsid"))).toList();
    }

    // The events query that follows a moved marker selects them by the marker versions in their mv. Spring's exists counts them
    private static boolean asksForTheEventsOfAMovedMarker(BsonDocument command) {
        return Set.of("find", "aggregate", "count").contains(command.getFirstKey()) && command.get(command.getFirstKey()).isString()
                && "events".equals(command.getString(command.getFirstKey()).getValue()) && command.toJson().contains("\"mv\"");
    }

    // The store joined the callers transaction, so its commands share the callers session and transaction number, and the only
    // abortTransaction is the callers own rollback at the end. The store's snapshot transaction is on a session of its own
    private static void assertTheCallersTransactionWasNeverRestartedAndEndedWith(String lastCommand) {
        List<BsonDocument> all = COMMANDS.stream().filter(command -> command.containsKey("autocommit")).toList();
        assertThat(all).as("the commands of the callers transaction").isNotEmpty();
        List<BsonDocument> inTransaction = all.stream().filter(command -> all.get(0).get("lsid").equals(command.get("lsid"))).toList();
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(inTransaction).extracting(command -> command.get("txnNumber")).containsOnly(inTransaction.get(0).get("txnNumber"));
            softly.assertThat(inTransaction.get(inTransaction.size() - 1).getFirstKey()).isEqualTo(lastCommand);
            softly.assertThat(inTransaction.subList(0, inTransaction.size() - 1)).extracting(BsonDocument::getFirstKey).as("nothing aborted the transaction before it ended").doesNotContain("abortTransaction");
        });
    }

    // The first shard committed its half of the transaction once the marker is there, while the second shard holds its own
    private void awaitMarkerOnTheFirstShard(String key) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (client.getDatabase(database).getCollection("events_dcb_checkpoints").find(new Document("_id", "marker:" + key)).first() == null) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("The marker " + key + " didn't show up on the first shard within 30 seconds");
            }
            Thread.sleep(50);
        }
    }

    // On a failing path the hold is already released when this runs, so the append still in flight finishes
    private static void awaitSettled(Future<?> append) throws Exception {
        if (append != null) {
            append.get(30, TimeUnit.SECONDS);
        }
    }
}
