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
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
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
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * What the reactor store answers to a conditional append through a mongos in front of two shards (ADR 175). The events
 * live on the second shard, the marker of tag course:c1 on the first and the marker of tag student:s1 on the second, so
 * a read can see one of an event's markers and not the other.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTwoShardClusterTest {

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

    private static final List<BsonDocument> COMMANDS = new CopyOnWriteArrayList<>();

    private static TwoShardMongoCluster cluster;
    private static MongoClient client;

    private String database;
    private SimpleReactiveMongoDatabaseFactory databaseFactory;
    private ReactiveMongoTemplate template;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore eventStore;

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
    void create_the_event_store_from_a_transaction_manager_with_its_markers_split_across_the_shards() {
        database = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
        databaseFactory = new SimpleReactiveMongoDatabaseFactory(client, database);
        template = new ReactiveMongoTemplate(databaseFactory);
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        eventStore = new ReactorMongoEventStore(template, configuration().transactionConfig(transactionManager).build());
        cluster.shardCheckpointCollection(database, "events_dcb_checkpoints");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_because_it_asks_the_events_at_one_point_in_time() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_another_conditional_append_on_the_same_criteria_that_appended_no_matching_event() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        DcbConsistencyToken otherToken = read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1)), failIfEventsMatch(otherToken)).block();

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());

        assertThat(thrown).as("The first append moved the marker of the criteria without a matching event, and the events above it settle that").isNull();
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_an_event_with_only_some_of_the_tags_of_the_criteria_whose_markers_sit_on_different_shards() {
        DcbConsistencyToken token = read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        append(taggedEventWithId("other student", ENROLLED, COURSE_C1, "student:s2"));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("enrolled", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(token)).block());

        assertThat(thrown).as("Only the marker of course c1 moved, and no event above it has student s1 as well").isNull();
        assertThat(read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("enrolled");
    }

    @Test
    void refuses_an_append_with_a_token_that_holds_one_tag_marker_from_after_a_matching_event_and_the_other_from_before_it() {
        DcbConsistencyToken before = read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        append(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1));
        DcbConsistencyToken after = read(ENROLLMENTS_OF_S1_IN_C1).consistencyToken();
        Map<String, MarkerObservation> tornMarkers = new HashMap<>(before.markers());
        tornMarkers.put(COURSE_C1_KEY, after.markers().get(COURSE_C1_KEY));
        DcbConsistencyToken torn = DcbConsistencyToken.of(ENROLLMENTS_OF_S1_IN_C1, before.version(), tornMarkers);

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(torn)).block());

        assertThat(thrown)
                .as("The marker of course c1 in the token is from after the matching event, so it didn't move. The marker of student s1 moved, "
                        + "and the store asks the events above it, where the matching event is")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_with_a_token_read_while_a_matching_event_had_committed_on_the_first_shard_and_not_on_the_second() throws Exception {
        CompletableFuture<?> matching = null;
        DcbEventStream readWhileHalfCommitted;
        try {
            try (Hold ignored = cluster.holdCommitsOnSecondShard()) {
                matching = eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1))).toFuture();
                awaitMarkerOnTheFirstShard(COURSE_C1_KEY);
                // A read that waited for the second shard would hang the test, so it gets a bound
                readWhileHalfCommitted = eventStore.read(ENROLLMENTS_OF_S1_IN_C1).block(Duration.ofSeconds(10));
            }
        } finally {
            // The hold is released by now, so the append still in flight finishes
            if (matching != null) {
                matching.get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(readWhileHalfCommitted.events()).as("the read must miss the matching event, or the token isn't torn").isEmpty();
        assertThat(readWhileHalfCommitted.consistencyToken().markers().get(COURSE_C1_KEY).version()).as("the read saw the marker of course c1 the matching event created").isEqualTo(1);
        assertThat(readWhileHalfCommitted.consistencyToken().markers().get(STUDENT_S1_KEY).version()).as("the read saw no marker of student s1 yet").isZero();

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(readWhileHalfCommitted.consistencyToken())).block());

        assertThat(thrown)
                .as("The read saw the marker of course c1 after the matching event and the marker of student s1 before it, and missed the event. "
                        + "Only the marker of student s1 moved, and the store finds the event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_after_an_event_of_another_type_with_the_tag_when_it_joins_a_callers_transaction_at_the_default_read_concern() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(transactionManager)
                .transactional(eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))).block());

        assertThat(thrown)
                .as("The caller's transaction doesn't read every shard at one point in time, so the store compares only the markers, and the marker of the tag moved")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("abortTransaction");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_the_store_was_built_from_a_transactional_operator() {
        ReactorMongoEventStore builtFromOperator = new ReactorMongoEventStore(template, configuration().transactionConfig(TransactionalOperator.create(transactionManager)).build());
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        Throwable thrown = catchThrowable(() -> builtFromOperator.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());

        assertThat(thrown).as("The store restarts the transaction its operator began at snapshot and majority, so it asks the events above the marker that moved, and none matches").isNull();
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_it_joins_a_callers_transaction_at_read_concern_snapshot_and_write_concern_majority() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));
        TransactionOptions snapshotMajority = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build();
        ReactiveMongoTransactionManager callersTransactionManager = new ReactiveMongoTransactionManager(databaseFactory, snapshotMajority);

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(callersTransactionManager)
                .transactional(eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))).block());

        assertThat(thrown).as("The caller's transaction reads every shard at one point in time, so the store asks the events above the marker that moved, and none matches").isNull();
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("commitTransaction");
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    // The store's own conditional append restarts its transaction at snapshot and majority, through a mongos
    @Test
    void commits_the_stores_own_conditional_append_with_the_wtimeout_and_journal_flag_of_the_client() {
        List<BsonDocument> commands = recordedConditionalAppendWithConfiguredWriteConcern().commands();

        BsonDocument commit = single(commands, "commitTransaction");
        assertThat(commit.getDocument("writeConcern"))
                .as("the commitTransaction of the append, whose write concern the client configured with a wtimeout and the journal flag")
                .isEqualTo(BsonDocument.parse("{\"w\": \"majority\", \"wtimeout\": " + WTIMEOUT_MS + ", \"j\": true}"));
    }

    @Test
    void sends_no_abortTransaction_while_the_store_restarts_its_own_conditional_append_and_the_append_commits() {
        Recorded recorded = recordedConditionalAppendWithConfiguredWriteConcern();
        List<BsonDocument> commands = recorded.commands();

        assertThat(commands).extracting(BsonDocument::getFirstKey).as("the restart aborts locally, because the transaction had sent nothing").doesNotContain("abortTransaction");
        assertThat(single(commands, "commitTransaction")).isNotNull();
        assertThat(recorded.eventIds()).containsExactly("sold");
    }

    @Test
    void runs_every_command_of_the_stores_own_conditional_append_in_one_session_and_transaction_that_starts_at_read_concern_snapshot() {
        List<BsonDocument> commands = recordedConditionalAppendWithConfiguredWriteConcern().commands();

        List<BsonDocument> inTransaction = commands.stream().filter(command -> command.containsKey("autocommit")).toList();
        assertThat(inTransaction).extracting(command -> command.getFirstKey()).as("the finds, the writes and the commit of the append").contains("find", "commitTransaction");
        assertThat(inTransaction).extracting(command -> command.get("lsid")).as("the session of every command").hasSize(inTransaction.size()).containsOnly(inTransaction.get(0).get("lsid"));
        assertThat(inTransaction).extracting(command -> command.get("txnNumber")).as("the transaction of every command").containsOnly(inTransaction.get(0).get("txnNumber"));
        assertThat(inTransaction.get(0).getDocument("readConcern").getString("level").getValue()).as("the first command of the transaction").isEqualTo("snapshot");
    }

    private Recorded recordedConditionalAppendWithConfiguredWriteConcern() {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        CommandListener recorder = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                recorded.add(event.getCommand().clone());
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString()))
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(WTIMEOUT_MS, MILLISECONDS).withJournal(true)).addCommandListener(recorder).build();
        try (MongoClient recordingClient = MongoClients.create(settings)) {
            String recordingDatabase = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
            SimpleReactiveMongoDatabaseFactory factory = new SimpleReactiveMongoDatabaseFactory(recordingClient, recordingDatabase);
            ReactorMongoEventStore store = new ReactorMongoEventStore(new ReactiveMongoTemplate(factory),
                    configuration().transactionConfig(new CommitRetryingReactiveMongoTransactionManager(factory)).build());
            cluster.shardCheckpointCollection(recordingDatabase, "events_dcb_checkpoints");
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).block().consistencyToken();
            store.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1))).block();
            recorded.clear();

            store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block();

            return new Recorded(new ArrayList<>(recorded), store.read(TICKETS_OF_K1).block().events().stream().map(CloudEvent::getId).toList());
        }
    }

    private record Recorded(List<BsonDocument> commands, List<String> eventIds) {
    }

    private static BsonDocument single(List<BsonDocument> commands, String name) {
        List<BsonDocument> matching = commands.stream().filter(command -> name.equals(command.getFirstKey())).toList();
        assertThat(matching).as("the " + name + " commands of the append").hasSize(1);
        return matching.get(0);
    }

    // The store joined the callers transaction, so its commands share the callers session and transaction number, and the only
    // abortTransaction is the callers own rollback at the end
    private static void assertTheCallersTransactionWasNeverRestartedAndEndedWith(String lastCommand) {
        List<BsonDocument> inTransaction = COMMANDS.stream().filter(command -> command.containsKey("autocommit")).toList();
        assertThat(inTransaction).as("the commands of the callers transaction").isNotEmpty();
        assertThat(inTransaction).extracting(command -> command.get("lsid")).containsOnly(inTransaction.get(0).get("lsid"));
        assertThat(inTransaction).extracting(command -> command.get("txnNumber")).containsOnly(inTransaction.get(0).get("txnNumber"));
        assertThat(inTransaction.get(inTransaction.size() - 1).getFirstKey()).isEqualTo(lastCommand);
        assertThat(inTransaction.subList(0, inTransaction.size() - 1)).extracting(BsonDocument::getFirstKey).as("nothing aborted the transaction before it ended").doesNotContain("abortTransaction");
    }

    private static EventStoreConfig.Builder configuration() {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB);
    }

    private DcbEventStream read(DcbCriteria criteria) {
        return eventStore.read(criteria).block();
    }

    private void append(CloudEvent event) {
        eventStore.append(List.of(event)).block();
    }

    // The first shard committed its half of the transaction once the marker is there, while the second shard holds its own
    private void awaitMarkerOnTheFirstShard(String key) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (Mono.from(client.getDatabase(database).getCollection("events_dcb_checkpoints").find(new Document("_id", "marker:" + key)).first()).block() == null) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("The marker " + key + " didn't show up on the first shard within 30 seconds");
            }
            Thread.sleep(50);
        }
    }
}
