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
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import org.assertj.core.api.SoftAssertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
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
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 * a read can see one of an event's markers and not the other. The store asks for the events a moved marker may stand for in
 * a snapshot transaction of its own, whatever transaction the append runs in.
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
    private static final DcbCriteria ENROLLMENTS_IN_C1 = DcbCriteria.type(ENROLLED).tags(tag(COURSE_C1));
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
    void returns_a_matching_event_with_a_tag_marker_on_each_shard_whose_commit_was_in_flight_on_the_events_shard_when_the_read_began() throws Exception {
        CompletableFuture<?> matching = null;
        DcbEventStream read;
        try {
            try (Hold hold = cluster.holdCommitsOnSecondShard()) {
                matching = eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1))).toFuture();
                awaitMarkerOnTheFirstShard(COURSE_C1_KEY);
                CompletableFuture<DcbEventStream> reading = CompletableFuture.supplyAsync(() -> eventStore.read(ENROLLMENTS_OF_S1_IN_C1).block(Duration.ofSeconds(10)));
                // Gives the read a fair chance to return or to block on the held commit
                try {
                    reading.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException blockedOnTheHeldCommit) {
                    // Released below
                }
                hold.close();
                read = reading.get(10, TimeUnit.SECONDS);
            }
        } finally {
            // The hold is released by now, so the append still in flight finishes
            if (matching != null) {
                matching.get(30, TimeUnit.SECONDS);
            }
        }
        // The marker of tag student:s1 lives on the events' shard and is read before the events wait for the commit, so it can be at either version
        assertThat(read.events()).as("The read returns the matching event once its commit on the events shard is visible").extracting(CloudEvent::getId).containsExactly("matching");
        assertThat(read.consistencyToken().markers().get(COURSE_C1_KEY).version()).as("The marker of tag course:c1 in the token is at the version the matching event created").isEqualTo(1);
    }

    @Test
    void refuses_an_append_with_a_token_whose_marker_on_the_events_shard_was_read_before_a_matching_event_the_read_returned() throws Exception {
        CompletableFuture<?> matching = null;
        DcbEventStream read;
        try {
            try (Hold hold = cluster.holdCommitsOnSecondShard()) {
                matching = eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1, STUDENT_S1))).toFuture();
                awaitMarkerOnTheFirstShard(COURSE_C1_KEY);
                CompletableFuture<DcbEventStream> reading = CompletableFuture.supplyAsync(() -> eventStore.read(ENROLLMENTS_OF_S1_IN_C1).block(Duration.ofSeconds(10)));
                // Gives the read a fair chance to return or to block on the held commit
                try {
                    reading.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException blockedOnTheHeldCommit) {
                    // Released below
                }
                hold.close();
                read = reading.get(10, TimeUnit.SECONDS);
            }
        } finally {
            // The hold is released by now, so the append still in flight finishes
            if (matching != null) {
                matching.get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(read.events()).as("The read returns the matching event").extracting(CloudEvent::getId).containsExactly("matching");
        assertThat(read.consistencyToken().markers().get(COURSE_C1_KEY).version()).as("The token holds the marker of tag course:c1 the matching event created").isEqualTo(1);
        assertThat(read.consistencyToken().markers().get(STUDENT_S1_KEY).version()).as("The token holds the marker of tag student:s1 from before the matching event").isZero();

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("second", ENROLLED, COURSE_C1, STUDENT_S1)), failIfEventsMatch(read.consistencyToken())).block());

        assertThat(thrown)
                .as("The marker of tag student:s1 in the token moved, and the store asks the events above it, where the matching event is")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(ENROLLMENTS_OF_S1_IN_C1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void returns_a_matching_event_whose_commit_was_in_flight_on_the_events_shard_when_the_read_began() throws Exception {
        DcbEventStream read = readMatchingEventWhoseCommitIsHeldOnTheEventsShard(eventStore);

        assertThat(read.events()).as("The read returns the matching event once its commit on the events shard is visible").extracting(CloudEvent::getId).containsExactly("matching");
        assertThat(read.consistencyToken().markers().get(COURSE_C1_KEY).version()).as("The marker of tag course:c1 in the token is at the version the matching event created").isEqualTo(1);
    }

    @Test
    void returns_a_matching_event_whose_commit_was_in_flight_on_the_events_shard_when_the_read_began_through_a_template_whose_session_bound_template_is_not_a_reactive_mongo_template() throws Exception {
        ReactiveMongoTemplate hidingItsSessionTemplate = new ReactiveMongoTemplate(databaseFactory) {
            @Override
            public ReactiveMongoOperations withSession(ClientSession session) {
                ReactiveMongoOperations sessionTemplate = super.withSession(session);
                return (ReactiveMongoOperations) Proxy.newProxyInstance(ReactiveMongoOperations.class.getClassLoader(), new Class<?>[]{ReactiveMongoOperations.class}, (proxy, method, args) -> {
                    try {
                        return method.invoke(sessionTemplate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
            }
        };
        ReactorMongoEventStore store = new ReactorMongoEventStore(hidingItsSessionTemplate, configuration().transactionConfig(transactionManager).build());

        DcbEventStream read = readMatchingEventWhoseCommitIsHeldOnTheEventsShard(store);

        assertThat(read.events()).as("The read returns the matching event, so it ran in the causally consistent session the store opened and not through the template").extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void returns_a_whole_store_read_event_whose_commit_was_in_flight_on_the_events_shard_when_the_read_began() throws Exception {
        DcbConsistencyToken emptyStoreToken = read(DcbCriteria.all()).consistencyToken();
        CompletableFuture<?> matching = null;
        DcbEventStream read;
        try {
            try (Hold hold = cluster.holdCommitsOnSecondShard()) {
                matching = eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1)), failIfEventsMatch(emptyStoreToken)).toFuture();
                awaitMarkerOnTheFirstShard(DcbMarkerVersionsSupport.MATCH_ALL_MARKER_KEY);
                CompletableFuture<DcbEventStream> reading = CompletableFuture.supplyAsync(() -> eventStore.read(DcbCriteria.all()).block(Duration.ofSeconds(10)));
                // Gives the read a fair chance to return or to block on the held commit
                try {
                    reading.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException blockedOnTheHeldCommit) {
                    // Released below
                }
                hold.close();
                read = reading.get(10, TimeUnit.SECONDS);
            }
        } finally {
            // The hold is released by now, so the append still in flight finishes
            if (matching != null) {
                matching.get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(read.events()).as("The read returns the event of the whole-store lock append once its commit on the events shard is visible").extracting(CloudEvent::getId).containsExactly("matching");
        assertThat(read.consistencyToken().markers().get(DcbMarkerVersionsSupport.MATCH_ALL_MARKER_KEY).version()).as("The match-all marker in the token is at the version the whole-store lock append created").isEqualTo(1);
    }

    @Test
    void refuses_an_append_after_a_matching_event_in_the_stores_own_transaction() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1));

        Throwable thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_a_plain_read_in_the_operators_transaction_comes_first() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        Throwable thrown = appendInTheOperatorsTransaction(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1), token);

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void refuses_an_append_after_a_matching_event_when_a_plain_read_in_the_operators_transaction_comes_first() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1));

        Throwable thrown = appendInTheOperatorsTransaction(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1), token);

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_it_joins_a_callers_transaction_at_the_default_read_concern() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        COMMANDS.clear();
        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(transactionManager)
                .transactional(eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))).block());

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it, whatever the caller's transaction reads").isNull();
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("commitTransaction");
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_when_the_store_was_built_from_a_transactional_operator() {
        ReactorMongoEventStore builtFromOperator = new ReactorMongoEventStore(template, configuration().transactionConfig(TransactionalOperator.create(transactionManager)).build());
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        Throwable thrown = catchThrowable(() -> builtFromOperator.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
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

        assertThat(thrown).as("The marker of the tag moved, and the store's own snapshot transaction finds no matching event above it").isNull();
        assertTheCallersTransactionWasNeverRestartedAndEndedWith("commitTransaction");
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
    }

    @Test
    void refuses_an_append_after_a_matching_event_when_it_joins_a_callers_transaction_at_the_default_read_concern() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1));

        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory))
                .transactional(eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))).block());

        assertThat(thrown)
                .as("The marker of the tag moved, and the store's own snapshot transaction finds the matching event above it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void refuses_an_append_after_a_matching_event_that_committed_after_the_callers_transaction_read_the_events_and_before_it_read_the_marker() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();

        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory)).transactional(
                template.exists(new Query(Criteria.where("id").is("none")), "events")
                        // toFuture subscribes without the caller's transaction, so the matching event commits on a transaction of its own
                        .then(Mono.defer(() -> Mono.fromFuture(eventStore.append(List.of(taggedEventWithId("matching", TICKET_SOLD, CONCERT_K1))).toFuture())))
                        .then(Mono.defer(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))))).block());

        assertThat(thrown)
                .as("The caller's transaction read the events before the matching event committed and the marker after it, so the marker moved while its own read of the events misses "
                        + "the matching event. The store's own snapshot transaction finds it")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("matching");
    }

    @Test
    void keeps_a_write_a_transactional_operator_made_before_the_stores_callback_and_sends_no_abortTransaction_when_a_conditional_append_runs_through_a_mongos() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));
        // A collection can't be created inside a distributed transaction
        template.createCollection("hook").block();
        TransactionalOperator delegate = TransactionalOperator.create(transactionManager);
        List<String> hookIds = new CopyOnWriteArrayList<>();
        TransactionalOperator hookedOperator = new TransactionalOperator() {
            @Override
            public <T> Flux<T> execute(TransactionCallback<T> action) {
                return delegate.execute(status -> {
                    String id = UUID.randomUUID().toString();
                    return template.insert(new Document("_id", id), "hook").doOnNext(inserted -> hookIds.add(id)).thenMany(action.doInTransaction(status));
                });
            }
        };
        ReactorMongoEventStore hookedEventStore = new ReactorMongoEventStore(template, configuration().transactionConfig(hookedOperator).build());

        COMMANDS.clear();
        hookedEventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block();

        assertThat(hookIds).as("the transactions the operator wrote in before it delegated").isNotEmpty();
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(Flux.from(client.getDatabase(database).getCollection("hook").find()).map(document -> document.getString("_id")).collectList().block())
                    .as("the documents the operator wrote in its transaction, which the store's check neither aborted nor restarted").containsExactlyInAnyOrderElementsOf(hookIds);
            softly.assertThat(COMMANDS).filteredOn(command -> "insert".equals(command.getFirstKey()) && "hook".equals(command.getString("insert").getValue()))
                    .as("the operator's insert, which was sent in the transaction").isNotEmpty().allMatch(command -> command.containsKey("autocommit"));
            softly.assertThat(COMMANDS).extracting(BsonDocument::getFirstKey).as("the commands of the conditional append").doesNotContain("abortTransaction");
            softly.assertThat(read(TICKETS_OF_K1).events()).extracting(CloudEvent::getId).containsExactly("sold");
        });
    }

    @Test
    void refuses_an_append_that_would_pass_when_the_commit_of_the_snapshot_transaction_fails() {
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();
        append(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1));

        Throwable thrown;
        try (Hold ignored = cluster.failNextCommitOnSecondShard()) {
            thrown = catchThrowable(() -> eventStore.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block());
        }

        assertThat(thrown)
                .as("Nothing matching was appended, so the append passes unless the snapshot transaction fails. The first commit after the fail point is the snapshot transaction's")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).as("the append's event wasn't stored").isEmpty();
    }

    @Test
    void refuses_the_second_of_two_conditional_appends_in_a_callers_transaction_when_the_first_one_moved_their_shared_marker() {
        append(taggedEventWithId("seed", TICKET_SOLD, CONCERT_K1));
        DcbConsistencyToken token = read(TICKETS_OF_K1).consistencyToken();

        Throwable second = TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory)).execute(status -> {
            status.setRollbackOnly();
            return eventStore.append(List.of(taggedEventWithId("first", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))
                    .then(Mono.defer(() -> eventStore.append(List.of(taggedEventWithId("second", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token))
                            .then(Mono.<Throwable>empty())
                            .onErrorResume(Mono::just)));
        }).blockFirst();

        assertThat(second)
                .as("The second append reads the marker at the first one's uncommitted increment, so it moved, while the store's own snapshot shows the version the token saw. "
                        + "The first event matches the criteria and isn't committed, so the snapshot can't find it, and the store refuses an append whose snapshot is behind its own read")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        assertThat(read(TICKETS_OF_K1).events()).as("neither of the caller's events was stored").extracting(CloudEvent::getId).containsExactly("seed");
    }

    @Test
    void commits_the_stores_own_conditional_append_with_the_configured_write_concern_through_a_mongos() {
        List<BsonDocument> commands = recordedConditionalAppendWithConfiguredConcerns().commands();

        BsonDocument commit = single(theAppendsTransaction(commands), "commitTransaction");
        assertThat(commit.getDocument("writeConcern"))
                .as("the commitTransaction of the append, which keeps the write concern the transaction options configured, not majority")
                .isEqualTo(BsonDocument.parse("{\"w\": 1, \"wtimeout\": " + WTIMEOUT_MS + ", \"j\": true}"));
    }

    @Test
    void sends_no_abortTransaction_when_the_stores_own_conditional_append_commits_through_a_mongos() {
        Recorded recorded = recordedConditionalAppendWithConfiguredConcerns();
        List<BsonDocument> commands = recorded.commands();

        single(theAppendsTransaction(commands), "commitTransaction");
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(commands).extracting(BsonDocument::getFirstKey).as("nothing aborts the append's transaction or the store's snapshot transaction").doesNotContain("abortTransaction");
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
        return recordedAppend(TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(CONFIGURED_WRITE_CONCERN).build(), true);
    }

    private Recorded recordedAppend(@Nullable TransactionOptions configured, boolean conditional) {
        List<BsonDocument> recorded = new CopyOnWriteArrayList<>();
        CommandListener recorder = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                recorded.add(event.getCommand().clone());
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString()))
                .writeConcern(WriteConcern.ACKNOWLEDGED).addCommandListener(recorder).build();
        try (MongoClient recordingClient = MongoClients.create(settings)) {
            String recordingDatabase = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
            SimpleReactiveMongoDatabaseFactory factory = new SimpleReactiveMongoDatabaseFactory(recordingClient, recordingDatabase);
            ReactorMongoEventStore store = new ReactorMongoEventStore(new ReactiveMongoTemplate(factory),
                    configuration().transactionConfig(new CommitRetryingReactiveMongoTransactionManager(factory, configured))
                            .markerVersionsCommitTimeout(Duration.ofMillis(MARKER_VERSIONS_COMMIT_TIMEOUT_MS)).build());
            cluster.shardCheckpointCollection(recordingDatabase, "events_dcb_checkpoints");
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).block().consistencyToken();
            store.append(List.of(taggedEventWithId("renamed", CONCERT_RENAMED, CONCERT_K1))).block();
            recorded.clear();

            if (conditional) {
                store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)).block();
            } else {
                store.append(List.of(taggedEventWithId("sold", TICKET_SOLD, CONCERT_K1))).block();
            }

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

    // The starters' path, where a SpringReactiveTransactionExecutor runs a plain command and then the append in a transaction of the store's own transaction manager
    private Throwable appendInTheOperatorsTransaction(CloudEvent event, DcbConsistencyToken token) {
        return catchThrowable(() -> TransactionalOperator.create(transactionManager)
                .transactional(template.exists(new Query(Criteria.where("id").is("none")), "events").then(eventStore.append(List.of(event), failIfEventsMatch(token)))).block());
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

    private static EventStoreConfig.Builder configuration() {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB);
    }

    private DcbEventStream readMatchingEventWhoseCommitIsHeldOnTheEventsShard(ReactorMongoEventStore store) throws Exception {
        CompletableFuture<?> matching = null;
        DcbEventStream read;
        try {
            try (Hold hold = cluster.holdCommitsOnSecondShard()) {
                matching = store.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1))).toFuture();
                awaitMarkerOnTheFirstShard(COURSE_C1_KEY);
                CompletableFuture<DcbEventStream> reading = CompletableFuture.supplyAsync(() -> store.read(ENROLLMENTS_IN_C1).block(Duration.ofSeconds(10)));
                // Gives the read a fair chance to return or to block on the held commit
                try {
                    reading.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException blockedOnTheHeldCommit) {
                    // Released below
                }
                hold.close();
                read = reading.get(10, TimeUnit.SECONDS);
            }
        } finally {
            // The hold is released by now, so the append still in flight finishes
            if (matching != null) {
                matching.get(30, TimeUnit.SECONDS);
            }
        }
        return read;
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
