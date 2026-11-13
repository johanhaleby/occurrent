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

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_EPOCH;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_STAMPED;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_VERSION;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.EVENT_MARKER_VERSIONS;

/**
 * What the reactor store writes to make marker versions work, how it behaves when its settings or its own snapshot
 * transaction change what it can report, and what it reports inside a reactive transaction the caller owns.
 * What a caller sees of marker versions on every store is asserted by
 * {@link org.occurrent.tck.eventstore.blocking.DcbMarkerVersionsConformance}, through a bridge that blocks on the
 * signals of this store.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbMarkerVersionsTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENTS = "events";
    private static final String DEPOSITED = "Deposited";
    private static final String ACCOUNT_A = "account:a1";
    private static final String ACCOUNT_B = "account:b1";
    private static final String KEY_A = "tag:" + ACCOUNT_A;
    private static final String KEY_B = "tag:" + ACCOUNT_B;
    private static final String KEY_DEPOSITED = DcbMarkerVersionsSupport.stripeKey(DEPOSITED, 0);
    private static final String KEY_D = DcbMarkerVersionsSupport.stripeKey("D", 0);
    private static final DcbCriteria BOUNDARY_A = DcbCriteria.tags(Tag.parse(ACCOUNT_A));
    private static final DcbCriteria BOUNDARY_B = DcbCriteria.tags(Tag.parse(ACCOUNT_B));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;
    private SimpleReactiveMongoDatabaseFactory databaseFactory;
    private ReactiveMongoTransactionManager transactionManager;
    private TransactionalOperator callerTransactions;
    private ReactorMongoEventStore eventStore;

    @BeforeEach
    void create_reactor_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".markerversions");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName);
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        callerTransactions = TransactionalOperator.create(transactionManager);
        eventStore = storeOver(config -> {
        });
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void an_append_gives_every_marker_it_touches_an_epoch_and_a_stamped_count_equal_to_its_version() {
        append("a", DEPOSITED, ACCOUNT_A);
        append("b", DEPOSITED, ACCOUNT_A);

        for (String key : List.of(KEY_A, KEY_DEPOSITED)) {
            Document marker = marker(key);
            assertThat(marker.get(CHECKPOINT_EPOCH)).as("epoch of %s", key).isInstanceOf(ObjectId.class);
            assertThat(marker.get(CHECKPOINT_VERSION)).as("version of %s", key).isEqualTo(2L);
            assertThat(marker.get(CHECKPOINT_STAMPED)).as("stamped count of %s", key).isEqualTo(2L);
        }
    }

    @Test
    void a_conditional_append_raises_the_stamped_count_of_the_markers_of_its_criteria_with_their_versions() {
        for (int i = 0; i < 2; i++) {
            eventStore.append(List.of(taggedEvent(UUID.randomUUID().toString(), DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(BOUNDARY_B, eventStore.read(BOUNDARY_B).block().consistencyToken())).block();
        }

        Document queryOnlyMarker = marker(KEY_B);
        assertThat(queryOnlyMarker.get(CHECKPOINT_VERSION))
                .as("the criteria of a conditional append are incremented although its events do not carry the tag")
                .isEqualTo(2L);
        assertThat(queryOnlyMarker.get(CHECKPOINT_STAMPED)).isEqualTo(2L);
        assertThat(queryOnlyMarker.get(CHECKPOINT_EPOCH)).isInstanceOf(ObjectId.class);
        assertThat(marker(KEY_A).get(CHECKPOINT_STAMPED)).isEqualTo(marker(KEY_A).get(CHECKPOINT_VERSION)).isEqualTo(2L);
    }

    @Test
    void an_event_records_the_key_epoch_and_version_of_each_of_its_own_markers() {
        append("a", DEPOSITED, ACCOUNT_A, ACCOUNT_B);

        assertThat(markerVersionsOf("a")).containsExactlyInAnyOrder(
                new Recorded(KEY_A, epochOf(KEY_A), 1),
                new Recorded(KEY_B, epochOf(KEY_B), 1),
                new Recorded(KEY_DEPOSITED, epochOf(KEY_DEPOSITED), 1));
    }

    @Test
    void an_event_records_the_versions_its_own_append_gave_its_markers() {
        append("first", DEPOSITED, ACCOUNT_A);
        append("second", DEPOSITED, ACCOUNT_A);

        assertThat(markerVersionsOf("first")).contains(new Recorded(KEY_A, epochOf(KEY_A), 1));
        assertThat(markerVersionsOf("second")).contains(new Recorded(KEY_A, epochOf(KEY_A), 2));
    }

    @Test
    void an_event_does_not_record_the_markers_that_only_the_criteria_of_its_condition_incremented() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_B, eventStore.read(BOUNDARY_B).block().consistencyToken())).block();

        assertThat(markerVersionsOf("a")).extracting(Recorded::key).containsExactlyInAnyOrder(KEY_A, KEY_DEPOSITED);
    }

    @Test
    void creates_an_index_over_the_marker_versions_of_an_event_when_dcb_is_enabled() {
        assertThat(indexKeysOf(collection(mongoClient, databaseName, EVENTS))).contains(new Document("mv.k", 1).append("mv.e", 1).append("mv.v", 1));
    }

    @Test
    void creates_no_index_over_the_marker_versions_of_an_event_when_dcb_is_not_enabled() {
        String databaseWithoutDcb = "withoutdcb" + UUID.randomUUID().toString().replace("-", "");
        try {
            SimpleReactiveMongoDatabaseFactory factory = new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseWithoutDcb);
            new ReactorMongoEventStore(new ReactiveMongoTemplate(factory), new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS)
                    .transactionConfig(new ReactiveMongoTransactionManager(factory)).timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build());

            assertThat(indexKeysOf(collection(mongoClient, databaseWithoutDcb, EVENTS)))
                    .as("the store has created its indexes, or this proves nothing")
                    .contains(new Document("id", 1).append("source", 1))
                    .doesNotContain(new Document("mv.k", 1).append("mv.e", 1).append("mv.v", 1));
        } finally {
            Mono.from(mongoClient.getDatabase(databaseWithoutDcb).drop()).block();
        }
    }

    @Test
    void gives_a_marker_written_before_marker_versions_an_epoch_on_its_next_increment_and_reports_its_earlier_increments_as_unstamped() {
        Mono.from(markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_A)).append(CHECKPOINT_VERSION, 5L).append("lastPosition", 3L))).block();

        append("a", DEPOSITED, ACCOUNT_A);

        MarkerVersion reported = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow().get(KEY_A).orElseThrow();
        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isInstanceOf(ObjectId.class);
        assertThat(reported.version()).isEqualTo(6);
        assertThat(reported.unstamped()).as("the five increments of the older writer carry no marker versions").isEqualTo(5);
        assertThat(markerVersionsOf("a")).contains(new Recorded(KEY_A, epochOf(KEY_A), 6));
    }

    @Test
    void keeps_the_epoch_the_upgrade_gave_a_marker_through_its_later_increments() {
        Mono.from(markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_A)).append(CHECKPOINT_VERSION, 5L))).block();
        append("a", DEPOSITED, ACCOUNT_A);
        Object epochAfterTheUpgrade = marker(KEY_A).get(CHECKPOINT_EPOCH);

        append("b", DEPOSITED, ACCOUNT_A);

        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isEqualTo(epochAfterTheUpgrade);
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow().get(KEY_A).orElseThrow().unstamped()).isEqualTo(5);
    }

    @Test
    void refuses_a_read_after_versions_when_a_writer_that_wrote_no_marker_versions_incremented_the_marker_after_they_were_saved() {
        append("a", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).block().events())
                .as("the saved versions are resumable until the older writer has incremented, or this proves nothing")
                .isEmpty();

        Mono.from(markers().updateOne(eq("_id", DcbMarkerModel.markerId(KEY_A)), new Document("$inc", new Document(CHECKPOINT_VERSION, 1L)))).block();

        assertThat(errorOf(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_A)
                .hasMessageContaining("writer that wrote no marker versions");
    }

    @Test
    void keeps_the_marker_versions_of_an_event_when_it_is_updated() {
        append("a", DEPOSITED, ACCOUNT_A);
        List<Document> before = rawEvent("a").getList(EVENT_MARKER_VERSIONS, Document.class);

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build()).block();

        assertThat(rawEvent("a").getString("subject")).isEqualTo("rewritten");
        assertThat(rawEvent("a").getList(EVENT_MARKER_VERSIONS, Document.class)).isEqualTo(before);
    }

    @Test
    void keeps_the_marker_versions_of_an_updated_event_whatever_their_bson_type() {
        List<Object> values = Arrays.asList("written by an older version", 7, 7L, 1.5d, true, new Document("k", "tag:x"), List.of(1, 2), null);
        for (Object value : values) {
            String id = UUID.randomUUID().toString();
            append(id, DEPOSITED, ACCOUNT_A);
            Mono.from(events().updateOne(eq("id", id), new Document("$set", new Document(EVENT_MARKER_VERSIONS, value)))).block();

            eventStore.updateEvent(id, SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build()).block();

            Document after = rawEvent(id);
            assertThat(after.getString("subject")).as("the update was applied for %s", value).isEqualTo("rewritten");
            assertThat(after.containsKey(EVENT_MARKER_VERSIONS)).as("mv is kept for %s", value).isTrue();
            assertThat(after.get(EVENT_MARKER_VERSIONS)).as("mv is kept as it was for %s", value).isEqualTo(value);
        }
    }

    @Test
    void reports_no_marker_versions_and_refuses_a_read_after_versions_when_resuming_from_marker_versions_is_turned_off() {
        ReactorMongoEventStore resuming = eventStore;
        ReactorMongoEventStore notResuming = storeOver(config -> config.resumeFromMarkerVersions(false));
        DcbAppendResult appended = notResuming.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)),
                failIfEventsMatch(BOUNDARY_A, notResuming.read(BOUNDARY_A).block().consistencyToken())).block();
        DcbMarkerVersions saved = resuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();

        DcbEventStream read = notResuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block();

        assertThat(appended.markerVersions()).as("the append result").isEmpty();
        assertThat(read.markerVersions()).as("the read").isEmpty();
        assertThat(read.events()).as("the read still returns the events").hasSize(1);
        assertThat(errorOf(notResuming.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)))).isInstanceOf(DcbSnapshotInvalidException.class);
        assertThat(resuming.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).block().events())
                .as("the same versions are accepted by a store that resumes, or the refusal proves nothing")
                .isEmpty();
    }

    @Test
    void writes_the_stamped_count_the_epoch_and_the_marker_versions_of_its_events_when_resuming_from_marker_versions_is_turned_off() {
        ReactorMongoEventStore notResuming = storeOver(config -> config.resumeFromMarkerVersions(false));

        notResuming.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A))).block();

        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isInstanceOf(ObjectId.class);
        assertThat(marker(KEY_A).get(CHECKPOINT_STAMPED)).isEqualTo(1L);
        assertThat(markerVersionsOf("a")).contains(new Recorded(KEY_A, epochOf(KEY_A), 1));
    }

    @Test
    void reports_the_events_of_a_read_that_asks_for_versions_but_no_versions_when_it_cannot_start_a_session() {
        append("a", DEPOSITED, ACCOUNT_A);
        AtomicBoolean failing = new AtomicBoolean();
        ReactorMongoEventStore store = storeWhoseSessionsFailToStartWhen(failing);
        assertThat(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions()).as("reported while sessions start").isPresent();
        failing.set(true);

        DcbEventStream read = store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block();

        assertThat(read.markerVersions()).isEmpty();
        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("a");
    }

    @Test
    void refuses_a_read_after_versions_when_it_cannot_start_a_session() {
        append("a", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        AtomicBoolean failing = new AtomicBoolean();
        ReactorMongoEventStore store = storeWhoseSessionsFailToStartWhen(failing);
        failing.set(true);

        assertThat(errorOf(store.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasRootCauseMessage("injected failure to start a session");
    }

    @Test
    void reports_the_events_of_a_read_that_asks_for_versions_but_no_versions_when_the_commit_of_its_snapshot_fails() {
        append("a", DEPOSITED, ACCOUNT_A);
        AtomicBoolean failing = new AtomicBoolean();
        ReactorMongoEventStore store = storeWhoseSessionsFailToCommitWhen(failing);
        assertThat(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions()).as("reported while commits succeed").isPresent();
        failing.set(true);

        DcbEventStream read = store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block();

        assertThat(read.markerVersions()).isEmpty();
        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("a");
    }

    @Test
    void refuses_a_read_after_versions_when_the_commit_of_its_snapshot_fails() {
        append("a", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        AtomicBoolean failing = new AtomicBoolean();
        ReactorMongoEventStore store = storeWhoseSessionsFailToCommitWhen(failing);
        failing.set(true);

        assertThat(errorOf(store.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasRootCauseMessage("injected failure to commit");
    }

    @Test
    void reports_the_events_of_a_read_inside_a_transaction_the_caller_owns_but_no_marker_versions() {
        append("before", DEPOSITED, ACCOUNT_A);

        DcbEventStream read = inCallerTransaction(() -> eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()));

        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(read.markerVersions()).isEmpty();
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions())
                .as("the same read outside the transaction reports versions, or the empty map proves nothing")
                .isPresent();
    }

    @Test
    void refuses_a_read_after_versions_inside_a_transaction_the_caller_owns() {
        append("before", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();

        Throwable refusal = inCallerTransaction(() -> errorSignalOf(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))));

        assertThat(refusal).isInstanceOf(DcbSnapshotInvalidException.class);
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).block().events())
                .as("the same read outside the transaction is accepted, or the refusal proves nothing")
                .isEmpty();
    }

    @Test
    void reports_no_marker_versions_for_a_read_after_an_append_in_a_transaction_the_caller_owns_that_has_not_committed() {
        append("before", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();

        Observed observed = inCallerTransaction(() -> eventStore.read(BOUNDARY_A)
                .then(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .flatMap(read -> errorSignalOf(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))).map(refusal -> new Observed(read, refusal))));

        assertThat(observed.read().events()).extracting(CloudEvent::getId).containsExactly("before", "own");
        assertThat(observed.read().markerVersions()).as("versions that cover an increment of the transaction would be handed out again if it aborts").isEmpty();
        assertThat(observed.refusal()).isInstanceOf(DcbSnapshotInvalidException.class);
    }

    @Test
    void reads_in_position_order_the_transactions_own_append_inside_a_transaction_the_caller_owns() {
        append("before", DEPOSITED, ACCOUNT_A);

        List<String> replayed = inCallerTransaction(() -> eventStore.read(BOUNDARY_A)
                .then(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))))
                .then(eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()).map(CloudEvent::getId).collectList()));

        assertThat(replayed).containsExactly("before", "own");
    }

    @Test
    void reports_the_marker_versions_of_an_append_inside_a_transaction_the_caller_owns() {
        DcbAppendResult result = inCallerTransaction(() -> eventStore.read(BOUNDARY_A).flatMap(read ->
                eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, read.consistencyToken()))));

        assertThat(result.markerVersions().orElseThrow().keys()).containsExactly(KEY_A);
    }

    @Test
    void does_not_confirm_an_append_made_in_a_transaction_the_caller_owns_until_it_commits() {
        Confirmation confirmation = callerTransactions.transactional(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)))
                .flatMap(result -> eventStore.isConfirmed(result).map(confirmed -> new Confirmation(result, confirmed)))).block();

        assertThat(confirmation.confirmed()).as("while the transaction is open").isFalse();
        assertThat(eventStore.isConfirmed(confirmation.result()).block()).as("after the transaction committed").isTrue();
    }

    @Test
    void does_not_confirm_an_append_made_in_a_transaction_the_caller_owns_that_aborts() {
        DcbAppendResult result = inCallerTransaction(() -> eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))));

        assertThat(eventStore.isConfirmed(result).block()).isFalse();
    }

    @Test
    void reports_marker_versions_for_a_read_that_is_the_first_operation_of_a_transaction_the_caller_opened_and_resumes_from_them_later() {
        append("before", DEPOSITED, ACCOUNT_A);

        DcbEventStream read = inCallerTransaction(() -> eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction()));

        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(read.markerVersions()).as("the versions of a read that is the first operation of the transaction").isPresent();
        append("later", DEPOSITED, ACCOUNT_A);
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(read.markerVersions().orElseThrow())).block().events())
                .extracting(CloudEvent::getId).containsExactly("later");
    }

    @Test
    void counts_the_uncommitted_append_of_the_transaction_for_a_read_that_says_it_is_the_first_operation_of_it_although_an_append_came_first() {
        append("before", DEPOSITED, ACCOUNT_A);

        DcbEventStream read = inCallerTransaction(() -> eventStore.read(BOUNDARY_A)
                .then(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction())));

        assertThat(read.markerVersions()).as("the store reports versions from the transaction the caller opened").isPresent();
        assertThat(read.markerVersions().orElseThrow().get(KEY_A).orElseThrow().version()).as("the read runs in the transaction and not on a session of its own, so it counts the increment of the append").isEqualTo(2);
    }

    @Test
    void reports_marker_versions_for_a_read_that_confirms_an_append_before_saving_in_a_scope_without_a_transaction_and_rolls_back_an_append_in_the_scope() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        Throwable failure = errorOf(inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .doOnNext(read::set)
                .then(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))))
                .then(Mono.error(new IllegalStateException("scope fails after the append")))));

        assertThat(failure).hasMessage("scope fails after the append");
        assertThat(read.get().markerVersions()).isPresent();
        assertThat(eventStore.read(BOUNDARY_A).block().events()).as("the append joined the session transaction the read bound, which rolled back").extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void reports_marker_versions_for_a_read_without_the_confirmation_in_a_scope_without_a_transaction_and_commits_an_append_in_the_scope_on_its_own() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        Throwable failure = errorOf(inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(read::set)
                .then(eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))))
                .then(Mono.error(new IllegalStateException("scope fails after the append")))));

        assertThat(failure).hasMessage("scope fails after the append");
        assertThat(read.get().markerVersions()).isPresent();
        assertThat(eventStore.read(BOUNDARY_A).block().events()).extracting(CloudEvent::getId).containsExactly("before", "own");
    }

    @Test
    void reads_through_the_session_a_first_read_bound_in_a_scope_without_a_transaction_and_reports_no_marker_versions_so_a_second_append_sees_the_first() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> second = new AtomicReference<>();
        DcbReadOptions confirming = DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, confirming)
                .flatMap(first -> eventStore.append(List.of(taggedEvent("first", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, first.consistencyToken())))
                .then(eventStore.read(BOUNDARY_A, confirming))
                .doOnNext(second::set)
                .flatMap(read -> eventStore.append(List.of(taggedEvent("second", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, read.consistencyToken()))))
                .block();

        assertThat(second.get().markerVersions()).as("versions of a read in a session whose writes may still abort").isEmpty();
        assertThat(eventStore.read(BOUNDARY_A).block().events()).extracting(CloudEvent::getId).containsExactly("before", "first", "second");
    }

    @Test
    void reads_a_versioned_read_without_the_confirmation_mark_through_a_session_already_bound_and_reports_no_marker_versions() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(eventStore.append(List.of(taggedEvent("in scope", DEPOSITED, ACCOUNT_A))))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .doOnNext(withoutMark::set))
                .block();

        assertThat(withoutMark.get().markerVersions()).as("versions of a read in a session whose writes may still abort").isEmpty();
        assertThat(withoutMark.get().events()).as("a read through the bound session sees its append").extracting(CloudEvent::getId).containsExactly("before", "in scope");
    }

    @Test
    void appends_conditioned_on_a_versioned_read_without_the_confirmation_mark_in_a_session_already_bound_after_an_append_in_that_session() {
        append("before", DEPOSITED, ACCOUNT_A);

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(eventStore.append(List.of(taggedEvent("in scope", DEPOSITED, ACCOUNT_A))))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .flatMap(read -> eventStore.append(List.of(taggedEvent("after read", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, read.consistencyToken()))))
                .block();

        assertThat(eventStore.read(BOUNDARY_A).block().events()).extracting(CloudEvent::getId).containsExactly("before", "in scope", "after read");
    }

    @Test
    void reports_marker_versions_for_a_read_without_the_confirmation_mark_in_a_session_a_plain_read_bound_that_the_store_has_not_written_in() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();
        AtomicReference<Object> sessionBoundByPlainRead = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(manager -> Optional.ofNullable(manager.getResource(databaseFactory))))
                .doOnNext(session -> sessionBoundByPlainRead.set(session.orElse(null)))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .doOnNext(withoutMark::set))
                .block();

        assertThat(sessionBoundByPlainRead.get()).as("session bound for the store's database factory by the plain read, or this proves nothing about a bound session").isNotNull();
        assertThat(withoutMark.get().markerVersions()).as("versions of a read in a session the store never wrote in").isNotEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void refuses_a_read_after_versions_in_a_session_bound_to_the_synchronization_that_the_store_wrote_in() {
        append("before", DEPOSITED, ACCOUNT_A);
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        AtomicReference<Throwable> refusal = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(eventStore.append(List.of(taggedEvent("in scope", DEPOSITED, ACCOUNT_A))))
                .then(errorSignalOf(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .doOnNext(refusal::set))
                .block();

        assertThat(refusal.get()).as("what the read after versions failed with in the session the store wrote in").isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining("that the event store wrote in");
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).block().events())
                .as("the same read outside the scope is accepted, or the refusal proves nothing")
                .extracting(CloudEvent::getId).containsExactly("in scope");
    }

    @Test
    void reports_no_marker_versions_for_a_read_with_the_confirmation_mark_in_a_session_a_plain_read_bound_that_the_store_has_not_written_in() {
        // Passes with or without the record of the store's writes, since a read with the mark gets no versions in any bound session
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> withMark = new AtomicReference<>();
        AtomicReference<Object> sessionBoundByPlainRead = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(manager -> Optional.ofNullable(manager.getResource(databaseFactory))))
                .doOnNext(session -> sessionBoundByPlainRead.set(session.orElse(null)))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving()))
                .doOnNext(withMark::set))
                .block();

        assertThat(sessionBoundByPlainRead.get()).as("session bound for the store's database factory by the plain read, or this proves nothing about a bound session").isNotNull();
        assertThat(withMark.get().markerVersions()).as("versions of a read with the mark in a bound session").isEmpty();
        assertThat(withMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void reports_no_marker_versions_for_a_read_without_the_confirmation_mark_in_a_session_bound_to_the_synchronization_after_the_store_deleted_through_the_template() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A), taggedEvent("unrelated", DEPOSITED, ACCOUNT_B))).block();
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.delete(Filter.id("unrelated"))
                .then(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .doOnNext(withoutMark::set))
                .block();

        assertThat(withoutMark.get().markerVersions()).as("versions of a read in a session the store deleted in").isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(eventStore.read(BOUNDARY_B).block().events()).as("the delete went through").isEmpty();
    }

    @Test
    void reads_a_versioned_read_without_the_confirmation_mark_without_binding_a_session_to_the_synchronization() {
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(withoutMark::set)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set))
                .block();

        assertThat(withoutMark.get().markerVersions()).isNotEmpty();
        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isEmpty();
    }

    @Test
    void reads_a_versioned_read_without_the_confirmation_mark_without_binding_a_session_to_the_synchronization_when_the_commit_of_its_snapshot_fails() {
        // A read without versions is a full read the caller applies from the initial state, so the fallback loses nothing. Binding no session
        // keeps a later append in the scope in a transaction of the store's own, which the store retries and which commits on its
        // own, rather than in a session transaction the scope commits or drops.
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicBoolean failing = new AtomicBoolean(true);
        ReactorMongoEventStore store = storeWhoseSessionsFailToCommitWhen(failing);
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(withoutMark::set)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set)
                .doOnNext(registered -> failing.set(false)))
                .block();

        assertThat(withoutMark.get().markerVersions()).as("the snapshot failed to commit, or this proves nothing about the fallback").isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isEmpty();
    }

    @Test
    void reads_a_versioned_read_without_the_confirmation_mark_without_binding_a_session_to_the_synchronization_when_resuming_from_marker_versions_is_turned_off() {
        // A read without versions is a full read the caller applies from the initial state, so the fallback loses nothing. Binding no session
        // keeps a later append in the scope in a transaction of the store's own, which the store retries and which commits on its
        // own, rather than in a session transaction the scope commits or drops.
        ReactorMongoEventStore notResuming = storeOver(config -> config.resumeFromMarkerVersions(false));
        notResuming.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A))).block();
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(notResuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(withoutMark::set)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set))
                .block();

        assertThat(withoutMark.get().markerVersions()).isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isEmpty();
    }

    @Test
    void reads_a_versioned_read_without_the_confirmation_mark_without_binding_a_session_to_the_synchronization_for_match_all_criteria() {
        // A read without versions is a full read the caller applies from the initial state, so the fallback loses nothing. Binding no session
        // keeps a later append in the scope in a transaction of the store's own, which the store retries and which commits on its
        // own, rather than in a session transaction the scope commits or drops.
        append("before", DEPOSITED, ACCOUNT_A);
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(DcbCriteria.all(), DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(withoutMark::set)
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set))
                .block();

        assertThat(withoutMark.get().markerVersions()).isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isEmpty();
    }

    @Test
    void reports_no_marker_versions_for_a_read_without_the_confirmation_mark_in_a_session_bound_to_the_synchronization_after_a_store_delete_was_cancelled() {
        // The delete has run in the session the template bound when the signal fires, and the cancel then ends it before it completes
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A), taggedEvent("unrelated", DEPOSITED, ACCOUNT_B))).block();
        Sinks.Empty<Void> deleted = Sinks.empty();
        ReactiveMongoTemplate signalling = new ReactiveMongoTemplate(databaseFactory) {
            @Override
            public Mono<DeleteResult> remove(Query query, String collectionName) {
                return super.remove(query, collectionName).flatMap(result -> {
                    deleted.tryEmitEmpty();
                    return Mono.<DeleteResult>never();
                });
            }
        };
        ReactorMongoEventStore store = storeOver(signalling, config -> {
        });
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();

        inScopeWithoutTransaction(store.delete(Filter.id("unrelated")).takeUntilOther(deleted.asMono())
                .then(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .doOnNext(withoutMark::set))
                .block(Duration.ofSeconds(30));

        assertThat(withoutMark.get().markerVersions()).as("versions of a read in a session the store cancelled a delete in").isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(eventStore.query(Filter.id("unrelated")).collectList().block()).as("the delete ran before the cancel, or this proves nothing about a write").isEmpty();
    }

    @Test
    void reports_no_marker_versions_for_a_read_without_the_confirmation_mark_in_a_session_bound_to_the_synchronization_after_a_dcb_append_was_cancelled() {
        // The insert has run in the session the template bound when the signal fires, and the cancel then ends the append before it completes
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A))).block();
        Sinks.Empty<Void> inserted = Sinks.empty();
        ReactiveMongoTemplate signalling = new ReactiveMongoTemplate(databaseFactory) {
            @Override
            public <T> Flux<T> insert(Collection<? extends T> batchToSave, String collectionName) {
                return super.insert(batchToSave, collectionName).collectList().flatMapMany(result -> {
                    inserted.tryEmitEmpty();
                    return Flux.<T>never();
                });
            }
        };
        ReactorMongoEventStore store = storeOver(signalling, config -> {
        });
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();

        inScopeWithoutTransaction(store.read(BOUNDARY_A)
                .then(store.append(List.of(taggedEvent("cancelled", DEPOSITED, ACCOUNT_A))).takeUntilOther(inserted.asMono()))
                .then(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()))
                .doOnNext(withoutMark::set))
                .block(Duration.ofSeconds(30));

        assertThat(withoutMark.get().markerVersions()).as("versions of a read in a session the store cancelled an append in").isEmpty();
        assertThat(withoutMark.get().events()).as("the read sees the insert, so it ran in the session the read uses, or this proves nothing about a write")
                .extracting(CloudEvent::getId).containsExactly("before", "cancelled");
    }

    @Test
    void reads_the_events_of_a_versioned_read_without_the_confirmation_mark_but_no_marker_versions_when_the_session_bound_template_is_not_a_reactive_mongo_template() {
        // The wrapper hides the ReactiveMongoTemplate type, so the store can't keep the read out of the synchronization and reads through its template
        // That read may bind a session but still returns every event, which beats failing the read
        ReactiveMongoTemplate wrapping = new ReactiveMongoTemplate(databaseFactory) {
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
        ReactorMongoEventStore store = storeOver(wrapping, config -> config.resumeFromMarkerVersions(false));
        store.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A))).block();
        AtomicReference<DcbEventStream> withoutMark = new AtomicReference<>();

        Throwable thrown = errorOf(inScopeWithoutTransaction(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .doOnNext(withoutMark::set)));

        assertThat(thrown).as("what the read failed with").isNull();
        // Checks that the fallback through the template reports no versions
        assertThat(withoutMark.get().markerVersions()).isEmpty();
        assertThat(withoutMark.get().events()).extracting(CloudEvent::getId).containsExactly("before");
    }

    @Test
    void keeps_an_append_after_a_versioned_read_without_the_confirmation_mark_when_the_scope_without_a_transaction_then_fails() {
        // A read without versions is a full read the caller applies from the initial state, so the fallback loses nothing. Binding no session
        // keeps a later append in the scope in a transaction of the store's own, which the store retries and which commits on its
        // own, rather than in a session transaction the scope commits or drops.
        ReactorMongoEventStore notResuming = storeOver(config -> config.resumeFromMarkerVersions(false));

        Throwable thrown = errorOf(inScopeWithoutTransaction(notResuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions())
                .then(notResuming.append(List.of(taggedEvent("in scope", DEPOSITED, ACCOUNT_A))))
                .then(Mono.error(new IllegalStateException("scope fails")))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("scope fails");
        assertThat(notResuming.read(BOUNDARY_A).block().events()).as("the append ran in the store's own transaction").extracting(CloudEvent::getId).containsExactly("in scope");
    }

    @Test
    void binds_a_session_to_the_synchronization_for_a_read_with_the_confirmation_mark_when_resuming_from_marker_versions_is_turned_off() {
        ReactorMongoEventStore notResuming = storeOver(config -> config.resumeFromMarkerVersions(false));
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(notResuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set))
                .block();

        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isNotEmpty();
    }

    @Test
    void binds_a_session_to_the_synchronization_for_a_read_with_the_confirmation_mark_for_match_all_criteria() {
        AtomicReference<List<TransactionSynchronization>> registeredAfterRead = new AtomicReference<>();

        inScopeWithoutTransaction(eventStore.read(DcbCriteria.all(), DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving())
                .then(TransactionSynchronizationManager.forCurrentTransaction().map(TransactionSynchronizationManager::getSynchronizations))
                .doOnNext(registeredAfterRead::set))
                .block();

        assertThat(registeredAfterRead.get()).as("synchronizations registered for a session bound by the read").isNotEmpty();
    }

    @Test
    void fails_an_append_that_conflicts_in_a_session_bound_in_a_scope_without_a_transaction_at_once_instead_of_retrying_until_the_deadline() {
        append("before", DEPOSITED, ACCOUNT_A);
        DcbReadOptions confirming = DcbReadOptions.fromBeginning().withVersions().confirmingAppendBeforeSaving();
        long startedAt = System.nanoTime();

        // toFuture subscribes without the scope's context, so the concurrent append runs outside the bound session
        Throwable thrown = errorOf(inScopeWithoutTransaction(eventStore.read(BOUNDARY_A, confirming)
                .flatMap(read -> Mono.fromFuture(() -> eventStore.append(List.of(taggedEvent("concurrent", DEPOSITED, ACCOUNT_A))).toFuture())
                        .then(eventStore.append(List.of(taggedEvent("in scope", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, read.consistencyToken()))))));

        assertThat(thrown).as("the append in the bound session conflicts with the concurrent one").isNotNull();
        assertThat(Stream.iterate(thrown, Objects::nonNull, Throwable::getCause)).as("a transient conflict, not a condition failure, in the cause chain")
                .anySatisfy(cause -> assertThat(cause).isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)).isTrue()));
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).as("a retry in an aborted transaction runs until its deadline").isLessThan(Duration.ofSeconds(30));
        assertThat(eventStore.read(BOUNDARY_A).block().events()).extracting(CloudEvent::getId).containsExactly("before", "concurrent");
    }

    @Test
    void adds_no_marker_versions_extension_to_the_events_a_query_returns() {
        append("a", DEPOSITED, ACCOUNT_A);
        assertThat(rawEvent("a").containsKey(EVENT_MARKER_VERSIONS)).as("the stored event has marker versions, or this proves nothing").isTrue();

        List<CloudEvent> queried = eventStore.query(Filter.all()).collectList().block();

        assertThat(queried).hasSize(1);
        assertThat(queried.getFirst().getExtensionNames()).doesNotContain(EVENT_MARKER_VERSIONS);
    }

    @Test
    void writes_only_the_first_stripe_of_a_type_when_type_marker_writes_are_not_spread() {
        for (int i = 0; i < 64; i++) {
            eventStore.append(List.of(taggedEvent("e" + i, DEPOSITED, "account:s" + i))).block();
        }

        assertThat(existingStripes(DEPOSITED)).containsExactly(0);
        assertThat(marker(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, 0)).get(CHECKPOINT_VERSION)).isEqualTo(64L);
        for (int i = 0; i < 64; i++) {
            assertThat(markerVersionsOf("e" + i)).extracting(Recorded::key).contains(KEY_DEPOSITED).doesNotContain(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, 1));
        }
    }

    @Test
    void writes_more_than_one_stripe_of_a_type_when_type_marker_writes_are_spread() {
        ReactorMongoEventStore spreading = storeOver(config -> config.spreadTypeMarkerWrites(true));
        for (int i = 0; i < 64; i++) {
            spreading.append(List.of(taggedEvent("e" + i, DEPOSITED, "account:s" + i))).block();
        }

        assertThat(existingStripes(DEPOSITED)).hasSizeGreaterThan(1);
        long total = existingStripes(DEPOSITED).stream().mapToLong(stripe -> marker(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, stripe)).getLong(CHECKPOINT_VERSION)).sum();
        assertThat(total).as("every append incremented exactly one stripe of its type").isEqualTo(64L);
        for (int i = 0; i < 64; i++) {
            List<String> typeKeys = markerVersionsOf("e" + i).stream().map(Recorded::key).filter(DcbMarkerVersionsSupport::isTypeKey).toList();
            assertThat(typeKeys).as("the type keys of event e%d", i).hasSize(1);
        }
    }

    @Test
    void increments_every_stripe_of_a_type_for_a_type_only_conditional_append_when_type_marker_writes_are_not_spread() {
        typeOnlyConditionalAppendIncrementsEveryStripe(eventStore);
    }

    @Test
    void increments_every_stripe_of_a_type_for_a_type_only_conditional_append_when_type_marker_writes_are_spread() {
        typeOnlyConditionalAppendIncrementsEveryStripe(storeOver(config -> config.spreadTypeMarkerWrites(true)));
    }

    private void typeOnlyConditionalAppendIncrementsEveryStripe(ReactorMongoEventStore store) {
        DcbCriteria criteria = DcbCriteria.types(DEPOSITED);
        store.append(List.of(taggedEvent("a", DEPOSITED)), failIfEventsMatch(criteria, store.read(criteria).block().consistencyToken())).block();

        assertThat(existingStripes(DEPOSITED)).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        for (int stripe = 0; stripe < DcbMarkerVersionsSupport.STRIPES; stripe++) {
            assertThat(marker(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, stripe)).get(CHECKPOINT_VERSION)).as("version of stripe %d", stripe).isEqualTo(1L);
        }
    }

    @Test
    void refuses_a_conditional_append_whose_unconditional_competitor_spread_its_type_marker_write_when_the_reader_does_not_spread() {
        conflictingAppendIsRefused(false, true);
    }

    @Test
    void refuses_a_conditional_append_whose_unconditional_competitor_wrote_the_first_stripe_when_the_reader_spreads() {
        conflictingAppendIsRefused(true, false);
    }

    @Test
    void refuses_a_conditional_append_whose_unconditional_competitor_spread_its_type_marker_write_when_the_reader_spreads() {
        conflictingAppendIsRefused(true, true);
    }

    @Test
    void accepts_a_conditional_append_when_the_unconditional_competitor_appended_another_type_whatever_the_settings() {
        for (boolean[] settings : new boolean[][]{{false, true}, {true, false}}) {
            ReactorMongoEventStore reader = storeOver(config -> config.spreadTypeMarkerWrites(settings[0]));
            ReactorMongoEventStore writer = storeOver(config -> config.spreadTypeMarkerWrites(settings[1]));
            for (int i = 0; i < 32; i++) {
                String type = "Control" + UUID.randomUUID();
                DcbCriteria criteria = DcbCriteria.types(type);
                DcbConsistencyToken token = reader.read(criteria).block().consistencyToken();
                writer.append(List.of(taggedEvent(UUID.randomUUID().toString(), "Other" + UUID.randomUUID()))).block();

                assertThatCode(() -> reader.append(List.of(taggedEvent(UUID.randomUUID().toString(), type)), failIfEventsMatch(criteria, token)).block()).doesNotThrowAnyException();
            }
        }
    }

    private void conflictingAppendIsRefused(boolean readerSpreads, boolean writerSpreads) {
        ReactorMongoEventStore reader = storeOver(config -> config.spreadTypeMarkerWrites(readerSpreads));
        ReactorMongoEventStore writer = storeOver(config -> config.spreadTypeMarkerWrites(writerSpreads));
        for (int i = 0; i < 32; i++) {
            String type = "Mixed" + UUID.randomUUID();
            DcbCriteria criteria = DcbCriteria.types(type);
            DcbConsistencyToken token = reader.read(criteria).block().consistencyToken();
            writer.append(List.of(taggedEvent(UUID.randomUUID().toString(), type))).block();

            assertThat(errorOf(reader.append(List.of(taggedEvent(UUID.randomUUID().toString(), type)), failIfEventsMatch(criteria, token))))
                    .as("the append %d of the type %s after a competitor that wrote it", i, type)
                    .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }
    }

    @Test
    void a_read_after_versions_saved_by_a_store_that_does_not_spread_returns_the_events_a_store_that_spreads_appended() {
        resumedAfterACompetitorAppendedUnconditionally(false, true);
    }

    @Test
    void a_read_after_versions_saved_by_a_store_that_spreads_returns_the_events_a_store_that_does_not_spread_appended() {
        resumedAfterACompetitorAppendedUnconditionally(true, false);
    }

    private void resumedAfterACompetitorAppendedUnconditionally(boolean readerSpreads, boolean writerSpreads) {
        ReactorMongoEventStore reader = storeOver(config -> config.spreadTypeMarkerWrites(readerSpreads));
        ReactorMongoEventStore writer = storeOver(config -> config.spreadTypeMarkerWrites(writerSpreads));
        DcbCriteria criteria = DcbCriteria.types("D");
        reader.append(List.of(taggedEvent("first", "D"))).block();
        DcbMarkerVersions saved = reader.read(criteria, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 32; i++) {
            ids.add("later" + i);
            writer.append(List.of(taggedEvent("later" + i, "D"))).block();
        }

        List<String> read = reader.read(criteria, DcbReadOptions.afterVersions(saved)).block().events().stream().map(CloudEvent::getId).toList();

        assertThat(read).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void refuses_a_type_only_conditional_append_after_a_writer_that_wrote_no_marker_versions_incremented_the_type_marker_when_type_marker_writes_are_not_spread() {
        refusesAfterAnOlderWriterIncrementedTheTypeMarker(eventStore);
    }

    @Test
    void refuses_a_type_only_conditional_append_after_a_writer_that_wrote_no_marker_versions_incremented_the_type_marker_when_type_marker_writes_are_spread() {
        refusesAfterAnOlderWriterIncrementedTheTypeMarker(storeOver(config -> config.spreadTypeMarkerWrites(true)));
    }

    private void refusesAfterAnOlderWriterIncrementedTheTypeMarker(ReactorMongoEventStore store) {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbConsistencyToken token = store.read(criteria).block().consistencyToken();

        Mono.from(markers().updateOne(eq("_id", DcbMarkerModel.markerId(KEY_D)), new Document("$inc", new Document(CHECKPOINT_VERSION, 1L)), new UpdateOptions().upsert(true))).block();

        assertThat(errorOf(store.append(List.of(taggedEvent("a", "D")), failIfEventsMatch(criteria, token))))
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_a_read_after_versions_with_an_absent_type_stripe_that_an_older_writer_has_incremented_since() {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbMarkerVersions saved = eventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        Mono.from(markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_D)).append(CHECKPOINT_VERSION, 2L).append(CHECKPOINT_STAMPED, 1L).append(CHECKPOINT_EPOCH, new ObjectId()))).block();

        assertThat(errorOf(eventStore.read(criteria, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_D)
                .hasMessageContaining("writer that wrote no marker versions");
    }

    @Test
    void refuses_a_read_after_versions_with_an_absent_type_stripe_whose_marker_has_no_epoch_now() {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbMarkerVersions saved = eventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions()).block().markerVersions().orElseThrow();
        Mono.from(markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_D)).append(CHECKPOINT_VERSION, 1L))).block();

        assertThat(errorOf(eventStore.read(criteria, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_D)
                .hasMessageContaining("has no epoch");
    }

    private List<Integer> existingStripes(String type) {
        return java.util.stream.IntStream.range(0, DcbMarkerVersionsSupport.STRIPES)
                .filter(stripe -> Mono.from(markers().countDocuments(eq("_id", DcbMarkerModel.markerId(DcbMarkerVersionsSupport.stripeKey(type, stripe))))).block() > 0)
                .boxed().toList();
    }

    private <T> Mono<T> inScopeWithoutTransaction(Mono<T> body) {
        return TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_SUPPORTS)).transactional(body);
    }

    private <T> T inCallerTransaction(Supplier<Mono<T>> body) {
        return callerTransactions.execute(status -> {
            status.setRollbackOnly();
            return body.get();
        }).single().block();
    }

    private DcbAppendResult append(String id, String type, String... tags) {
        return eventStore.append(List.of(taggedEvent(id, type, tags))).block();
    }

    private static Throwable errorOf(Mono<?> mono) {
        return catchThrowable(mono::block);
    }

    private static Mono<Throwable> errorSignalOf(Mono<?> mono) {
        return mono.then(Mono.<Throwable>error(new AssertionError("Expected an error signal, but the read completed")))
                .onErrorResume(error -> error instanceof AssertionError ? Mono.error(error) : Mono.just(error));
    }

    private ReactorMongoEventStore storeOver(Consumer<EventStoreConfig.Builder> configure) {
        return storeOver(new ReactiveMongoTemplate(databaseFactory), configure);
    }

    private ReactorMongoEventStore storeOver(ReactiveMongoTemplate template, Consumer<EventStoreConfig.Builder> configure) {
        EventStoreConfig.Builder builder = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENTS)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB);
        configure.accept(builder);
        return new ReactorMongoEventStore(template, builder.build());
    }

    private ReactorMongoEventStore storeWhoseSessionsFailToStartWhen(AtomicBoolean failing) {
        SimpleReactiveMongoDatabaseFactory injecting = new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName) {
            @Override
            public Mono<ClientSession> getSession(ClientSessionOptions options) {
                return failing.get() ? Mono.error(new MongoException("injected failure to start a session")) : super.getSession(options);
            }
        };
        return storeOver(new ReactiveMongoTemplate(injecting), config -> {
        });
    }

    private ReactorMongoEventStore storeWhoseSessionsFailToCommitWhen(AtomicBoolean failing) {
        SimpleReactiveMongoDatabaseFactory injecting = new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName) {
            @Override
            public Mono<ClientSession> getSession(ClientSessionOptions options) {
                return super.getSession(options).map(session -> failingToCommitWhen(session, failing));
            }
        };
        return storeOver(new ReactiveMongoTemplate(injecting), config -> {
        });
    }

    private static ClientSession failingToCommitWhen(ClientSession session, AtomicBoolean failing) {
        return (ClientSession) Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[]{ClientSession.class}, (proxy, method, args) -> {
            if (method.getName().equals("commitTransaction") && failing.get()) {
                return Mono.error(new MongoException("injected failure to commit"));
            }
            try {
                return method.invoke(session, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private MongoCollection<Document> events() {
        return collection(mongoClient, databaseName, EVENTS);
    }

    private MongoCollection<Document> markers() {
        return collection(mongoClient, databaseName, DcbMarkerModel.checkpointCollectionName(EVENTS));
    }

    private static MongoCollection<Document> collection(MongoClient client, String database, String name) {
        return client.getDatabase(database).getCollection(name);
    }

    private Document marker(String key) {
        return requireNonNull(Mono.from(markers().find(eq("_id", DcbMarkerModel.markerId(key))).first()).block(), "No marker " + key);
    }

    private String epochOf(String key) {
        return marker(key).getObjectId(CHECKPOINT_EPOCH).toHexString();
    }

    private Document rawEvent(String id) {
        return requireNonNull(Mono.from(events().find(eq("id", id)).first()).block(), "No event " + id);
    }

    private List<Recorded> markerVersionsOf(String id) {
        return rawEvent(id).getList(EVENT_MARKER_VERSIONS, Document.class).stream()
                .map(element -> new Recorded(element.getString("k"), element.getObjectId("e").toHexString(), ((Number) element.get("v")).longValue()))
                .toList();
    }

    private static List<Document> indexKeysOf(MongoCollection<Document> collection) {
        return reactor.core.publisher.Flux.from(collection.listIndexes()).map(index -> index.get("key", Document.class)).collectList().block();
    }

    private record Recorded(String key, String epoch, long version) {
    }

    private record Observed(DcbEventStream read, Throwable refusal) {
    }

    private record Confirmation(DcbAppendResult result, boolean confirmed) {
    }

    private static CloudEvent taggedEvent(String id, String type, String... tags) {
        return DcbCloudEvents.withTags(CloudEventBuilder.v1()
                .withId(id)
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build(), Arrays.stream(tags).map(Tag::parse).toList());
    }
}
