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
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
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
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
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
import org.occurrent.tck.ConcurrentRendezvous;
import org.occurrent.tck.ConcurrentRendezvous.Outcome;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.HoldingMongoClient;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_EPOCH;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_STAMPED;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.CHECKPOINT_VERSION;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.EVENT_MARKER_VERSIONS;

/**
 * What the native store writes to make marker versions work, how it behaves when its settings or its own snapshot
 * transaction change what it can report, and what it reports inside a transaction the caller owns. What a caller
 * sees of marker versions on every store is asserted by
 * {@link org.occurrent.tck.eventstore.blocking.DcbMarkerVersionsConformance}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbMarkerVersionsTest {

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
    private MongoDatabase database;
    private MongoEventStore eventStore;

    @BeforeEach
    void create_mongo_native_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".markerversions");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventStore = storeOver(mongoClient, config -> {
        });
    }

    @AfterEach
    void close_mongo_client() {
        ClientSessionHolder.remove();
        mongoClient.close();
    }

    @Test
    void an_append_gives_every_marker_it_touches_an_epoch_and_a_stamped_count_equal_to_its_version() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        eventStore.append(List.of(taggedEvent("b", DEPOSITED, ACCOUNT_A)));

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
                    failIfEventsMatch(BOUNDARY_B, eventStore.read(BOUNDARY_B).consistencyToken()));
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
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A, ACCOUNT_B)));

        assertThat(markerVersionsOf("a")).containsExactlyInAnyOrder(
                new Recorded(KEY_A, epochOf(KEY_A), 1),
                new Recorded(KEY_B, epochOf(KEY_B), 1),
                new Recorded(KEY_DEPOSITED, epochOf(KEY_DEPOSITED), 1));
    }

    @Test
    void an_event_records_the_versions_its_own_append_gave_its_markers() {
        eventStore.append(List.of(taggedEvent("first", DEPOSITED, ACCOUNT_A)));
        eventStore.append(List.of(taggedEvent("second", DEPOSITED, ACCOUNT_A)));

        assertThat(markerVersionsOf("first")).contains(new Recorded(KEY_A, epochOf(KEY_A), 1));
        assertThat(markerVersionsOf("second")).contains(new Recorded(KEY_A, epochOf(KEY_A), 2));
    }

    @Test
    void an_event_does_not_record_the_markers_that_only_the_criteria_of_its_condition_incremented() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_B, eventStore.read(BOUNDARY_B).consistencyToken()));

        assertThat(markerVersionsOf("a")).extracting(Recorded::key).containsExactlyInAnyOrder(KEY_A, KEY_DEPOSITED);
    }

    @Test
    void creates_an_index_over_the_marker_versions_of_an_event_when_dcb_is_enabled() {
        assertThat(indexKeysOf(database.getCollection(EVENTS))).contains(new Document("mv.k", 1).append("mv.e", 1).append("mv.v", 1));
    }

    @Test
    void creates_no_index_over_the_marker_versions_of_an_event_when_dcb_is_not_enabled() {
        String databaseWithoutDcb = "withoutdcb" + UUID.randomUUID().toString().replace("-", "");
        try {
            new MongoEventStore(mongoClient, databaseWithoutDcb, EVENTS, new EventStoreConfig.Builder().timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build());

            assertThat(indexKeysOf(mongoClient.getDatabase(databaseWithoutDcb).getCollection(EVENTS)))
                    .as("the store has created its indexes, or this proves nothing")
                    .contains(new Document("id", 1).append("source", 1))
                    .doesNotContain(new Document("mv.k", 1).append("mv.e", 1).append("mv.v", 1));
        } finally {
            mongoClient.getDatabase(databaseWithoutDcb).drop();
        }
    }

    @Test
    void gives_a_marker_written_before_marker_versions_an_epoch_on_its_next_increment_and_reports_its_earlier_increments_as_unstamped() {
        markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_A)).append(CHECKPOINT_VERSION, 5L).append("lastPosition", 3L));

        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));

        MarkerVersion reported = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow().get(KEY_A).orElseThrow();
        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isInstanceOf(ObjectId.class);
        assertThat(reported.version()).isEqualTo(6);
        assertThat(reported.unstamped()).as("the five increments of the older writer carry no marker versions").isEqualTo(5);
        assertThat(markerVersionsOf("a")).contains(new Recorded(KEY_A, epochOf(KEY_A), 6));
    }

    @Test
    void keeps_the_epoch_the_upgrade_gave_a_marker_through_its_later_increments() {
        markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_A)).append(CHECKPOINT_VERSION, 5L));
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        Object epochAfterTheUpgrade = marker(KEY_A).get(CHECKPOINT_EPOCH);

        eventStore.append(List.of(taggedEvent("b", DEPOSITED, ACCOUNT_A)));

        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isEqualTo(epochAfterTheUpgrade);
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow().get(KEY_A).orElseThrow().unstamped()).isEqualTo(5);
    }

    @Test
    void refuses_a_read_after_versions_when_a_writer_that_wrote_no_marker_versions_incremented_the_marker_after_they_were_saved() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).events())
                .as("the saved versions are resumable until the older writer has incremented, or this proves nothing")
                .isEmpty();

        markers().updateOne(eq("_id", DcbMarkerModel.markerId(KEY_A)), new Document("$inc", new Document(CHECKPOINT_VERSION, 1L)));

        assertThatThrownBy(() -> eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_A)
                .hasMessageContaining("writer that wrote no marker versions");
    }

    @Test
    void keeps_the_marker_versions_of_an_event_when_it_is_updated() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        List<Document> before = rawEvent("a").getList(EVENT_MARKER_VERSIONS, Document.class);

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        assertThat(rawEvent("a").getString("subject")).isEqualTo("rewritten");
        assertThat(rawEvent("a").getList(EVENT_MARKER_VERSIONS, Document.class)).isEqualTo(before);
    }

    @Test
    void keeps_the_marker_versions_of_an_updated_event_whatever_their_bson_type() {
        List<Object> values = Arrays.asList("written by an older version", 7, 7L, 1.5d, true, new Document("k", "tag:x"), List.of(1, 2), null);
        for (Object value : values) {
            String id = UUID.randomUUID().toString();
            eventStore.append(List.of(taggedEvent(id, DEPOSITED, ACCOUNT_A)));
            events().updateOne(eq("id", id), new Document("$set", new Document(EVENT_MARKER_VERSIONS, value)));

            eventStore.updateEvent(id, SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

            Document after = rawEvent(id);
            assertThat(after.getString("subject")).as("the update was applied for %s", value).isEqualTo("rewritten");
            assertThat(after.containsKey(EVENT_MARKER_VERSIONS)).as("mv is kept for %s", value).isTrue();
            assertThat(after.get(EVENT_MARKER_VERSIONS)).as("mv is kept as it was for %s", value).isEqualTo(value);
        }
    }

    @Test
    void reports_no_marker_versions_and_refuses_a_read_after_versions_when_resuming_from_marker_versions_is_turned_off() {
        MongoEventStore resuming = eventStore;
        MongoEventStore notResuming = storeOver(mongoClient, config -> config.resumeFromMarkerVersions(false));
        DcbAppendResult appended = notResuming.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, notResuming.read(BOUNDARY_A).consistencyToken()));
        DcbMarkerVersions saved = resuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();

        DcbEventStream read = notResuming.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions());

        assertThat(appended.markerVersions()).as("the append result").isEmpty();
        assertThat(read.markerVersions()).as("the read").isEmpty();
        assertThat(read.events()).as("the read still returns the events").hasSize(1);
        assertThat(catchThrowable(() -> notResuming.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class);
        assertThat(resuming.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).events())
                .as("the same versions are accepted by a store that resumes, or the refusal proves nothing")
                .isEmpty();
    }

    @Test
    void writes_the_stamped_count_the_epoch_and_the_marker_versions_of_its_events_when_resuming_from_marker_versions_is_turned_off() {
        MongoEventStore notResuming = storeOver(mongoClient, config -> config.resumeFromMarkerVersions(false));

        notResuming.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));

        assertThat(marker(KEY_A).get(CHECKPOINT_EPOCH)).isInstanceOf(ObjectId.class);
        assertThat(marker(KEY_A).get(CHECKPOINT_STAMPED)).isEqualTo(1L);
        assertThat(markerVersionsOf("a")).contains(new Recorded(KEY_A, epochOf(KEY_A), 1));
    }

    @Test
    void reports_the_events_of_a_read_that_asks_for_versions_but_no_versions_when_it_cannot_start_a_session() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        AtomicBoolean failing = new AtomicBoolean();
        MongoEventStore store = storeOver(HoldingMongoClient.beforeEachSession(mongoClient, () -> {
            if (failing.get()) {
                throw new MongoException("injected failure to start a session");
            }
        }), config -> {
        });
        assertThat(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions()).as("reported while sessions start").isPresent();
        failing.set(true);

        DcbEventStream read = store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions());

        assertThat(read.markerVersions()).isEmpty();
        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("a");
    }

    @Test
    void refuses_a_read_after_versions_when_it_cannot_start_a_session() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        AtomicBoolean failing = new AtomicBoolean();
        MongoEventStore store = storeOver(HoldingMongoClient.beforeEachSession(mongoClient, () -> {
            if (failing.get()) {
                throw new MongoException("injected failure to start a session");
            }
        }), config -> {
        });
        failing.set(true);

        assertThat(catchThrowable(() -> store.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasRootCauseMessage("injected failure to start a session");
    }

    @Test
    void reports_the_events_of_a_read_that_asks_for_versions_but_no_versions_when_the_commit_of_its_snapshot_fails() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        AtomicBoolean failing = new AtomicBoolean();
        MongoEventStore store = storeOver(failingToCommitWhen(mongoClient, failing), config -> {
        });
        assertThat(store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions()).as("reported while commits succeed").isPresent();
        failing.set(true);

        DcbEventStream read = store.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions());

        assertThat(read.markerVersions()).isEmpty();
        assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("a");
    }

    @Test
    void refuses_a_read_after_versions_when_the_commit_of_its_snapshot_fails() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        AtomicBoolean failing = new AtomicBoolean();
        MongoEventStore store = storeOver(failingToCommitWhen(mongoClient, failing), config -> {
        });
        failing.set(true);

        assertThat(catchThrowable(() -> store.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasRootCauseMessage("injected failure to commit");
    }

    @Test
    void reports_the_events_of_a_read_inside_a_transaction_the_caller_owns_but_no_marker_versions() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A)));

        inCallerTransaction(() -> {
            DcbEventStream read = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions());

            assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("before");
            assertThat(read.markerVersions()).isEmpty();
        });
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions())
                .as("the same read outside the transaction reports versions, or the empty map proves nothing")
                .isPresent();
    }

    @Test
    void refuses_a_read_after_versions_inside_a_transaction_the_caller_owns() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A)));
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();

        inCallerTransaction(() -> assertThat(catchThrowable(() -> eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved))))
                .isInstanceOf(DcbSnapshotInvalidException.class));
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)).events())
                .as("the same read outside the transaction is accepted, or the refusal proves nothing")
                .isEmpty();
    }

    @Test
    void reports_no_marker_versions_for_a_read_after_an_append_in_a_transaction_the_caller_owns_that_has_not_committed() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A)));
        DcbMarkerVersions saved = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();

        inCallerTransaction(() -> {
            eventStore.read(BOUNDARY_A);
            eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)));

            DcbEventStream read = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().withVersions());

            assertThat(read.events()).extracting(CloudEvent::getId).containsExactly("before", "own");
            assertThat(read.markerVersions()).as("versions that cover an increment of the transaction would be handed out again if it aborts").isEmpty();
            assertThat(catchThrowable(() -> eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(saved)))).isInstanceOf(DcbSnapshotInvalidException.class);
        });
    }

    @Test
    void reports_the_marker_versions_of_an_append_inside_a_transaction_the_caller_owns() {
        inCallerTransaction(() -> {
            DcbAppendResult result = eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)), failIfEventsMatch(BOUNDARY_A, eventStore.read(BOUNDARY_A).consistencyToken()));

            assertThat(result.markerVersions().orElseThrow().keys()).containsExactly(KEY_A);
        });
    }

    @Test
    void does_not_confirm_an_append_made_in_a_transaction_the_caller_owns_until_it_commits() {
        try (ClientSession session = mongoClient.startSession()) {
            session.startTransaction();
            ClientSessionHolder.set(session);
            final DcbAppendResult result;
            try {
                result = eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)));

                assertThat(eventStore.isConfirmed(result)).as("while the transaction is open").isFalse();
            } finally {
                ClientSessionHolder.remove();
            }
            session.commitTransaction();

            assertThat(eventStore.isConfirmed(result)).as("after the transaction committed").isTrue();
        }
    }

    @Test
    void does_not_confirm_an_append_made_in_a_transaction_the_caller_owns_that_aborts() {
        DcbAppendResult[] result = new DcbAppendResult[1];

        inCallerTransaction(() -> result[0] = eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A))));

        assertThat(eventStore.isConfirmed(result[0])).isFalse();
    }

    @Test
    void reports_marker_versions_for_a_read_that_is_the_first_operation_of_a_transaction_the_caller_opened_and_resumes_from_them_later() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A)));
        AtomicReference<DcbEventStream> read = new AtomicReference<>();

        inCallerTransaction(() -> read.set(eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction())));

        assertThat(read.get().events()).extracting(CloudEvent::getId).containsExactly("before");
        assertThat(read.get().markerVersions()).as("the versions of a read that is the first operation of the transaction").isPresent();
        eventStore.append(List.of(taggedEvent("later", DEPOSITED, ACCOUNT_A)));
        assertThat(eventStore.read(BOUNDARY_A, DcbReadOptions.afterVersions(read.get().markerVersions().orElseThrow())).events())
                .extracting(CloudEvent::getId).containsExactly("later");
    }

    @Test
    void counts_the_uncommitted_append_of_the_transaction_for_a_read_that_says_it_is_the_first_operation_of_it_although_an_append_came_first() {
        eventStore.append(List.of(taggedEvent("before", DEPOSITED, ACCOUNT_A)));

        inCallerTransaction(() -> {
            eventStore.read(BOUNDARY_A);
            eventStore.append(List.of(taggedEvent("own", DEPOSITED, ACCOUNT_A)));

            DcbEventStream read = eventStore.read(BOUNDARY_A, DcbReadOptions.fromBeginning().asFirstOperationOfTransaction());

            assertThat(read.markerVersions()).as("the store reports versions from the transaction the caller opened").isPresent();
            assertThat(read.markerVersions().orElseThrow().get(KEY_A).orElseThrow().version()).as("the read runs in the transaction and not on a session of its own, so it counts the increment of the append").isEqualTo(2);
        });
    }

    @Test
    void adds_no_marker_versions_extension_to_the_events_a_stream_read_and_a_query_return() {
        eventStore.append(List.of(taggedEvent("a", DEPOSITED, ACCOUNT_A)));
        Document stored = rawEvent("a");
        assertThat(stored.containsKey(EVENT_MARKER_VERSIONS)).as("the stored event has marker versions, or this proves nothing").isTrue();

        List<CloudEvent> queried = eventStore.query(Filter.all()).toList();

        assertThat(queried).hasSize(1);
        assertThat(queried.getFirst().getExtensionNames()).doesNotContain(EVENT_MARKER_VERSIONS);
    }

    @Test
    void writes_only_the_first_stripe_of_a_type_when_type_marker_writes_are_not_spread() {
        for (int i = 0; i < 64; i++) {
            eventStore.append(List.of(taggedEvent("e" + i, DEPOSITED, "account:s" + i)));
        }

        assertThat(existingStripes(DEPOSITED)).containsExactly(0);
        assertThat(marker(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, 0)).get(CHECKPOINT_VERSION)).isEqualTo(64L);
        for (int i = 0; i < 64; i++) {
            assertThat(markerVersionsOf("e" + i)).extracting(Recorded::key).contains(KEY_DEPOSITED).doesNotContain(DcbMarkerVersionsSupport.stripeKey(DEPOSITED, 1));
        }
    }

    @Test
    void writes_more_than_one_stripe_of_a_type_when_type_marker_writes_are_spread() {
        MongoEventStore spreading = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(true));
        for (int i = 0; i < 64; i++) {
            spreading.append(List.of(taggedEvent("e" + i, DEPOSITED, "account:s" + i)));
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
    void writes_more_than_one_stripe_of_a_type_when_tagged_conditional_appends_spread_their_type_marker_write() {
        MongoEventStore spreading = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(true));
        for (int i = 0; i < 64; i++) {
            DcbCriteria criteria = DcbCriteria.tags(Tag.parse("seat:" + i));
            spreading.append(List.of(taggedEvent("e" + i, DEPOSITED, "seat:" + i)), failIfEventsMatch(criteria, spreading.read(criteria).consistencyToken()));
        }

        assertThat(existingStripes(DEPOSITED)).hasSizeGreaterThan(1);
    }

    @Test
    void increments_every_stripe_of_a_type_for_a_type_only_conditional_append_when_type_marker_writes_are_not_spread() {
        typeOnlyConditionalAppendIncrementsEveryStripe(eventStore);
    }

    @Test
    void increments_every_stripe_of_a_type_for_a_type_only_conditional_append_when_type_marker_writes_are_spread() {
        typeOnlyConditionalAppendIncrementsEveryStripe(storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(true)));
    }

    private void typeOnlyConditionalAppendIncrementsEveryStripe(MongoEventStore store) {
        DcbCriteria criteria = DcbCriteria.types(DEPOSITED);
        store.append(List.of(taggedEvent("a", DEPOSITED)), failIfEventsMatch(criteria, store.read(criteria).consistencyToken()));

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
        for (boolean[] settings : new boolean[][]{{false, false}, {false, true}, {true, false}, {true, true}}) {
            MongoEventStore reader = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(settings[0]));
            MongoEventStore writer = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(settings[1]));
            for (int i = 0; i < 32; i++) {
                String type = "Control" + UUID.randomUUID();
                DcbCriteria criteria = DcbCriteria.types(type);
                DcbConsistencyToken token = reader.read(criteria).consistencyToken();
                writer.append(List.of(taggedEvent(UUID.randomUUID().toString(), "Other" + UUID.randomUUID())));

                assertThatCode(() -> reader.append(List.of(taggedEvent(UUID.randomUUID().toString(), type)), failIfEventsMatch(criteria, token))).doesNotThrowAnyException();
            }
        }
    }

    private void conflictingAppendIsRefused(boolean readerSpreads, boolean writerSpreads) {
        MongoEventStore reader = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(readerSpreads));
        MongoEventStore writer = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(writerSpreads));
        for (int i = 0; i < 32; i++) {
            String type = "Mixed" + UUID.randomUUID();
            DcbCriteria criteria = DcbCriteria.types(type);
            DcbConsistencyToken token = reader.read(criteria).consistencyToken();
            writer.append(List.of(taggedEvent(UUID.randomUUID().toString(), type)));

            assertThatThrownBy(() -> reader.append(List.of(taggedEvent(UUID.randomUUID().toString(), type)), failIfEventsMatch(criteria, token)))
                    .as("the append %d of the type %s after a competitor that wrote it", i, type)
                    .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }
    }

    @Test
    void exactly_one_of_two_racing_type_only_conditional_appends_wins_when_the_first_store_does_not_spread_and_the_second_spreads() {
        racingTypeOnlyConditionalAppendsHaveOneWinner(false, true);
    }

    @Test
    void exactly_one_of_two_racing_type_only_conditional_appends_wins_when_the_first_store_spreads_and_the_second_does_not_spread() {
        racingTypeOnlyConditionalAppendsHaveOneWinner(true, false);
    }

    private void racingTypeOnlyConditionalAppendsHaveOneWinner(boolean firstSpreads, boolean secondSpreads) {
        MongoEventStore first = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(firstSpreads));
        MongoEventStore second = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(secondSpreads));
        for (int i = 0; i < 16; i++) {
            String type = "Raced" + UUID.randomUUID();
            DcbCriteria criteria = DcbCriteria.types(type);
            DcbAppendCondition firstCondition = failIfEventsMatch(criteria, first.read(criteria).consistencyToken());
            DcbAppendCondition secondCondition = failIfEventsMatch(criteria, second.read(criteria).consistencyToken());
            // The tags are disjoint on purpose. A shared tag would put a common tag marker under both writers and
            // serialise them whatever the type stripes do, so only the stripes of the type are left to conflict on.
            CloudEvent firstEvent = taggedEvent(UUID.randomUUID().toString(), type, "writer:first" + i);
            CloudEvent secondEvent = taggedEvent(UUID.randomUUID().toString(), type, "writer:second" + i);

            List<Outcome<DcbAppendResult>> outcomes = ConcurrentRendezvous.collide(2, index -> () -> index == 0
                    ? first.append(List.of(firstEvent), firstCondition)
                    : second.append(List.of(secondEvent), secondCondition));

            assertThat(outcomes.stream().filter(Outcome::succeeded).count())
                    .as("the winners of two racing type-only conditional appends of %s, iteration %d", type, i)
                    .isEqualTo(1);
            assertThat(outcomes.stream().filter(outcome -> !outcome.succeeded()).map(Outcome::failure).toList())
                    .as("the loser of the race over %s, iteration %d", type, i)
                    .singleElement()
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
        MongoEventStore reader = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(readerSpreads));
        MongoEventStore writer = storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(writerSpreads));
        DcbCriteria criteria = DcbCriteria.types("D");
        reader.append(List.of(taggedEvent("first", "D")));
        DcbMarkerVersions saved = reader.read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 32; i++) {
            ids.add("later" + i);
            writer.append(List.of(taggedEvent("later" + i, "D")));
        }

        List<String> read = reader.read(criteria, DcbReadOptions.afterVersions(saved)).events().stream().map(CloudEvent::getId).toList();

        assertThat(read).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void refuses_a_type_only_conditional_append_after_a_writer_that_wrote_no_marker_versions_incremented_the_type_marker_when_type_marker_writes_are_not_spread() {
        refusesAfterAnOlderWriterIncrementedTheTypeMarker(eventStore);
    }

    @Test
    void refuses_a_type_only_conditional_append_after_a_writer_that_wrote_no_marker_versions_incremented_the_type_marker_when_type_marker_writes_are_spread() {
        refusesAfterAnOlderWriterIncrementedTheTypeMarker(storeOver(mongoClient, config -> config.spreadTypeMarkerWrites(true)));
    }

    private void refusesAfterAnOlderWriterIncrementedTheTypeMarker(MongoEventStore store) {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbConsistencyToken token = store.read(criteria).consistencyToken();

        markers().updateOne(eq("_id", DcbMarkerModel.markerId(KEY_D)), new Document("$inc", new Document(CHECKPOINT_VERSION, 1L)), new UpdateOptions().upsert(true));

        assertThatThrownBy(() -> store.append(List.of(taggedEvent("a", "D")), failIfEventsMatch(criteria, token)))
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_a_read_after_versions_with_an_absent_type_stripe_that_an_older_writer_has_incremented_since() {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbMarkerVersions saved = eventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_D)).append(CHECKPOINT_VERSION, 2L).append(CHECKPOINT_STAMPED, 1L).append(CHECKPOINT_EPOCH, new ObjectId()));

        assertThatThrownBy(() -> eventStore.read(criteria, DcbReadOptions.afterVersions(saved)))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_D)
                .hasMessageContaining("writer that wrote no marker versions");
    }

    @Test
    void refuses_a_read_after_versions_with_an_absent_type_stripe_whose_marker_has_no_epoch_now() {
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbMarkerVersions saved = eventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        markers().insertOne(new Document("_id", DcbMarkerModel.markerId(KEY_D)).append(CHECKPOINT_VERSION, 1L));

        assertThatThrownBy(() -> eventStore.read(criteria, DcbReadOptions.afterVersions(saved)))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining(KEY_D)
                .hasMessageContaining("has no epoch");
    }

    private List<Integer> existingStripes(String type) {
        return java.util.stream.IntStream.range(0, DcbMarkerVersionsSupport.STRIPES)
                .filter(stripe -> markers().countDocuments(eq("_id", DcbMarkerModel.markerId(DcbMarkerVersionsSupport.stripeKey(type, stripe)))) > 0)
                .boxed().toList();
    }

    private void inCallerTransaction(Runnable body) {
        try (ClientSession session = mongoClient.startSession()) {
            session.startTransaction();
            ClientSessionHolder.set(session);
            try {
                body.run();
            } finally {
                ClientSessionHolder.remove();
                session.abortTransaction();
            }
        }
    }

    private MongoEventStore storeOver(MongoClient client, Consumer<EventStoreConfig.Builder> configure) {
        EventStoreConfig.Builder builder = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB);
        configure.accept(builder);
        return new MongoEventStore(client, database.getName(), EVENTS, builder.build());
    }

    private static MongoClient failingToCommitWhen(MongoClient delegate, AtomicBoolean failing) {
        return (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(), new Class<?>[]{MongoClient.class}, (proxy, method, args) -> {
            Object result = invoke(delegate, method, args);
            if (!method.getName().equals("startSession")) {
                return result;
            }
            ClientSession session = (ClientSession) result;
            return Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[]{ClientSession.class}, (sessionProxy, sessionMethod, sessionArgs) -> {
                if (sessionMethod.getName().equals("commitTransaction") && failing.get()) {
                    throw new MongoException("injected failure to commit");
                }
                return invoke(session, sessionMethod, sessionArgs);
            });
        });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private MongoCollection<Document> events() {
        return database.getCollection(EVENTS);
    }

    private MongoCollection<Document> markers() {
        return database.getCollection(DcbMarkerModel.checkpointCollectionName(EVENTS));
    }

    private Document marker(String key) {
        return requireNonNull(markers().find(eq("_id", DcbMarkerModel.markerId(key))).first(), "No marker " + key);
    }

    private String epochOf(String key) {
        return marker(key).getObjectId(CHECKPOINT_EPOCH).toHexString();
    }

    private Document rawEvent(String id) {
        return requireNonNull(events().find(eq("id", id)).first(), "No event " + id);
    }

    private List<Recorded> markerVersionsOf(String id) {
        return rawEvent(id).getList(EVENT_MARKER_VERSIONS, Document.class).stream()
                .map(element -> new Recorded(element.getString("k"), element.getObjectId("e").toHexString(), ((Number) element.get("v")).longValue()))
                .toList();
    }

    private static List<Document> indexKeysOf(MongoCollection<Document> collection) {
        return collection.listIndexes().map(index -> index.get("key", Document.class)).into(new java.util.ArrayList<>());
    }

    private record Recorded(String key, String epoch, long version) {
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
