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

package org.occurrent.eventstore.mongodb.dcb.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Deployment;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.ItemQuery;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.KeySelection;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Outcome;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Pass;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.QueryEvents;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Refuse;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Scope;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayName("DcbAppendConditionCheck scope")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbAppendConditionCheckScopeTest {

    private static final String EPOCH = "epoch-1";
    private static final String KEY_A = "tag:a:1";
    private static final String KEY_B = "tag:b:1";
    private static final String KEY_Z = "tag:z:1";
    private static final String KEY_X = DcbMarkerVersionsSupport.stripeKey("X", 0);
    private static final String KEY_Y = DcbMarkerVersionsSupport.stripeKey("Y", 0);
    private static final DcbCriterion TAGS_A_AND_B = DcbCriteria.tags(Tag.of("a", "1"), Tag.of("b", "1"));
    private static final DcbCriterion TAGS_Z = DcbCriteria.tags(Tag.of("z", "1"));
    private static final DcbCriterion TYPES_X_AND_Y = DcbCriteria.types("X", "Y");

    private static final TransactionOptions SNAPSHOT_MAJORITY = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build();

    // Scope.of

    @Test
    void scope_of_a_replica_set_is_REPLICA_SET_whatever_the_transaction_options() {
        assertThat(Scope.of(Deployment.REPLICA_SET, null)).isEqualTo(Scope.REPLICA_SET);
        assertThat(Scope.of(Deployment.REPLICA_SET, TransactionOptions.builder().build())).isEqualTo(Scope.REPLICA_SET);
        assertThat(Scope.of(Deployment.REPLICA_SET, SNAPSHOT_MAJORITY)).isEqualTo(Scope.REPLICA_SET);
        assertThat(Scope.of(Deployment.REPLICA_SET, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.W1).build())).isEqualTo(Scope.REPLICA_SET);
    }

    @Test
    void scope_of_a_mongos_is_SNAPSHOT_THROUGH_MONGOS_for_read_concern_snapshot_with_write_concern_majority() {
        assertThat(Scope.of(Deployment.MONGOS, SNAPSHOT_MAJORITY)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void scope_of_a_mongos_is_MARKERS_ONLY_when_the_transaction_options_could_not_be_read() {
        assertThat(Scope.of(Deployment.MONGOS, null)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void scope_of_a_mongos_is_MARKERS_ONLY_for_read_concern_snapshot_with_write_concern_w1() {
        TransactionOptions options = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.W1).build();

        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void scope_of_a_mongos_is_MARKERS_ONLY_for_write_concern_majority_without_read_concern_snapshot() {
        TransactionOptions majorityRead = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY).build();

        assertThat(Scope.of(Deployment.MONGOS, majorityRead)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void scope_of_a_mongos_is_MARKERS_ONLY_for_write_concern_majority_without_a_read_concern() {
        TransactionOptions options = TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build();

        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void scope_of_a_mongos_is_MARKERS_ONLY_for_read_concern_snapshot_without_a_write_concern() {
        TransactionOptions options = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).build();

        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void scope_of_any_other_deployment_is_MARKERS_ONLY_even_for_read_concern_snapshot_with_write_concern_majority() {
        assertThat(Scope.of(Deployment.OTHER, SNAPSHOT_MAJORITY)).isEqualTo(Scope.MARKERS_ONLY);
        assertThat(Scope.of(Deployment.OTHER, null)).isEqualTo(Scope.MARKERS_ONLY);
    }

    // snapshotThroughMongos

    @Test
    void snapshotThroughMongos_sets_read_concern_snapshot_and_write_concern_majority() {
        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(TransactionOptions.builder().build(), WriteConcern.ACKNOWLEDGED);

        assertThat(options.getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(options.getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void snapshotThroughMongos_replaces_a_weaker_read_and_write_concern() {
        TransactionOptions weaker = TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.W1).build();

        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(weaker, WriteConcern.ACKNOWLEDGED);

        assertThat(options.getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(options.getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
    }

    @Test
    void snapshotThroughMongos_keeps_a_configured_wtimeout() {
        TransactionOptions configured = TransactionOptions.builder().writeConcern(WriteConcern.W1.withWTimeout(750, TimeUnit.MILLISECONDS)).build();

        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(configured, WriteConcern.ACKNOWLEDGED);

        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS)).isEqualTo(750);
    }

    @Test
    void snapshotThroughMongos_keeps_the_wtimeout_and_journal_flag_of_the_options_write_concern_next_to_read_concern_snapshot_and_write_concern_majority() {
        TransactionOptions configured = TransactionOptions.builder().writeConcern(WriteConcern.W1.withWTimeout(750, TimeUnit.MILLISECONDS).withJournal(true)).build();

        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(configured, WriteConcern.W2.withWTimeout(20, TimeUnit.MILLISECONDS).withJournal(false));

        assertThat(options.getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS)).isEqualTo(750);
        assertThat(options.getWriteConcern().getJournal()).isTrue();
    }

    @Test
    void snapshotThroughMongos_takes_the_wtimeout_and_journal_flag_from_the_client_write_concern_when_the_options_have_no_write_concern() {
        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(TransactionOptions.builder().build(), WriteConcern.W1.withWTimeout(300, TimeUnit.MILLISECONDS).withJournal(true));

        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS)).isEqualTo(300);
        assertThat(options.getWriteConcern().getJournal()).isTrue();
    }

    @Test
    void snapshotThroughMongos_sets_no_wtimeout_when_none_was_configured() {
        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(TransactionOptions.builder().writeConcern(WriteConcern.W1).build(), WriteConcern.ACKNOWLEDGED);

        assertThat(options.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void snapshotThroughMongos_keeps_the_other_options() {
        TransactionOptions configured = TransactionOptions.builder()
                .maxCommitTime(1500L, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.primaryPreferred())
                .build();

        TransactionOptions options = DcbAppendConditionCheck.snapshotThroughMongos(configured, WriteConcern.ACKNOWLEDGED);

        assertThat(options.getMaxCommitTime(TimeUnit.MILLISECONDS)).isEqualTo(1500L);
        assertThat(options.getReadPreference()).isEqualTo(ReadPreference.primaryPreferred());
    }

    @Test
    void restartAtOnePointInTime_is_empty_for_options_with_read_concern_snapshot_and_write_concern_majority() {
        assertThat(DcbAppendConditionCheck.restartAtOnePointInTime(SNAPSHOT_MAJORITY)).isEmpty();
    }

    @Test
    void restartAtOnePointInTime_gives_read_concern_snapshot_and_write_concern_majority_with_the_wtimeout_and_journal_flag_and_keeps_the_other_options() {
        TransactionOptions running = TransactionOptions.builder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.W1.withWTimeout(750, TimeUnit.MILLISECONDS).withJournal(true))
                .maxCommitTime(1500L, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.primaryPreferred())
                .build();

        Optional<TransactionOptions> restarted = DcbAppendConditionCheck.restartAtOnePointInTime(running);

        assertThat(restarted).isPresent();
        TransactionOptions options = restarted.get();
        assertThat(options.getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS)).isEqualTo(750);
        assertThat(options.getWriteConcern().getJournal()).isTrue();
        assertThat(options.getMaxCommitTime(TimeUnit.MILLISECONDS)).isEqualTo(1500L);
        assertThat(options.getReadPreference()).isEqualTo(ReadPreference.primaryPreferred());
    }

    // Scope.reading

    @Test
    void reading_on_a_mongos_is_SNAPSHOT_THROUGH_MONGOS_for_options_with_read_concern_snapshot_and_write_concern_majority() {
        assertThat(Scope.reading(Deployment.MONGOS, () -> SNAPSHOT_MAJORITY)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void reading_on_a_mongos_is_MARKERS_ONLY_when_reading_the_options_throws_a_RuntimeException() {
        Scope[] scope = new Scope[1];

        Throwable thrown = catchThrowable(() -> scope[0] = Scope.reading(Deployment.MONGOS, () -> {
            throw new IllegalStateException("no options");
        }));

        assertThat(thrown).as("reading the scope doesn't fail the append").isNull();
        assertThat(scope[0]).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void reading_on_a_mongos_is_MARKERS_ONLY_when_reading_the_options_throws_a_LinkageError() {
        Scope[] scope = new Scope[1];

        Throwable thrown = catchThrowable(() -> scope[0] = Scope.reading(Deployment.MONGOS, () -> {
            throw new NoSuchMethodError("getTransactionOptions");
        }));

        assertThat(thrown).as("reading the scope doesn't fail the append").isNull();
        assertThat(scope[0]).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void reading_on_a_replica_set_is_REPLICA_SET_without_reading_the_options() {
        AtomicInteger reads = new AtomicInteger();

        Scope scope = Scope.reading(Deployment.REPLICA_SET, () -> {
            reads.incrementAndGet();
            return SNAPSHOT_MAJORITY;
        });

        assertThat(scope).isEqualTo(Scope.REPLICA_SET);
        assertThat(reads).hasValue(0);
    }

    // deployment

    @Test
    void deployment_is_MONGOS_for_an_answer_with_msg_isdbgrid() {
        assertThat(DcbAppendConditionCheck.deployment(command -> new Document("msg", "isdbgrid"))).isEqualTo(Deployment.MONGOS);
    }

    @Test
    void deployment_is_MONGOS_for_an_answer_with_msg_isdbgrid_and_a_setName() {
        assertThat(DcbAppendConditionCheck.deployment(command -> new Document("msg", "isdbgrid").append("setName", "rs0"))).isEqualTo(Deployment.MONGOS);
    }

    @Test
    void deployment_is_REPLICA_SET_for_an_answer_with_a_setName() {
        assertThat(DcbAppendConditionCheck.deployment(command -> new Document("setName", "rs0"))).isEqualTo(Deployment.REPLICA_SET);
    }

    @Test
    void deployment_is_OTHER_for_an_answer_with_neither_msg_isdbgrid_nor_a_setName() {
        assertThat(DcbAppendConditionCheck.deployment(command -> new Document("isWritablePrimary", true))).isEqualTo(Deployment.OTHER);
    }

    @Test
    void deployment_falls_back_to_isMaster_when_hello_fails_with_command_not_found() {
        List<Document> sent = new ArrayList<>();

        Deployment deployment = DcbAppendConditionCheck.deployment(command -> {
            sent.add(command);
            if (command.containsKey("hello")) {
                throw commandFailed(59);
            }
            return new Document("msg", "isdbgrid");
        });

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(deployment).as("the isMaster answer is from a mongos").isEqualTo(Deployment.MONGOS);
    }

    // check

    @Test
    void a_token_torn_across_shards_selects_the_events_above_the_marker_that_moved_when_the_transaction_snapshots_through_a_mongos() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(1), KEY_B, version(2));

        Outcome outcome = check(TAGS_A_AND_B, read, now, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(outcome)
                .as("marker a can be from after an event the read missed, so a being unmoved does not rule the item out, and the event is above b's version, not on the first tag key")
                .isEqualTo(new QueryEvents(List.of(new ItemQuery(TAGS_A_AND_B, List.of(new KeySelection(KEY_B, 1))))));
    }

    @Test
    void a_token_torn_across_shards_passes_on_a_replica_set_because_the_unmoved_marker_rules_the_item_out() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(2), KEY_B, version(1));

        Outcome outcome = check(TAGS_A_AND_B, read, now, Scope.REPLICA_SET);

        assertThat(outcome).isEqualTo(new Pass());
    }

    @Test
    void a_token_torn_across_shards_is_refused_when_only_the_markers_can_be_compared() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(2), KEY_B, version(1));

        Outcome outcome = check(TAGS_A_AND_B, read, now, Scope.MARKERS_ONLY);

        assertThat(outcome).isEqualTo(new Refuse());
    }

    @Test
    void an_item_with_tags_whose_markers_all_moved_selects_on_every_one_of_them_when_the_transaction_snapshots_through_a_mongos() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(2), KEY_B, version(3));

        Outcome outcome = check(TAGS_A_AND_B, read, now, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(outcome).isInstanceOf(QueryEvents.class);
        List<ItemQuery> items = ((QueryEvents) outcome).items();
        assertThat(items).hasSize(1);
        assertThat(items.getFirst().item()).isEqualTo(TAGS_A_AND_B);
        assertThat(items.getFirst().selections()).containsExactlyInAnyOrder(new KeySelection(KEY_A, 1), new KeySelection(KEY_B, 1));
    }

    @Test
    void an_item_with_tags_whose_markers_all_moved_selects_on_one_key_on_a_replica_set() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(2), KEY_B, version(3));

        Outcome outcome = check(TAGS_A_AND_B, read, now, Scope.REPLICA_SET);

        assertThat(outcome).isInstanceOf(QueryEvents.class);
        assertThat(((QueryEvents) outcome).items()).singleElement().satisfies(item -> assertThat(item.selections()).hasSize(1));
    }

    @Test
    void an_item_none_of_whose_tag_markers_moved_is_ruled_out_and_the_item_whose_marker_moved_is_the_only_one_queried_when_the_transaction_snapshots_through_a_mongos() {
        DcbCriteria criteria = DcbCriteria.anyOf(TAGS_A_AND_B, TAGS_Z);
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1), KEY_Z, version(1));
        Map<String, MarkerObservation> now = markers(KEY_A, version(1), KEY_B, version(1), KEY_Z, version(2));

        Outcome outcome = check(criteria, read, now, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(outcome).isEqualTo(new QueryEvents(List.of(new ItemQuery(TAGS_Z, List.of(new KeySelection(KEY_Z, 1))))));
    }

    @Test
    void an_item_with_tags_is_ruled_out_when_none_of_its_markers_moved_and_nothing_else_moved_when_the_transaction_snapshots_through_a_mongos() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));

        Outcome outcome = check(TAGS_A_AND_B, read, read, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(outcome).isEqualTo(new Pass());
    }

    @Test
    void an_item_without_tags_is_checked_the_same_on_a_replica_set_as_when_the_transaction_snapshots_through_a_mongos_when_one_type_marker_moved() {
        Map<String, MarkerObservation> read = typeMarkers(version(1));
        Map<String, MarkerObservation> now = with(read, KEY_X, version(2));

        Outcome onReplicaSet = check(TYPES_X_AND_Y, read, now, Scope.REPLICA_SET);
        Outcome throughMongos = check(TYPES_X_AND_Y, read, now, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(onReplicaSet).as("the item has no tags, so one moved type marker means an event may match").isInstanceOf(QueryEvents.class);
        assertThat(throughMongos).isEqualTo(onReplicaSet);
    }

    @Test
    void an_item_without_tags_is_checked_the_same_on_a_replica_set_as_when_the_transaction_snapshots_through_a_mongos_when_no_marker_moved() {
        Map<String, MarkerObservation> read = typeMarkers(version(1));

        assertThat(check(TYPES_X_AND_Y, read, read, Scope.REPLICA_SET)).isEqualTo(new Pass());
        assertThat(check(TYPES_X_AND_Y, read, read, Scope.SNAPSHOT_THROUGH_MONGOS)).isEqualTo(new Pass());
    }

    @Test
    void an_item_without_tags_is_checked_the_same_on_a_replica_set_as_when_the_transaction_snapshots_through_a_mongos_when_both_type_markers_moved() {
        Map<String, MarkerObservation> read = typeMarkers(version(1));
        Map<String, MarkerObservation> now = with(with(read, KEY_X, version(2)), KEY_Y, version(2));

        Outcome onReplicaSet = check(TYPES_X_AND_Y, read, now, Scope.REPLICA_SET);
        Outcome throughMongos = check(TYPES_X_AND_Y, read, now, Scope.SNAPSHOT_THROUGH_MONGOS);

        assertThat(onReplicaSet).isInstanceOf(QueryEvents.class);
        assertThat(throughMongos).isEqualTo(onReplicaSet);
    }

    private static Outcome check(DcbCriteria criteria, Map<String, MarkerObservation> read, Map<String, MarkerObservation> now, Scope scope) {
        return DcbAppendConditionCheck.check(tokenOf(criteria, read), criteria, now, scope);
    }

    private static DcbConsistencyToken tokenOf(DcbCriteria criteria, Map<String, MarkerObservation> read) {
        return DcbConsistencyToken.of(criteria, read.values().stream().mapToLong(MarkerObservation::version).sum(), read);
    }

    private static MarkerObservation version(long version) {
        return new MarkerObservation(EPOCH, version, 0);
    }

    private static Map<String, MarkerObservation> markers(Object... keysAndObservations) {
        Map<String, MarkerObservation> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndObservations.length; i += 2) {
            map.put((String) keysAndObservations[i], (MarkerObservation) keysAndObservations[i + 1]);
        }
        return map;
    }

    // Every stripe key of X and of Y, which a token for an item without tags holds since type markers are striped (ADR 162)
    private static Map<String, MarkerObservation> typeMarkers(MarkerObservation observation) {
        Map<String, MarkerObservation> map = new LinkedHashMap<>();
        for (int stripe = 0; stripe < DcbMarkerVersionsSupport.STRIPES; stripe++) {
            map.put(DcbMarkerVersionsSupport.stripeKey("X", stripe), observation);
            map.put(DcbMarkerVersionsSupport.stripeKey("Y", stripe), observation);
        }
        return map;
    }

    private static Map<String, MarkerObservation> with(Map<String, MarkerObservation> observed, String key, MarkerObservation observation) {
        Map<String, MarkerObservation> map = new LinkedHashMap<>(observed);
        map.put(key, observation);
        return map;
    }

    private static MongoCommandException commandFailed(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("command failed"))
                .append("code", new BsonInt32(code));
        return new MongoCommandException(response, new ServerAddress());
    }
}
