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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // Scope.of

    @Test
    void scope_of_a_replica_set_is_REPLICA_SET() {
        assertThat(Scope.of(Deployment.REPLICA_SET)).isEqualTo(Scope.REPLICA_SET);
    }

    @Test
    void scope_of_a_mongos_is_SNAPSHOT_THROUGH_MONGOS() {
        assertThat(Scope.of(Deployment.MONGOS)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void scope_of_any_other_deployment_is_MARKERS_ONLY() {
        assertThat(Scope.of(Deployment.OTHER)).isEqualTo(Scope.MARKERS_ONLY);
    }

    // snapshotHoldsTheAppendsRead

    @Test
    void a_snapshot_holds_the_appends_read_when_every_marker_has_the_same_version() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(3), KEY_B, version(2));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, version(3), KEY_B, version(2)))).isTrue();
    }

    @Test
    void a_snapshot_holds_the_appends_read_when_a_marker_has_a_higher_version_in_the_snapshot() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(3), KEY_B, version(2));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, version(3), KEY_B, version(5)))).isTrue();
    }

    @Test
    void a_snapshot_does_not_hold_the_appends_read_when_a_marker_has_a_lower_version_in_the_snapshot() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(3), KEY_B, version(2));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, version(3), KEY_B, version(1)))).as("a marker the snapshot reads from before the append's read").isFalse();
    }

    @Test
    void a_snapshot_does_not_hold_the_appends_read_when_a_marker_has_another_epoch_in_the_snapshot_even_at_a_higher_version() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(3));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, new MarkerObservation("epoch-2", 9, 0)))).as("the versions of another epoch can't be compared").isFalse();
    }

    @Test
    void a_snapshot_does_not_hold_the_appends_read_when_a_marker_is_present_in_the_append_and_absent_in_the_snapshot() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, MarkerObservation.absent()))).as("a marker the snapshot doesn't have yet").isFalse();
    }

    @Test
    void a_snapshot_holds_the_appends_read_when_a_marker_is_absent_in_both() {
        Map<String, MarkerObservation> read = markers(KEY_A, MarkerObservation.absent(), KEY_B, version(1));

        assertThat(DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, MarkerObservation.absent(), KEY_B, version(1)))).isTrue();
    }

    @Test
    void a_snapshot_does_not_hold_the_appends_read_when_a_key_of_the_append_is_missing_from_the_snapshot_map() {
        Map<String, MarkerObservation> read = markers(KEY_A, version(1), KEY_B, version(1));

        Boolean[] holds = new Boolean[1];

        Throwable thrown = catchThrowable(() -> holds[0] = DcbAppendConditionCheck.snapshotHoldsTheAppendsRead(read, markers(KEY_A, version(1))));

        assertThat(thrown).as("a key the snapshot didn't read is answered, not dereferenced").isNull();
        assertThat(holds[0]).as("a marker the snapshot has no observation of").isFalse();
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
}
