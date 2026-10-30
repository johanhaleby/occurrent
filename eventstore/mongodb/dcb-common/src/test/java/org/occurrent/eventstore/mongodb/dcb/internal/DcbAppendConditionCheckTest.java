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
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.ItemQuery;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.KeySelection;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Pass;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Query;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Refuse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayName("DcbAppendConditionCheck")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbAppendConditionCheckTest {

    private static final String E1 = "epoch-1";
    private static final String E2 = "epoch-2";
    private static final String KEY_A = "tag:a:1";
    private static final String KEY_B = "tag:b:1";
    private static final String KEY_X = "type:X";
    private static final String KEY_Y = "type:Y";
    private static final DcbCriterion TAGS_A = DcbCriteria.tags(Tag.of("a", "1"));
    private static final DcbCriterion TAGS_A_AND_B = DcbCriteria.tags(Tag.of("a", "1"), Tag.of("b", "1"));
    private static final DcbCriterion TAGS_B = DcbCriteria.tags(Tag.of("b", "1"));
    private static final DcbCriterion TYPE_X = DcbCriteria.type("X");
    private static final DcbCriterion TYPES_X_AND_Y = DcbCriteria.types("X", "Y");

    @Test
    void passes_when_no_marker_has_changed() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, observation(E1, 2)), read(KEY_A, observation(E1, 2)));

        assertThat(outcome).isEqualTo(new Pass());
    }

    @Test
    void refuses_when_the_epoch_of_a_marker_the_token_saw_has_changed_although_its_version_has_not() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, observation(E1, 2)), read(KEY_A, observation(E2, 2)));

        assertThat(outcome).as("a marker deleted and recreated to the same version is the prune case").isEqualTo(new Refuse());
    }

    @Test
    void refuses_when_a_marker_the_token_saw_is_missing() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, observation(E1, 2)), read(KEY_A, MarkerObservation.absent()));

        assertThat(outcome).isEqualTo(new Refuse());
    }

    @Test
    void refuses_when_the_version_of_a_marker_the_token_saw_is_lower_in_the_same_epoch() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, observation(E1, 2)), read(KEY_A, observation(E1, 1)));

        assertThat(outcome).isEqualTo(new Refuse());
    }

    @Test
    void refuses_when_one_marker_was_lowered_by_as_much_as_another_was_raised() {
        DcbCriteria criteria = DcbCriteria.anyOf(TAGS_A, TAGS_B);
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 2), KEY_B, MarkerObservation.absent());
        Map<String, MarkerObservation> now = read(KEY_A, MarkerObservation.absent(), KEY_B, observation(E2, 2));

        DcbAppendConditionCheck.Outcome outcome = check(criteria, read, now);

        assertThat(outcome).as("the versions of both add up to 2 at the read and now, so a comparison of sums would pass").isEqualTo(new Refuse());
    }

    @Test
    void refuses_when_a_writer_that_writes_no_marker_versions_incremented_a_marker_after_the_read() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, observation(E1, 2)), read(KEY_A, new MarkerObservation(E1, 3, 1)));

        assertThat(outcome).as("an event of that writer has no mv, so the events can't be asked for it").isEqualTo(new Refuse());
    }

    @Test
    void refuses_a_moved_marker_that_had_no_epoch_at_the_read() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, new MarkerObservation(null, 2, 2)), read(KEY_A, new MarkerObservation(E1, 3, 2)));

        assertThat(outcome).as("a marker last written by 0.34 may have events without mv").isEqualTo(new Refuse());
    }

    @Test
    void refuses_a_moved_marker_that_has_no_epoch_when_the_read_saw_it_without_one_and_it_still_has_none() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, new MarkerObservation(null, 2, 2)), read(KEY_A, new MarkerObservation(null, 3, 2)));

        assertThat(outcome).as("there is no epoch to select the events by").isEqualTo(new Refuse());
    }

    @Test
    void does_not_refuse_a_marker_that_was_absent_at_the_read_and_exists_now_but_asks_for_the_events_above_version_0() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, MarkerObservation.absent()), read(KEY_A, observation(E1, 1)));

        assertThat(outcome).as("every mv element with the epoch it has now is newer than the read").isEqualTo(query(TAGS_A, new KeySelection(KEY_A, 0)));
    }

    @Test
    void passes_when_a_marker_was_absent_at_the_read_and_is_still_absent() {
        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read(KEY_A, MarkerObservation.absent()), read(KEY_A, MarkerObservation.absent()));

        assertThat(outcome).isEqualTo(new Pass());
    }

    @Test
    void asks_for_the_events_above_the_version_of_the_first_tag_key_of_an_item_with_tags_when_every_tag_marker_moved() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 1), KEY_B, observation(E2, 4));
        Map<String, MarkerObservation> now = read(KEY_A, observation(E1, 2), KEY_B, observation(E2, 5));

        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A_AND_B, read, now);

        assertThat(outcome).as("every matching event has every tag, so the first key in canonical order finds them all").isEqualTo(query(TAGS_A_AND_B, new KeySelection(KEY_A, 1)));
    }

    @Test
    void skips_an_item_with_tags_when_one_of_its_tag_markers_did_not_move() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 1), KEY_B, observation(E2, 4));
        Map<String, MarkerObservation> now = read(KEY_A, observation(E1, 2), KEY_B, observation(E2, 4));

        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A_AND_B, read, now);

        assertThat(outcome).as("a matching event increments both tag markers, so an event that incremented only one of them doesn't match").isEqualTo(new Pass());
    }

    @Test
    void decides_each_item_on_its_own_markers_and_not_on_those_of_the_whole_criteria() {
        DcbCriteria criteria = DcbCriteria.anyOf(TAGS_A, TAGS_B);
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 1), KEY_B, observation(E2, 1));
        Map<String, MarkerObservation> now = read(KEY_A, observation(E1, 1), KEY_B, observation(E2, 2));

        DcbAppendConditionCheck.Outcome outcome = check(criteria, read, now);

        assertThat(outcome).as("the marker of the first item didn't move, but the second item's did").isEqualTo(query(TAGS_B, new KeySelection(KEY_B, 1)));
    }

    @Test
    void skips_an_item_without_tags_when_none_of_its_type_markers_moved_and_asks_only_for_the_item_that_moved() {
        DcbCriteria criteria = DcbCriteria.anyOf(TYPE_X, TAGS_A);
        Map<String, MarkerObservation> read = read(KEY_X, observation(E1, 1), KEY_A, observation(E2, 1));
        Map<String, MarkerObservation> now = read(KEY_X, observation(E1, 1), KEY_A, observation(E2, 2));

        DcbAppendConditionCheck.Outcome outcome = check(criteria, read, now);

        assertThat(outcome).as("a matching event of type X increments type:X").isEqualTo(query(TAGS_A, new KeySelection(KEY_A, 1)));
    }

    @Test
    void passes_when_the_only_moved_marker_belongs_to_an_item_with_tags_that_another_tag_marker_rules_out() {
        DcbCriteria criteria = DcbCriteria.anyOf(TYPE_X, TAGS_A_AND_B);
        Map<String, MarkerObservation> read = read(KEY_X, observation(E1, 1), KEY_A, observation(E2, 1), KEY_B, observation(E2, 1));
        Map<String, MarkerObservation> now = read(KEY_X, observation(E1, 1), KEY_A, observation(E2, 2), KEY_B, observation(E2, 1));

        DcbAppendConditionCheck.Outcome outcome = check(criteria, read, now);

        assertThat(outcome).as("type(X) has no moved marker and tags(a, b) has an unmoved one, so neither can have a match").isEqualTo(new Pass());
    }

    @Test
    void asks_for_the_events_above_each_moved_type_marker_of_an_item_without_tags_and_not_for_the_unmoved_ones() {
        Map<String, MarkerObservation> read = read(KEY_X, observation(E1, 3), KEY_Y, observation(E2, 5));
        Map<String, MarkerObservation> now = read(KEY_X, observation(E1, 3), KEY_Y, observation(E2, 6));

        DcbAppendConditionCheck.Outcome outcome = check(TYPES_X_AND_Y, read, now);

        assertThat(outcome).as("an event of type Y matches, and one of type X would have moved type:X").isEqualTo(query(TYPES_X_AND_Y, new KeySelection(KEY_Y, 5)));
    }

    @Test
    void passes_for_a_criteria_that_matches_every_event_when_the_all_marker_did_not_move() {
        DcbAppendConditionCheck.Outcome outcome = check(DcbCriteria.all(), read("all", observation(E1, 3)), read("all", observation(E1, 3)));

        assertThat(outcome).isEqualTo(new Pass());
    }

    @Test
    void refuses_for_a_criteria_that_matches_every_event_when_the_all_marker_moved() {
        DcbAppendConditionCheck.Outcome outcome = check(DcbCriteria.all(), read("all", observation(E1, 3)), read("all", observation(E1, 4)));

        assertThat(outcome).as("there is no selection key to ask the events with").isEqualTo(new Refuse());
    }

    @Test
    void throws_IllegalArgumentException_when_the_token_holds_a_marker_key_the_criteria_lack() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 2), KEY_B, observation(E1, 2));

        Throwable thrown = catchThrowable(() -> check(TAGS_A, read, read(KEY_A, observation(E1, 2))));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class).hasMessageContaining(KEY_B).hasMessageContaining("didn't create it");
    }

    @Test
    void throws_IllegalArgumentException_when_the_token_lacks_a_marker_key_of_the_criteria() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 2));

        Throwable thrown = catchThrowable(() -> check(DcbCriteria.anyOf(TAGS_A, TAGS_B), read, read(KEY_A, observation(E1, 2), KEY_B, observation(E1, 2))));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class).hasMessageContaining(KEY_B);
    }

    @Test
    void throws_IllegalArgumentException_when_the_token_holds_no_markers_for_a_criteria_with_marker_keys_although_the_markers_are_unchanged() {
        DcbConsistencyToken token = DcbConsistencyToken.of(TAGS_A, 0);

        Throwable thrown = catchThrowable(() -> DcbAppendConditionCheck.check(token, TAGS_A, read(KEY_A, MarkerObservation.absent())));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    private static DcbAppendConditionCheck.Outcome check(DcbCriteria criteria, Map<String, MarkerObservation> read, Map<String, MarkerObservation> now) {
        DcbConsistencyToken token = DcbConsistencyToken.of(criteria, read.values().stream().mapToLong(MarkerObservation::version).sum(), read);
        return DcbAppendConditionCheck.check(token, criteria, now);
    }

    private static MarkerObservation observation(String epoch, long version) {
        return new MarkerObservation(epoch, version, 0);
    }

    private static Map<String, MarkerObservation> read(String key, MarkerObservation observation) {
        return Map.of(key, observation);
    }

    private static Map<String, MarkerObservation> read(String key1, MarkerObservation observation1, String key2, MarkerObservation observation2) {
        Map<String, MarkerObservation> map = new LinkedHashMap<>();
        map.put(key1, observation1);
        map.put(key2, observation2);
        return map;
    }

    private static Map<String, MarkerObservation> read(String key1, MarkerObservation observation1, String key2, MarkerObservation observation2, String key3, MarkerObservation observation3) {
        Map<String, MarkerObservation> map = read(key1, observation1, key2, observation2);
        map.put(key3, observation3);
        return map;
    }

    private static Query query(DcbCriterion item, KeySelection... selections) {
        return new Query(List.of(new ItemQuery(item, List.of(selections))));
    }
}
