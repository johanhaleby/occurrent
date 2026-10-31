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
import com.mongodb.ServerAddress;
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
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.ItemQuery;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.KeySelection;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Pass;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.QueryEvents;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Refuse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayName("DcbAppendConditionCheck")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbAppendConditionCheckTest {

    private static final String E1 = "epoch-1";
    private static final String E2 = "epoch-2";
    private static final String KEY_A = "tag:a:1";
    private static final String KEY_B = "tag:b:1";
    private static final String KEY_Z = "tag:z:1";
    private static final String KEY_X = "type:X";
    private static final String KEY_Y = "type:Y";
    private static final DcbCriterion TAGS_A = DcbCriteria.tags(Tag.of("a", "1"));
    private static final DcbCriterion TAGS_A_AND_B = DcbCriteria.tags(Tag.of("a", "1"), Tag.of("b", "1"));
    private static final DcbCriterion TAGS_A_AND_Z = DcbCriteria.tags(Tag.of("a", "1"), Tag.of("z", "1"));
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

        Throwable thrown = catchThrowable(() -> DcbAppendConditionCheck.check(token, TAGS_A, read(KEY_A, MarkerObservation.absent()), true));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refuses_a_condition_the_events_would_have_settled_when_the_store_may_not_query_them() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 1));
        Map<String, MarkerObservation> now = read(KEY_A, observation(E1, 2));

        DcbAppendConditionCheck.Outcome outcome = check(TAGS_A, read, now, false);

        assertThat(outcome).as("with the flag set this is a query of the events above version 1, so without it the check can only refuse").isEqualTo(new Refuse());
    }

    @Test
    void passes_when_no_marker_moved_whether_or_not_the_store_may_query_the_events() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 1), KEY_B, observation(E2, 4));

        assertThat(check(TAGS_A_AND_B, read, read, false)).as("nothing moved, so nothing needs the events").isEqualTo(new Pass());
        assertThat(check(TAGS_A_AND_B, read, read, true)).isEqualTo(new Pass());
    }

    @Test
    void refuses_an_item_with_an_unmoved_tag_marker_when_the_store_may_not_query_the_events_and_passes_it_when_it_may() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 4), KEY_Z, observation(E2, 1));
        Map<String, MarkerObservation> now = read(KEY_A, observation(E1, 4), KEY_Z, observation(E2, 2));

        assertThat(check(TAGS_A_AND_Z, read, now, false))
                .as("A read that can tear holds tag a from after a matching event and tag z from before it, so an unmoved marker of a doesn't rule the event out")
                .isEqualTo(new Refuse());
        assertThat(check(TAGS_A_AND_Z, read, now, true))
                .as("Where the read can't tear, the unmoved marker of a rules the item out")
                .isEqualTo(new Pass());
    }

    @Test
    void refuses_what_step_one_refuses_whether_or_not_the_store_may_query_the_events() {
        Map<String, MarkerObservation> read = read(KEY_A, observation(E1, 2));

        assertThat(check(TAGS_A, read, read(KEY_A, observation(E2, 2)), false)).as("the epoch changed").isEqualTo(new Refuse());
        assertThat(check(TAGS_A, read, read(KEY_A, MarkerObservation.absent()), false)).as("the marker is missing").isEqualTo(new Refuse());
        assertThat(check(TAGS_A, read, read(KEY_A, observation(E1, 1)), false)).as("the version is lower").isEqualTo(new Refuse());
    }

    @Test
    void allows_the_query_for_an_answer_with_a_replica_set_name() {
        assertThat(DcbAppendConditionCheck.queryAllowed(answering(new Document("setName", "rs0")))).isTrue();
    }

    @Test
    void refuses_the_query_for_an_answer_from_a_mongos() {
        assertThat(DcbAppendConditionCheck.queryAllowed(answering(new Document("msg", "isdbgrid")))).isFalse();
    }

    @Test
    void refuses_the_query_for_an_answer_with_a_replica_set_name_that_comes_from_a_mongos() {
        assertThat(DcbAppendConditionCheck.queryAllowed(answering(new Document("setName", "rs0").append("msg", "isdbgrid")))).isFalse();
    }

    @Test
    void refuses_the_query_for_an_answer_without_a_replica_set_name() {
        assertThat(DcbAppendConditionCheck.queryAllowed(answering(new Document()))).as("a standalone server has no setName").isFalse();
    }

    @Test
    void asks_hello_and_nothing_else_when_the_server_knows_it() {
        List<Document> sent = new ArrayList<>();

        DcbAppendConditionCheck.queryAllowed(command -> {
            sent.add(command);
            return new Document("setName", "rs0");
        });

        assertThat(sent).containsExactly(new Document("hello", 1));
    }

    @Test
    void falls_back_to_isMaster_when_hello_fails_with_command_not_found_and_uses_its_answer() {
        List<Document> sent = new ArrayList<>();
        AtomicBoolean allowed = new AtomicBoolean();

        assertThatCode(() -> allowed.set(DcbAppendConditionCheck.queryAllowed(oldServerAnswering(sent, new Document("setName", "rs0")))))
                .as("MongoDB 4.2.0 to 4.2.9 answer hello with code 59, and creating the store must not fail on that")
                .doesNotThrowAnyException();

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(allowed).as("the isMaster answer has a setName").isTrue();
    }

    @Test
    void falls_back_to_isMaster_when_hello_fails_with_command_not_found_and_refuses_the_query_for_a_mongos() {
        AtomicBoolean allowed = new AtomicBoolean(true);

        assertThatCode(() -> allowed.set(DcbAppendConditionCheck.queryAllowed(oldServerAnswering(new ArrayList<>(), new Document("msg", "isdbgrid")))))
                .doesNotThrowAnyException();

        assertThat(allowed).as("the isMaster answer is from a mongos").isFalse();
    }

    @Test
    void falls_back_to_isMaster_when_command_not_found_is_the_cause_of_the_exception_hello_throws() {
        List<Document> sent = new ArrayList<>();
        AtomicBoolean allowed = new AtomicBoolean();
        Function<Document, Document> server = command -> {
            sent.add(command);
            if (command.containsKey("hello")) {
                throw new IllegalStateException("The Spring template wraps the driver's exception", new RuntimeException("Another layer", commandFailed(59)));
            }
            return new Document("setName", "rs0");
        };

        assertThatCode(() -> allowed.set(DcbAppendConditionCheck.queryAllowed(server)))
                .as("a template wraps the driver's exception, so the code is looked for in the causes and not only in the exception thrown")
                .doesNotThrowAnyException();

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(allowed).isTrue();
    }

    @Test
    void propagates_a_failure_of_hello_that_is_not_command_not_found() {
        MongoCommandException unauthorized = commandFailed(13);
        List<Document> sent = new ArrayList<>();

        assertThatThrownBy(() -> DcbAppendConditionCheck.queryAllowed(command -> {
            sent.add(command);
            throw unauthorized;
        })).as("only a server that doesn't know hello gets the fallback, any other failure must stop the store from being created")
                .isSameAs(unauthorized);

        assertThat(sent).containsExactly(new Document("hello", 1));
    }

    @Test
    void propagates_a_failure_of_hello_that_is_not_a_command_exception() {
        IllegalStateException failure = new IllegalStateException("The server is unreachable");

        assertThatThrownBy(() -> DcbAppendConditionCheck.queryAllowed(command -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void propagates_command_not_found_from_isMaster() {
        MongoCommandException notFoundAgain = commandFailed(59);

        assertThatThrownBy(() -> DcbAppendConditionCheck.queryAllowed(command -> {
            throw command.containsKey("hello") ? commandFailed(59) : notFoundAgain;
        })).as("a server that knows neither command has nothing left to ask")
                .isSameAs(notFoundAgain);
    }

    private static DcbAppendConditionCheck.Outcome check(DcbCriteria criteria, Map<String, MarkerObservation> read, Map<String, MarkerObservation> now) {
        return check(criteria, read, now, true);
    }

    private static DcbAppendConditionCheck.Outcome check(DcbCriteria criteria, Map<String, MarkerObservation> read, Map<String, MarkerObservation> now, boolean queryAllowed) {
        DcbConsistencyToken token = DcbConsistencyToken.of(criteria, read.values().stream().mapToLong(MarkerObservation::version).sum(), read);
        return DcbAppendConditionCheck.check(token, criteria, now, queryAllowed);
    }

    private static Function<Document, Document> answering(Document answer) {
        return command -> answer;
    }

    // A server that fails hello with code 59, as MongoDB 4.2.0 to 4.2.9 do, and answers isMaster
    private static Function<Document, Document> oldServerAnswering(List<Document> sent, Document isMasterAnswer) {
        return command -> {
            sent.add(command);
            if (command.containsKey("hello")) {
                throw commandFailed(59);
            }
            return isMasterAnswer;
        };
    }

    private static MongoCommandException commandFailed(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("command failed"))
                .append("code", new BsonInt32(code));
        return new MongoCommandException(response, new ServerAddress());
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

    private static QueryEvents query(DcbCriterion item, KeySelection... selections) {
        return new QueryEvents(List.of(new ItemQuery(item, List.of(selections))));
    }
}
