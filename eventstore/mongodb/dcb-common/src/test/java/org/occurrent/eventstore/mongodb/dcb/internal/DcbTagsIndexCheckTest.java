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

import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbTagsIndexCheckTest {

    private static final String COLLECTION = "events";

    @Test
    void a_sparse_index_on_dcb_tags_alone_needs_no_warning() {
        Document index = dcbTagsIndex().append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_sparse_flag_that_listIndexes_returns_as_a_number_counts_as_sparse() {
        Document index = dcbTagsIndex().append("sparse", 1);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_direction_that_listIndexes_returns_as_a_double_still_counts_as_ascending() {
        Document index = new Document("v", 2).append("key", new Document("dcbTags", 1.0)).append("name", "dcbTags_1").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_collection_with_no_indexes_is_missing_the_index() {
        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of())).isEqualTo(DcbTagsIndexCheck.missingIndexMessage(COLLECTION));
    }

    @Test
    void a_compound_index_that_starts_with_dcb_tags_is_not_the_index() {
        Document compound = new Document("v", 2)
                .append("key", new Document("dcbTags", 1).append("position", 1))
                .append("name", "dcbTags_1_position_1")
                .append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(compound))).isEqualTo(DcbTagsIndexCheck.missingIndexMessage(COLLECTION));
    }

    @Test
    void a_sparse_descending_index_on_dcb_tags_alone_needs_no_warning() {
        Document descending = new Document("v", 2).append("key", new Document("dcbTags", -1)).append("name", "dcbTags_-1").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(descending))).isNull();
    }

    @Test
    void a_non_sparse_index_with_the_match_all_partial_filter_expression_needs_no_warning() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", true)));

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void an_exists_flag_in_the_partial_filter_expression_that_listIndexes_returns_as_a_number_counts_as_true() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", 1)));

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_non_sparse_index_on_dcb_tags_alone_is_reported_as_unusable() {
        Document index = dcbTagsIndex();

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("'dcbTags_1'", "isn't sparse", "dropIndex(\"dcbTags_1\")", "createIndex({ dcbTags: 1 }, { sparse: true })")
                .doesNotContain("collMod");
    }

    @Test
    void a_sparse_index_with_a_partial_filter_expression_is_reported_as_unusable() {
        Document index = dcbTagsIndex().append("sparse", true).append("partialFilterExpression", new Document("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression", "dropIndex(\"dcbTags_1\")")
                .doesNotContain("isn't sparse", "is hidden", "collMod");
    }

    @Test
    void a_partial_index_that_is_not_sparse_is_reported_for_its_partial_filter_expression_only() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                .doesNotContain("isn't sparse");
    }

    @Test
    void a_partial_filter_expression_that_only_partly_matches_the_match_all_one_is_reported_as_unusable() {
        Document extraCondition = dcbTagsIndex().append("partialFilterExpression",
                new Document("dcbTags", new Document("$exists", true)).append("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(extraCondition));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, extraCondition));
        assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                .doesNotContain("isn't sparse", "collMod");
    }

    @Test
    void a_partial_filter_expression_that_requires_dcb_tags_to_be_missing_is_reported_as_unusable() {
        Document existsFalse = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", false)));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(existsFalse));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, existsFalse));
        assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                .doesNotContain("isn't sparse", "collMod");
    }

    @Test
    void a_hidden_index_that_is_otherwise_usable_is_fixed_by_unhiding_it_and_not_by_recreating_it() {
        Document index = dcbTagsIndex().append("sparse", true).append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("'dcbTags_1'", "is hidden", "collMod: \"events\"", "hidden: false")
                .doesNotContain("dropIndex", "isn't sparse", "has a partialFilterExpression");
    }

    @Test
    void a_hidden_index_with_the_match_all_partial_filter_expression_is_fixed_by_unhiding_it_and_not_by_recreating_it() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", true))).append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("'dcbTags_1'", "is hidden", "collMod: \"events\"", "hidden: false")
                .doesNotContain("dropIndex", "isn't sparse", "partialFilterExpression other than");
    }

    @Test
    void a_hidden_index_that_is_also_not_sparse_is_fixed_by_recreating_it_since_unhiding_alone_would_not_help() {
        Document index = dcbTagsIndex().append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, index));
        assertThat(warning).contains("isn't sparse and is hidden", "dropIndex(\"dcbTags_1\")", "createIndex({ dcbTags: 1 }, { sparse: true })")
                .doesNotContain("collMod");
    }

    @Test
    void a_hidden_flag_set_to_false_does_not_make_the_index_unusable() {
        Document index = dcbTagsIndex().append("sparse", true).append("hidden", false);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_usable_index_anywhere_in_the_list_wins_over_an_unusable_one() {
        Document unusable = new Document("v", 2).append("key", new Document("dcbTags", 1)).append("name", "unusable");
        Document usable = new Document("v", 2).append("key", new Document("dcbTags", 1)).append("name", "usable").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(unusable, usable))).isNull();
        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(usable, unusable))).isNull();
    }

    @Test
    void an_unusable_index_is_reported_even_when_unrelated_indexes_are_present() {
        Document unusable = dcbTagsIndex();
        Document position = new Document("v", 2).append("key", new Document("position", 1)).append("name", "position_1").append("sparse", true);
        Document compound = new Document("v", 2).append("key", new Document("dcbTags", 1).append("position", 1)).append("name", "dcbTags_1_position_1").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(position, compound, unusable)))
                .isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, unusable));
    }

    private static Document dcbTagsIndex() {
        return new Document("v", 2).append("key", new Document("dcbTags", 1)).append("name", "dcbTags_1");
    }
}
