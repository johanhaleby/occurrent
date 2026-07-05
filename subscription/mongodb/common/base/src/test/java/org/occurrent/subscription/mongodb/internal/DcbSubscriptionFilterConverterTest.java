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

package org.occurrent.subscription.mongodb.internal;

import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbCriterion;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionFilterConverterTest {

    private static final String FULL_DOCUMENT = "fullDocument";
    private static final String TYPE_FIELD = FULL_DOCUMENT + ".type";
    private static final String TAGS_FIELD = FULL_DOCUMENT + ".dcbTags";

    @Test
    void match_all_query_produces_only_the_dcb_tags_existence_condition() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(DcbCriteria.all());

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);
        assertThat(match).doesNotContainKey("$or");
    }

    @Test
    void single_type_query_produces_dollar_in_on_type_field() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(DcbCriteria.type("OrderPlaced"));

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(1);

        Document itemCondition = (Document) orList.get(0);
        Document typeDoc = itemCondition.get(TYPE_FIELD, Document.class);
        assertThat(typeDoc.getList("$in", String.class)).containsExactlyInAnyOrder("OrderPlaced");
        assertThat(typeDoc).doesNotContainKey("$nin");
    }

    @Test
    void multiple_types_query_produces_dollar_in_with_all_types() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(DcbCriteria.types("OrderPlaced", "OrderCancelled"));

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(1);

        Document itemCondition = (Document) orList.get(0);
        Document typeDoc = itemCondition.get(TYPE_FIELD, Document.class);
        assertThat(typeDoc.getList("$in", String.class)).containsExactlyInAnyOrder("OrderPlaced", "OrderCancelled");
    }

    @Test
    void tags_all_of_query_produces_dollar_all_on_tags_field() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(DcbCriteria.tags(Tag.parse("order:1"), Tag.parse("tenant:2")));

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(1);

        Document itemCondition = (Document) orList.get(0);
        assertThat(itemCondition).doesNotContainKey(TYPE_FIELD);
        Document tagsDoc = itemCondition.get(TAGS_FIELD, Document.class);
        assertThat(tagsDoc.getList("$all", String.class)).containsExactlyInAnyOrder("order:1", "tenant:2");
    }

    @Test
    void excluded_types_query_produces_dollar_nin_on_type_field() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(
                DcbCriteria.tags(Set.of(Tag.parse("order:1"))).excludingTypes(Set.of("OrderDeleted")));

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(1);

        Document itemCondition = (Document) orList.get(0);
        Document typeDoc = itemCondition.get(TYPE_FIELD, Document.class);
        assertThat(typeDoc.getList("$nin", String.class)).containsExactlyInAnyOrder("OrderDeleted");
        assertThat(typeDoc).doesNotContainKey("$in");
    }

    @Test
    void type_and_tags_all_of_query_combines_dollar_in_and_dollar_all() {
        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(
                DcbCriteria.types(Set.of("OrderPlaced")).tags(Set.of(Tag.parse("order:1"))));

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(1);

        Document itemCondition = (Document) orList.get(0);
        Document typeDoc = itemCondition.get(TYPE_FIELD, Document.class);
        assertThat(typeDoc.getList("$in", String.class)).containsExactlyInAnyOrder("OrderPlaced");

        Document tagsDoc = itemCondition.get(TAGS_FIELD, Document.class);
        assertThat(tagsDoc.getList("$all", String.class)).containsExactlyInAnyOrder("order:1");
    }

    @Test
    void any_of_multiple_items_produces_one_dollar_or_entry_per_item() {
        DcbCriteria query = DcbCriteria.anyOf(
                DcbCriteria.type("OrderPlaced"),
                DcbCriteria.tags(Set.of(Tag.parse("customer:99")))
        );

        Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(query);

        Document match = stage.get("$match", Document.class);
        assertDcbTagsConditionPresent(match);

        List<?> orList = match.getList("$or", Document.class);
        assertThat(orList).hasSize(2);

        Document first = (Document) orList.get(0);
        Document second = (Document) orList.get(1);

        // One item should have a type condition and the other a tags condition; order not guaranteed.
        boolean firstHasType = first.containsKey(TYPE_FIELD);
        Document typeItem = firstHasType ? first : second;
        Document tagsItem = firstHasType ? second : first;

        assertThat(typeItem.get(TYPE_FIELD, Document.class).getList("$in", String.class)).containsExactlyInAnyOrder("OrderPlaced");
        assertThat(tagsItem.get(TAGS_FIELD, Document.class).getList("$all", String.class)).containsExactlyInAnyOrder("customer:99");
    }

    @Test
    void every_produced_match_stage_contains_the_top_level_dollar_match_key() {
        for (DcbCriteria query : List.of(DcbCriteria.all(), DcbCriteria.type("T"), DcbCriteria.tags(Tag.parse("x:1")))) {
            Document stage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(query);
            assertThat(stage).containsKey("$match");
        }
    }

    private static void assertDcbTagsConditionPresent(Document match) {
        Document tagsCondition = match.get(TAGS_FIELD, Document.class);
        assertThat(tagsCondition).isNotNull();
        assertThat(tagsCondition.getBoolean("$exists")).isTrue();
    }
}
