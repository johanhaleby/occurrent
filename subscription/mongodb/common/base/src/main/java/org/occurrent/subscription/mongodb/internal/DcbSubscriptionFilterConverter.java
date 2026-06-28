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
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbQueryItem;

import java.util.ArrayList;
import java.util.List;

import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.DCB_TAGS_INDEX_FIELD;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.FULL_DOCUMENT;

/**
 * Converts a {@link DcbQuery} into a MongoDB change stream {@code $match} stage, reproducing the
 * {@link DcbCloudEvents#matches(io.cloudevents.CloudEvent, DcbQuery)} semantics server-side: within a query item types
 * are any-of, tags are all-of, and excluded types are none-of, and the items are OR-ed together. A position filter of
 * {@code dcbposition > 0} is always applied so only DCB-tagged events are delivered, matching the in-process guard the
 * DCB subscription DSL uses.
 * <p>
 * The change stream wraps the stored event document under {@value org.occurrent.subscription.mongodb.MongoFilterSpecification#FULL_DOCUMENT},
 * so every field is matched under that prefix. Tag containment matches the {@value DCB_TAGS_INDEX_FIELD} array the event
 * store writes for exactly this purpose, not the newline-joined {@code dcbtags} extension string.
 */
@NullMarked
public final class DcbSubscriptionFilterConverter {

    private static final String TYPE_FIELD = FULL_DOCUMENT + ".type";
    private static final String POSITION_FIELD = FULL_DOCUMENT + "." + DcbCloudEvents.POSITION;
    private static final String TAGS_FIELD = FULL_DOCUMENT + "." + DCB_TAGS_INDEX_FIELD;

    private DcbSubscriptionFilterConverter() {
    }

    /**
     * Returns the single {@code {$match: ...}} aggregation stage that selects the events matching {@code query}.
     */
    public static Document toChangeStreamMatchStage(DcbQuery query) {
        Document conditions = new Document(POSITION_FIELD, new Document("$gt", 0));
        List<DcbQueryItem> items = itemsOf(query);
        if (!items.isEmpty()) {
            List<Document> itemConditions = items.stream()
                    .map(DcbSubscriptionFilterConverter::toItemCondition)
                    .toList();
            conditions.put("$or", itemConditions);
        }
        return new Document("$match", conditions);
    }

    // A bare DcbQueryItem is a single alternative, Items is several, and MatchAll yields no constraint (position only).
    private static List<DcbQueryItem> itemsOf(DcbQuery query) {
        if (query instanceof DcbQueryItem item) {
            return List.of(item);
        }
        if (query instanceof DcbQuery.Items items) {
            return items.items();
        }
        return List.of();
    }

    private static Document toItemCondition(DcbQueryItem item) {
        Document condition = new Document();
        Document typeOperators = new Document();
        if (!item.types().isEmpty()) {
            typeOperators.put("$in", new ArrayList<>(item.types()));
        }
        if (!item.excludedTypes().isEmpty()) {
            typeOperators.put("$nin", new ArrayList<>(item.excludedTypes()));
        }
        if (!typeOperators.isEmpty()) {
            condition.put(TYPE_FIELD, typeOperators);
        }
        if (!item.tags().isEmpty()) {
            condition.put(TAGS_FIELD, new Document("$all", new ArrayList<>(item.tags())));
        }
        return condition;
    }
}
