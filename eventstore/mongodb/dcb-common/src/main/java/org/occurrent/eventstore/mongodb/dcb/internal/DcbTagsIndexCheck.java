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
import org.bson.conversions.Bson;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.jspecify.annotations.NullMarked;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper.DCB_TAGS_INDEX_FIELD;

/**
 * The startup check a MongoDB event store with {@code DCB} and without {@code STREAM} runs when its event collection
 * has no {@code dcbTags} index. Without that index a match-all DCB query reads every stream event that has a
 * {@code position}, so the store warns when it finds one.
 * <p>
 * Send {@link #positionedStreamEvent()} with {@link #hint()}, {@link #min()}, {@link #max()} and a limit of 1. The
 * bounds keep the scan to the index keys whose {@code dcbTags} is null, so a collection holding only DCB events reads
 * no keys at all.
 */
@NullMarked
public final class DcbTagsIndexCheck {

    private DcbTagsIndexCheck() {
    }

    /**
     * @param index a document that {@code listIndexes} returns
     * @return {@code true} if the index is keyed on {@code dcbTags} ascending and nothing else
     */
    public static boolean isDcbTagsIndex(Document index) {
        return index.get("key") instanceof Document key
                && key.size() == 1
                && key.get(DCB_TAGS_INDEX_FIELD) instanceof Number direction
                && direction.doubleValue() == 1;
    }

    /**
     * @return a filter matching a stream event that has a numeric {@code position}
     */
    public static Bson positionedStreamEvent() {
        return and(exists(DCB_TAGS_INDEX_FIELD, false), type(POSITION, "number"));
    }

    /**
     * @return the key of the {@code (dcbTags, position)} index the store creates whenever {@code DCB} is enabled
     */
    public static Bson hint() {
        return new Document(DCB_TAGS_INDEX_FIELD, 1).append(POSITION, 1);
    }

    /**
     * @return the lower bound, inclusive, of the index keys whose {@code dcbTags} is null
     */
    public static Bson min() {
        return new Document(DCB_TAGS_INDEX_FIELD, null).append(POSITION, new MinKey());
    }

    /**
     * @return the upper bound, exclusive, of the index keys whose {@code dcbTags} is null
     */
    public static Bson max() {
        return new Document(DCB_TAGS_INDEX_FIELD, null).append(POSITION, new MaxKey());
    }

    /**
     * @param collectionName the event collection
     * @return the warning a store logs when the check finds a stream event that has a {@code position}
     */
    public static String missingIndexMessage(String collectionName) {
        return "The event collection '" + collectionName + "' holds stream events that have a position, and it has no"
                + " index on dcbTags alone. A store with DCB and without STREAM doesn't create that index. Without it, a"
                + " read, count or exists with DcbCriteria.all(), and the append check of"
                + " DcbAppendCondition.wholeStoreLock(), read every stream event that has a position in the range as"
                + " well as the DCB events. The results are correct, only slower. Create the index with"
                + " db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true })";
    }

    /**
     * @param collectionName the event collection
     * @return what a store logs when the check itself fails, before it starts anyway
     */
    public static String checkFailedMessage(String collectionName) {
        return "Couldn't check whether the event collection '" + collectionName + "' holds stream events that have a"
                + " position and no index on dcbTags alone. The store starts anyway. If the collection holds such events, create the index with"
                + " db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true })";
    }
}
