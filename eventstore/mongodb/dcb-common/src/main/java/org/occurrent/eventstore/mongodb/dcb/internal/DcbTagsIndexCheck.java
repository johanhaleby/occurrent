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
import org.bson.types.MinKey;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;
import static org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper.DCB_TAGS_INDEX_FIELD;

/**
 * The startup check a MongoDB event store with {@code DCB} and without {@code STREAM} runs on its event collection.
 * Without a usable index on {@code dcbTags} alone, a match-all DCB query reads every stream event that has a
 * {@code position} in the range, so the store warns when the collection has no such index and holds such an event.
 * <p>
 * Call {@link #warningFor(String, Iterable)} with the collection's indexes. Only when it returns a warning, send
 * {@link #positionedStreamEvent()} with {@link #hint()}, {@link #min()}, {@link #max()} and a limit of 1, and log the
 * warning if that finds an event. The bounds keep the scan to the index keys whose {@code dcbTags} is null and whose
 * {@code position} is a number, so a collection holding only DCB events reads no keys at all.
 */
@NullMarked
public final class DcbTagsIndexCheck {

    private static final String SPARSE = "sparse";
    private static final String PARTIAL_FILTER_EXPRESSION = "partialFilterExpression";
    private static final String HIDDEN = "hidden";
    private static final String EXISTS = "$exists";

    private DcbTagsIndexCheck() {
    }

    /**
     * @param collectionName the event collection
     * @param indexes        the documents {@code listIndexes} returns for it
     * @return {@code null} when one of the indexes is keyed on {@code dcbTags} and nothing else, ascending or
     * descending, isn't hidden, and either is sparse without a {@code partialFilterExpression} or has the
     * {@code partialFilterExpression} {@code { dcbTags: { $exists: true } }}. Otherwise the warning to log if the
     * collection holds a stream event that has a {@code position}, {@link #unusableIndexMessage(String, Document)}
     * when there's an index on {@code dcbTags} alone that fails one of those conditions,
     * {@link #missingIndexMessage(String)} when there's none.
     */
    public static @Nullable String warningFor(String collectionName, Iterable<Document> indexes) {
        Document unusableIndex = null;
        for (Document index : indexes) {
            if (isKeyedOnDcbTagsAlone(index)) {
                if (isUsable(index)) {
                    return null;
                }
                if (unusableIndex == null) {
                    unusableIndex = index;
                }
            }
        }
        return unusableIndex == null ? missingIndexMessage(collectionName) : unusableIndexMessage(collectionName, unusableIndex);
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
     * @return the upper bound, exclusive, of the index keys whose {@code dcbTags} is null and whose {@code position}
     * is a number. MongoDB sorts every number before every string, and the empty string before every other string, so
     * the scan stops before the first stream event whose {@code position} is a string.
     */
    public static Bson max() {
        return new Document(DCB_TAGS_INDEX_FIELD, null).append(POSITION, "");
    }

    /**
     * @param collectionName the event collection
     * @return the warning a store logs when the collection has no index on {@code dcbTags} alone and holds a stream
     * event that has a {@code position}
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
     * @param index          the document {@code listIndexes} returns for an index on {@code dcbTags} alone that is
     *                       hidden, has a {@code partialFilterExpression} other than
     *                       {@code { dcbTags: { $exists: true } }}, or has neither that nor {@code sparse}
     * @return the warning a store logs when the collection has that index, no usable one, and a stream event that has
     * a {@code position}
     */
    public static String unusableIndexMessage(String collectionName, Document index) {
        String indexName = String.valueOf(index.get("name"));
        List<String> reasons = new ArrayList<>();
        if (index.containsKey(PARTIAL_FILTER_EXPRESSION)) {
            if (!isTheMatchAllPredicate(index.get(PARTIAL_FILTER_EXPRESSION))) {
                reasons.add("has a partialFilterExpression other than { dcbTags: { $exists: true } }");
            }
        } else if (!isTrue(index.get(SPARSE))) {
            reasons.add("isn't sparse");
        }
        boolean hidden = isTrue(index.get(HIDDEN));
        if (hidden) {
            reasons.add("is hidden");
        }
        String fix = hidden && reasons.size() == 1
                ? "Unhide it with db.runCommand({ collMod: \"" + collectionName + "\", index: { name: \"" + indexName + "\", hidden: false } })"
                : "Replace it with db." + collectionName + ".dropIndex(\"" + indexName + "\") and then"
                + " db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true })";
        return "The event collection '" + collectionName + "' holds stream events that have a position, and its index '"
                + indexName + "' on dcbTags alone " + String.join(" and ", reasons) + ". A store with DCB and without"
                + " STREAM only counts an index on dcbTags alone that isn't hidden and is either sparse or partial on"
                + " { dcbTags: { $exists: true } } as the index that lets a read, count or exists with"
                + " DcbCriteria.all(), and the append check of DcbAppendCondition.wholeStoreLock(), read the DCB events without the stream events. The results"
                + " are correct either way. " + fix;
    }

    /**
     * @param collectionName the event collection
     * @return what a store logs when the check itself fails, before it starts anyway
     */
    public static String checkFailedMessage(String collectionName) {
        return "Couldn't check whether the event collection '" + collectionName + "' holds stream events that have a"
                + " position and no index on dcbTags alone that isn't hidden and is either sparse or partial on"
                + " { dcbTags: { $exists: true } }."
                + " The store starts anyway. If the collection holds such events, it needs that index, and"
                + " db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true }) creates it";
    }

    private static boolean isKeyedOnDcbTagsAlone(Document index) {
        return index.get("key") instanceof Document key
                && key.size() == 1
                && key.get(DCB_TAGS_INDEX_FIELD) instanceof Number direction
                && Math.abs(direction.doubleValue()) == 1;
    }

    private static boolean isUsable(Document index) {
        boolean holdsOnlyDcbEvents = index.containsKey(PARTIAL_FILTER_EXPRESSION)
                ? isTheMatchAllPredicate(index.get(PARTIAL_FILTER_EXPRESSION))
                : isTrue(index.get(SPARSE));
        return holdsOnlyDcbEvents && !isTrue(index.get(HIDDEN));
    }

    private static boolean isTheMatchAllPredicate(@Nullable Object partialFilterExpression) {
        return partialFilterExpression instanceof Document filter
                && filter.size() == 1
                && filter.get(DCB_TAGS_INDEX_FIELD) instanceof Document predicate
                && predicate.size() == 1
                && isTrue(predicate.get(EXISTS));
    }

    private static boolean isTrue(@Nullable Object option) {
        return option instanceof Boolean b ? b : option instanceof Number n && n.doubleValue() != 0;
    }
}
