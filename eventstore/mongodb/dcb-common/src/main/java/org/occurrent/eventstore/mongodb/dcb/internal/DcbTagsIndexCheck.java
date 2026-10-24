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
 * Send {@link #positionedStreamEvent()} with {@link #hint()}, {@link #min()}, {@link #max()} and a limit of 1 first.
 * The bounds keep the scan to the index keys whose {@code dcbTags} is null and whose {@code position} is a number, so
 * a collection holding only DCB events reads no keys at all, and the store never lists its indexes. Only when that
 * finds an event, call {@link #warningFor(String, Iterable)} with the collection's indexes and log the warning it
 * returns.
 */
@NullMarked
public final class DcbTagsIndexCheck {

    private static final String SPARSE = "sparse";
    private static final String PARTIAL_FILTER_EXPRESSION = "partialFilterExpression";
    private static final String HIDDEN = "hidden";
    private static final String EXISTS = "$exists";
    private static final String AND = "$and";

    private DcbTagsIndexCheck() {
    }

    /**
     * @param collectionName the event collection
     * @param indexes        the documents {@code listIndexes} returns for it
     * @return {@code null} when one of the indexes is keyed on {@code dcbTags} and nothing else, ascending or
     * descending, isn't hidden, and either is sparse without a {@code partialFilterExpression} or has the
     * {@code partialFilterExpression} {@code { dcbTags: { $exists: true } }}, or an {@code $and} of one or more
     * copies of it. Otherwise the warning to log, {@link #unusableIndexMessage(String, List)} naming every index on
     * {@code dcbTags} alone when there are any, {@link #missingIndexMessage(String)} when there are none.
     */
    public static @Nullable String warningFor(String collectionName, Iterable<Document> indexes) {
        List<Document> unusableIndexes = new ArrayList<>();
        for (Document index : indexes) {
            if (isKeyedOnDcbTagsAlone(index)) {
                if (isUsable(index)) {
                    return null;
                }
                unusableIndexes.add(index);
            }
        }
        return unusableIndexes.isEmpty() ? missingIndexMessage(collectionName) : unusableIndexMessage(collectionName, unusableIndexes);
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
                + " index on dcbTags alone. A store with DCB and without STREAM doesn't create that index."
                + whatTheIndexDoes() + " Create the index with db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true })";
    }

    /**
     * @param collectionName the event collection
     * @param indexes        the documents {@code listIndexes} returns for the indexes on {@code dcbTags} alone, each
     *                       of them hidden, with a {@code partialFilterExpression} that isn't
     *                       {@code { dcbTags: { $exists: true } }}, or with neither that nor {@code sparse}
     * @return the warning a store logs when the collection has those indexes, no usable one, and a stream event that
     * has a {@code position}. It names every index and ends with the commands that leave one usable index. The
     * commands unhide an index that is only hidden, and drop all the others, or drop all of them and create the
     * sparse one.
     */
    public static String unusableIndexMessage(String collectionName, List<Document> indexes) {
        if (indexes.isEmpty()) {
            throw new IllegalArgumentException("indexes cannot be empty");
        }
        StringBuilder message = new StringBuilder("The event collection '").append(collectionName)
                .append("' holds stream events that have a position, and ")
                .append(indexes.size() == 1 ? "its index on dcbTags alone can't be used." : "none of its indexes on dcbTags alone can be used.");
        for (Document index : indexes) {
            message.append(" '").append(indexName(index)).append("' ").append(String.join(" and ", reasonsWhyUnusable(index))).append('.');
        }
        message.append(" A store with DCB and without STREAM only counts an index on dcbTags alone that isn't hidden and is"
                + " either sparse or partial on { dcbTags: { $exists: true } }.").append(whatTheIndexDoes())
                .append(' ').append(fix(collectionName, indexes));
        return message.toString();
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
                + " db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true }) creates it,"
                + " unless an index named dcbTags_1 with other options is already there";
    }

    private static boolean isKeyedOnDcbTagsAlone(Document index) {
        return index.get("key") instanceof Document key
                && key.size() == 1
                && key.get(DCB_TAGS_INDEX_FIELD) instanceof Number direction
                && Math.abs(direction.doubleValue()) == 1;
    }

    private static boolean isUsable(Document index) {
        return holdsOnlyDcbEvents(index) && !isTrue(index.get(HIDDEN));
    }

    private static boolean holdsOnlyDcbEvents(Document index) {
        return index.containsKey(PARTIAL_FILTER_EXPRESSION)
                ? isTheMatchAllPredicate(index.get(PARTIAL_FILTER_EXPRESSION))
                : isTrue(index.get(SPARSE));
    }

    private static List<String> reasonsWhyUnusable(Document index) {
        List<String> reasons = new ArrayList<>();
        if (!holdsOnlyDcbEvents(index)) {
            reasons.add(index.containsKey(PARTIAL_FILTER_EXPRESSION)
                    ? "has a partialFilterExpression other than { dcbTags: { $exists: true } }"
                    : "isn't sparse");
        }
        if (isTrue(index.get(HIDDEN))) {
            reasons.add("is hidden");
        }
        return reasons;
    }

    private static String whatTheIndexDoes() {
        return " Without a usable index, count, exists and the append check of DcbAppendCondition.wholeStoreLock() read"
                + " stream events as well as DCB events, and so can a read with DcbCriteria.all(). The results are correct,"
                + " only slower. With one, those three and a read with DcbCriteria.all() read only DCB events when"
                + " MongoDB uses the index. It may not for a backward read or a read with a limit, which can still walk"
                + " the position index.";
    }

    // Leaves one usable index. A hidden index that holds only DCB events is unhidden, since that builds nothing,
    // and every other index is dropped before the sparse one is created, because createIndex fails with
    // IndexKeySpecsConflict while another index still has the name dcbTags_1.
    private static String fix(String collectionName, List<Document> indexes) {
        Document unhideable = indexes.stream().filter(index -> holdsOnlyDcbEvents(index) && isTrue(index.get(HIDDEN))).findFirst().orElse(null);
        List<String> commands = new ArrayList<>();
        if (unhideable != null) {
            commands.add("db.runCommand({ collMod: \"" + collectionName + "\", index: { name: \"" + escaped(indexName(unhideable)) + "\", hidden: false } })");
        }
        for (Document index : indexes) {
            if (index != unhideable) {
                commands.add("db." + collectionName + ".dropIndex(\"" + escaped(indexName(index)) + "\")");
            }
        }
        if (unhideable == null) {
            commands.add("db." + collectionName + ".createIndex({ dcbTags: 1 }, { sparse: true })");
        }
        return (commands.size() == 1 ? "Run " : "Run, in this order, ") + String.join(" and then ", commands)
                + (commands.size() > 1 ? ". Dropping an index removes it, so check that nothing else uses it first" : "");
    }

    private static String indexName(Document index) {
        return String.valueOf(index.get("name"));
    }

    private static String escaped(String name) {
        return name.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static boolean isTheMatchAllPredicate(@Nullable Object partialFilterExpression) {
        return isExistsDcbTags(partialFilterExpression)
                || partialFilterExpression instanceof Document filter
                && filter.size() == 1
                && filter.get(AND) instanceof List<?> operands
                && !operands.isEmpty()
                && operands.stream().allMatch(DcbTagsIndexCheck::isExistsDcbTags);
    }

    private static boolean isExistsDcbTags(@Nullable Object filter) {
        return filter instanceof Document document
                && document.size() == 1
                && document.get(DCB_TAGS_INDEX_FIELD) instanceof Document predicate
                && predicate.size() == 1
                && isTrue(predicate.get(EXISTS));
    }

    private static boolean isTrue(@Nullable Object option) {
        return option instanceof Boolean b ? b : option instanceof Number n && n.doubleValue() != 0;
    }
}
