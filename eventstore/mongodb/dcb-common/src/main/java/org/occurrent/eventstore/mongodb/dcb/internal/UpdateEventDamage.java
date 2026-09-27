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

import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

/**
 * What a stored event whose position or tag index is wrong looks like, starting with the damage {@code updateEvent}
 * did before 0.34.0. The update-event repair tool walks what {@link #damagedEvent()} matches, repairing what it can and
 * reporting the rest, and counts what {@link #positionLost()} matches as a position it cannot restore. A MongoDB event
 * store with {@code requireRepairedEvents(true)} refuses to start while anything matches
 * {@link #wrongPositionOrTagIndex()} or {@link #positionAboveCounter(Document, Document)} holds. Together those two
 * cover every event whose position is anything other than a positive integer, every DCB event whose tag index does not
 * hold exactly the tags its {@code dcbtags} lists, and, where the store has a position counter document, every
 * position above that counter. A non DCB event with no position field at all is in neither, since
 * {@code requireBackfilledPosition} is the check for that one.
 */
@NullMarked
public final class UpdateEventDamage {

    // What DcbCloudEvents joins the tags in a dcbtags string with
    private static final String TAG_SEPARATOR = "\n";

    private UpdateEventDamage() {
    }

    /**
     * An event whose {@code position} is a string, which is what the old write-back's coercion left behind. The
     * {@code position} index keeps strings in their own type range, so where that index exists this reads no keys on
     * a collection that was never damaged.
     *
     * @return the filter
     */
    public static Bson positionStoredAsString() {
        return type(POSITION, BsonType.STRING);
    }

    /**
     * An event the repair tool looks at. That is an event whose {@code position} holds a value that fails
     * {@link #validPosition()}, a string being what the old write-back left behind, and a DCB event whose tag index
     * fails {@link #tagIndexMatchesTags()}. The repair turns a string back into a number where it can, rebuilds a tag
     * index from {@code dcbtags}, and reports any other position, since nothing else holds the value it lost. A
     * {@code null} or missing position is left to {@link #positionLost()}. The two halves are separate because one
     * update can produce either alone. No index covers the tag half, so this filter reads the whole collection when
     * nothing matches.
     *
     * @return the filter
     */
    public static Bson damagedEvent() {
        return or(
                and(ne(POSITION, null), expr(new Document("$not", List.of(validPosition())))),
                and(exists(DcbCloudEvents.TAGS), expr(new Document("$not", List.of(tagIndexMatchesTags()))))
        );
    }

    /**
     * An event that was written by a DCB append, so it had a position, and now has none, either because the field is
     * gone or because it holds {@code null}. The repair reads both the same way and cannot put the position back, so
     * this is what survives a completed run rather than what a run is looking for. The repair rebuilds such an event's
     * tag array, after which {@link #damagedEvent()} no longer matches it.
     *
     * @return the filter
     */
    public static Bson positionLost() {
        return and(exists(DcbCloudEvents.TAGS), eq(POSITION, null));
    }

    /**
     * Every event whose position or tag index is wrong, as far as a filter can tell without the position counter. That
     * is everything {@link #damagedEvent()} and {@link #positionLost()} match, and a {@code null} position on a non DCB
     * event. A non DCB event with no {@code position} field is left to {@code requireBackfilledPosition}. Whether a
     * valid position is above the counter is what {@link #positionAboveCounter(Document, Document)} answers. No index
     * narrows either expression, so this filter reads the whole collection when nothing matches.
     *
     * @return the filter
     */
    public static Bson wrongPositionOrTagIndex() {
        return or(damagedEvent(), positionLost(), type(POSITION, BsonType.NULL));
    }

    /**
     * An aggregation expression that holds when {@code position} is a positive integer that fits in a {@code long}, the
     * only kind of value a store assigns. It is written as what a valid position is rather than as a list of what is
     * wrong, since BSON has more kinds of wrong value than anyone lists. It is false for a missing field, {@code null},
     * a string, an array, any other type, {@code NaN}, an infinity, zero, a negative number and a number with a
     * fraction. {@code $and} stops at the first false clause, so {@code $trunc} only ever sees a number.
     * {@link #validPositionValue(Object)} answers the same question for a value already read.
     *
     * @return the expression, for use inside {@code $expr}
     */
    public static Document validPosition() {
        String position = "$" + POSITION;
        return new Document("$and", List.of(
                new Document("$isNumber", position),
                new Document("$gt", List.of(position, 0)),
                new Document("$lte", List.of(position, Long.MAX_VALUE)),
                new Document("$eq", List.of(position, new Document("$trunc", position)))
        ));
    }

    /**
     * An aggregation expression that holds when a DCB event's tag index, the {@code dcbTags} array, holds the same tags
     * as its {@code dcbtags} string lists, one per line, and none when that string is empty. That is what every store
     * writes. DCB reads and the conflict query behind a conditional append find an event by its index alone, so an
     * index that is missing, is not an array, or names other tags hides the event from them. Order and repeats do not
     * change what those reads find, so they do not count here either. It is false when {@code dcbtags} is not a
     * string. {@code $and} stops at the first false clause, so {@code $split} only ever sees a string.
     * {@link #listedTags(String)} reads a {@code dcbtags} string the same way.
     *
     * @return the expression, for use inside {@code $expr}
     */
    public static Document tagIndexMatchesTags() {
        String tags = "$" + DcbCloudEvents.TAGS;
        String index = "$" + DcbDocumentMapper.DCB_TAGS_INDEX_FIELD;
        Document listed = new Document("$cond", List.of(
                new Document("$eq", List.of(tags, "")),
                List.of(),
                new Document("$split", List.of(tags, TAG_SEPARATOR))));
        return new Document("$and", List.of(
                new Document("$eq", List.of(new Document("$type", tags), "string")),
                new Document("$isArray", index),
                new Document("$setEquals", List.of(index, listed))
        ));
    }

    /**
     * The tags a {@code dcbtags} string lists, as {@link #tagIndexMatchesTags()} reads them. A store writes every tag
     * in its canonical form, so for an event it wrote these are the event's tags.
     *
     * @param encodedTags the {@code dcbtags} string
     * @return each line of {@code encodedTags} exactly as written, or none for an empty string
     */
    public static Set<String> listedTags(String encodedTags) {
        return encodedTags.isEmpty() ? Set.of() : new HashSet<>(Arrays.asList(encodedTags.split(TAG_SEPARATOR, -1)));
    }

    /**
     * An event whose {@code position} is a number. Sorted by {@code position} descending, the first match holds the
     * highest position in the collection, and the {@code position} index serves that sort. An array holding a number
     * matches too and can come first, so this finds the highest position only once
     * {@link #wrongPositionOrTagIndex()} has found nothing.
     *
     * @return the filter
     */
    public static Bson positionIsANumber() {
        return type(POSITION, "number");
    }

    /**
     * Whether the highest position in the collection is above the store's position counter. No store assigns such a
     * value, and DCB reads and reads in position order skip it, since they stop at the counter. Ask only once
     * {@link #wrongPositionOrTagIndex()} has found nothing, so that every position is valid. Read
     * {@code highestPositioned} first and {@code counter} second. Every writer raises the counter before the position
     * it reserved becomes visible, and nothing lowers it, so a counter read after the highest position is at least
     * that position, even with appends in flight. With no counter document there is nothing to compare against, and
     * the answer is no, as it is in the repair tool.
     *
     * @param highestPositioned the event with the highest {@code position}, or {@code null} if there is none
     * @param counter           the position counter document, or {@code null} if there is none
     * @return {@code true} if the highest position is above the counter
     */
    public static boolean positionAboveCounter(@Nullable Document highestPositioned, @Nullable Document counter) {
        if (highestPositioned == null || counter == null) {
            return false;
        }
        BigDecimal highest = finiteNumber(highestPositioned.get(POSITION));
        BigDecimal ceiling = finiteNumber(counter.get(DcbMarkerModel.COUNTER_POSITION));
        return highest != null && ceiling != null && highest.compareTo(ceiling) > 0;
    }

    /**
     * The position {@code stored} holds if it passes {@link #validPosition()}, a positive integer that fits in a
     * {@code long}, so the repair tool and the stores agree on which stored values are positions.
     *
     * @param stored the stored {@code position} value
     * @return the position, or {@code null} if {@code stored} is anything else
     */
    public static @Nullable Long validPositionValue(@Nullable Object stored) {
        Long whole = wholeNumber(stored);
        return whole != null && whole > 0 ? whole : null;
    }

    /**
     * The value {@code stored} holds if it is a whole number that fits in a {@code long}, the stored counterpart of a
     * string {@link Long#parseLong(String)} accepts. {@link #validPositionValue(Object)} is this and above zero.
     *
     * @param stored the stored {@code position} value
     * @return the whole number, or {@code null} for a fraction, a value beyond a {@code long} and anything that is not
     * a finite number
     */
    public static @Nullable Long wholeNumber(@Nullable Object stored) {
        BigDecimal number = finiteNumber(stored);
        if (number == null) {
            return null;
        }
        try {
            return number.longValueExact();
        } catch (ArithmeticException e) {
            // A fraction, or a value beyond Long.MIN_VALUE or Long.MAX_VALUE
            return null;
        }
    }

    /**
     * The exact value of a BSON number, so that neither a comparison nor a check for a fraction goes through a
     * {@code double}, which rounds a {@code Decimal128} above 2^53.
     *
     * @param value a stored value
     * @return the value of an int32, int64, double or {@code Decimal128} that is neither {@code NaN} nor an infinity,
     * with negative zero as zero, or {@code null} for anything else
     */
    public static @Nullable BigDecimal finiteNumber(@Nullable Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Double number) {
            return Double.isFinite(number) ? new BigDecimal(number) : null;
        }
        if (value instanceof Decimal128 decimal) {
            if (decimal.isNaN() || decimal.isInfinite()) {
                return null;
            }
            try {
                return decimal.bigDecimalValue();
            } catch (ArithmeticException e) {
                // Negative zero, the one finite value BigDecimal has no form for
                return BigDecimal.ZERO;
            }
        }
        return null;
    }
}
