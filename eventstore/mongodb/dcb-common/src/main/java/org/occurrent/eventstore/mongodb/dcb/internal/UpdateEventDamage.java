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
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

/**
 * What a stored event or position counter looks like when a read cannot handle it the way it handles one a store
 * wrote, starting with the damage {@code updateEvent} did before 0.34.0. The update-event repair tool walks what
 * {@link #damagedEvent()} matches, repairing what it can and reporting the rest, and counts what
 * {@link #positionLost()} matches as a position it cannot restore. A MongoDB event store with
 * {@code requireRepairedEvents(true)} refuses to start while anything matches {@link #wrongPositionOrTagIndex()} or
 * {@link #wrongCounter(Document, Document)} holds. Together those two cover every event whose position is anything
 * other than a positive integer, every DCB event whose {@code dcbtags} and tag index fail {@link #validTags()}, every
 * tag index on an event without {@code dcbtags}, a counter that is not what a writer stores, and every position above
 * the counter, where a missing counter document counts as zero, the value every store reads it as. A non DCB event with
 * no position field at all is in neither, since {@code requireBackfilledPosition} is the check for that one.
 */
@NullMarked
public final class UpdateEventDamage {

    // What DcbCloudEvents joins the tags in a dcbtags string with
    private static final String TAG_SEPARATOR = "\n";

    // Every character String.strip removes, the way Tag strips a tag, so $trim removes exactly those. Its default set
    // differs from Java's in both directions, a no-break space for one.
    private static final String STRIPPED_CHARACTERS = IntStream.rangeClosed(0, Character.MAX_CODE_POINT)
            .filter(Character::isWhitespace)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();

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
     * {@link #validPosition()}, a string being what the old write-back left behind, and an event {@link #wrongTags()}
     * matches. The repair turns a string back into a number where it can, rebuilds {@code dcbtags} and the tag index
     * from a {@code dcbtags} that decodes, and reports any other position or tag field, since nothing else holds the
     * value it lost. A {@code null} or missing position is left to {@link #positionLost()}, so a DCB event whose only
     * damage is a lost position is not in here. The two halves are separate because one update can produce either
     * alone. No index covers the tag half, so this filter reads the whole collection when nothing matches.
     *
     * @return the filter
     */
    public static Bson damagedEvent() {
        return or(
                and(ne(POSITION, null), expr(new Document("$not", List.of(validPosition())))),
                wrongTags()
        );
    }

    /**
     * An event whose tag fields are not what a store writes. That is a DCB event, one with a {@code dcbtags} field,
     * whose fields fail {@link #validTags()}, and an event with a {@code dcbTags} field and no {@code dcbtags}. DCB
     * reads take any event with a {@code dcbTags} field for a DCB event and find it under the tags that field holds,
     * while {@code DcbCloudEvents.getTags} reads the tags from {@code dcbtags}, so that second kind turns up under tags
     * it does not have.
     *
     * @return the filter
     */
    public static Bson wrongTags() {
        return or(
                and(exists(DcbCloudEvents.TAGS), expr(new Document("$not", List.of(validTags())))),
                and(exists(DcbCloudEvents.TAGS, false), exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD))
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
     * valid position is above the counter is what {@link #wrongCounter(Document, Document)} answers. No index
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
     * An aggregation expression that holds when a DCB event's {@code dcbtags} string and {@code dcbTags} index are what
     * a store writes. {@code dcbtags} is a string, every line of it is non-empty and holds nothing
     * {@code String.strip} would remove, the form {@code Tag} gives a tag, and the index holds the same set of tags as
     * those lines, none when the string is empty. {@code DcbCloudEvents.decodeTags} fails on an empty line, and strips a
     * line that DCB reads, which find an event by its index alone, match unstripped. An index that is missing, is not an
     * array or names other tags hides the event from those reads. Order and repeats change neither, so they do not
     * count here. {@code $and} stops at the first false clause, so {@code $split} only ever sees a string.
     * {@link #listedTags(String)} reads a {@code dcbtags} string the same way.
     *
     * @return the expression, for use inside {@code $expr}
     */
    public static Document validTags() {
        String tags = "$" + DcbCloudEvents.TAGS;
        String index = "$" + DcbDocumentMapper.DCB_TAGS_INDEX_FIELD;
        Document lines = new Document("$cond", List.of(
                new Document("$eq", List.of(tags, "")),
                List.of(),
                new Document("$split", List.of(tags, TAG_SEPARATOR))));
        Document lineIsATag = new Document("$and", List.of(
                new Document("$ne", List.of("$$line", "")),
                new Document("$eq", List.of("$$line", new Document("$trim", new Document("input", "$$line").append("chars", STRIPPED_CHARACTERS))))
        ));
        return new Document("$and", List.of(
                new Document("$eq", List.of(new Document("$type", tags), "string")),
                new Document("$isArray", index),
                new Document("$allElementsTrue", List.of(new Document("$map", new Document("input", lines).append("as", "line").append("in", lineIsATag)))),
                new Document("$setEquals", List.of(index, lines))
        ));
    }

    /**
     * The tags a {@code dcbtags} string lists, as {@link #validTags()} reads them. A store writes every tag in its
     * canonical form, so for an event it wrote these are the event's tags. For a {@code dcbtags} that decodes, they
     * differ from the decoded tags exactly when {@link #validTags()} refuses one of its lines.
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
     * Whether the store's position counter is one the stores cannot use. Every store reads a missing counter document
     * as zero, so DCB reads and reads in position order, which stop at the counter, return nothing and the next append
     * reserves a position an event already holds. A counter value {@link #counterValue(Object)} rejects is read wrongly,
     * not at all, or rounded by the next append. So this holds for a counter document with such a value, and for a highest position above the
     * counter, zero when there is no counter document. A collection with no positioned event and no counter document is
     * how every store starts, and passes. Ask only once {@link #wrongPositionOrTagIndex()} has found nothing, so that
     * every position is valid. Read {@code highestPositioned} first and {@code counter} second. Every writer raises the
     * counter before the position it reserved becomes visible, and nothing lowers it, so a counter read after the
     * highest position is at least that position, even with appends in flight.
     *
     * @param highestPositioned the event with the highest {@code position}, or {@code null} if there is none
     * @param counter           the position counter document, or {@code null} if there is none
     * @return {@code true} if the counter is unreadable or below the highest position
     */
    public static boolean wrongCounter(@Nullable Document highestPositioned, @Nullable Document counter) {
        long ceiling = 0;
        if (counter != null) {
            Long value = counterValue(counter.get(DcbMarkerModel.COUNTER_POSITION));
            if (value == null) {
                return true;
            }
            ceiling = value;
        }
        if (highestPositioned == null) {
            return false;
        }
        BigDecimal highest = finiteNumber(highestPositioned.get(POSITION));
        return highest != null && highest.compareTo(BigDecimal.valueOf(ceiling)) > 0;
    }

    /**
     * The value a counter document's {@code position} holds if it is what a writer stores. Every writer stores an int32
     * or an int64 there, the stores and the position backfill with {@code $inc} of an int or a long and the backfill's
     * seed with {@code $max} of a long, and {@code $inc} keeps either exact. A {@code double} counter, which is what
     * mongosh stores for a bare number outside the int32 range, rounds once {@code $inc} takes it past 2^53, so two
     * appends can reserve the same position, and the stores read a {@code Decimal128} through a {@code double}, which
     * can round it above 2^53. A negative counter makes the next append reserve a position at or below zero.
     *
     * @param stored the stored counter value
     * @return the counter, or {@code null} if {@code stored} is not an int32 or int64 at or above zero
     */
    public static @Nullable Long counterValue(@Nullable Object stored) {
        return (stored instanceof Integer || stored instanceof Long) && ((Number) stored).longValue() >= 0
                ? ((Number) stored).longValue()
                : null;
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
