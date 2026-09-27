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
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

/**
 * What a stored event whose position or tag index is wrong looks like, starting with the damage {@code updateEvent}
 * did before 0.34.0. The update-event repair tool repairs what {@link #damagedEvent()} matches and counts what
 * {@link #positionLost()} matches as a position it cannot restore. A MongoDB event store with
 * {@code requireRepairedEvents(true)} refuses to start while anything matches {@link #wrongPositionOrMissingTagIndex()}
 * or {@link #positionAboveCounter(Document, Document)} holds. Together those two cover every event whose position is
 * anything other than a positive integer no greater than the store's position counter, and every DCB event without
 * its tag index. A non DCB event with no position field at all is in neither, since
 * {@code requireBackfilledPosition} is the check for that one.
 */
@NullMarked
public final class UpdateEventDamage {

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
     * An event whose {@code position} is a string, or that has the {@code dcbtags} extension without the indexed
     * {@code dcbTags} array derived from it. The two are separate because one update can produce either alone. An
     * event with no DCB tags only ever loses its position, and a second update of an already repaired event would
     * restore neither on its own. The second half matches whatever {@code position} the event has, a number, none at
     * all, or a string. No index covers it, so this filter reads the whole collection when nothing matches.
     *
     * @return the filter
     */
    public static Bson damagedEvent() {
        return or(
                positionStoredAsString(),
                and(exists(DcbCloudEvents.TAGS), exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, false))
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
     * is an event with a {@code position} field that fails {@link #validPosition()}, a DCB event with no
     * {@code position} or a {@code null} one, and a DCB event without its tag index. A non DCB event with no
     * {@code position} field is left to {@code requireBackfilledPosition}. Whether a valid position is above the
     * counter is what {@link #positionAboveCounter(Document, Document)} answers. No index narrows the expression or
     * covers the tag index half, so this filter reads the whole collection when nothing matches.
     *
     * @return the filter
     */
    public static Bson wrongPositionOrMissingTagIndex() {
        return or(
                and(exists(POSITION), expr(new Document("$not", List.of(validPosition())))),
                positionLost(),
                and(exists(DcbCloudEvents.TAGS), exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, false))
        );
    }

    /**
     * An aggregation expression that holds when {@code position} is a positive integer that fits in a {@code long}, the
     * only kind of value a store assigns. It is written as what a valid position is rather than as a list of what is
     * wrong, since BSON has more kinds of wrong value than anyone lists. It is false for a missing field, {@code null},
     * a string, an array, any other type, {@code NaN}, an infinity, zero, a negative number and a number with a
     * fraction. {@code $and} stops at the first false clause, so {@code $trunc} only ever sees a number.
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
     * An event whose {@code position} is a number. Sorted by {@code position} descending, the first match holds the
     * highest position in the collection, and the {@code position} index serves that sort. An array holding a number
     * matches too and can come first, so this finds the highest position only once
     * {@link #wrongPositionOrMissingTagIndex()} has found nothing.
     *
     * @return the filter
     */
    public static Bson positionIsANumber() {
        return type(POSITION, "number");
    }

    /**
     * Whether the highest position in the collection is above the store's position counter. No store assigns such a
     * value, and DCB reads and reads in position order skip it, since they stop at the counter. Ask only once
     * {@link #wrongPositionOrMissingTagIndex()} has found nothing, so that every position is valid. Read
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
        Object highest = highestPositioned.get(POSITION);
        Object ceiling = counter.get(DcbMarkerModel.COUNTER_POSITION);
        if (!(highest instanceof Number highestNumber) || !(ceiling instanceof Number ceilingNumber)) {
            return false;
        }
        return exactly(highestNumber).compareTo(exactly(ceilingNumber)) > 0;
    }

    // Exact for any valid position, where comparing doubles would round a Decimal128 position above 2^53 onto the
    // counter below it
    private static BigDecimal exactly(Number number) {
        if (number instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue();
        }
        if (number instanceof Long || number instanceof Integer) {
            return BigDecimal.valueOf(number.longValue());
        }
        return new BigDecimal(number.doubleValue());
    }
}
