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
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

/**
 * What an event that {@code updateEvent} damaged before 0.34.0 looks like when stored. The update-event repair tool
 * repairs what {@link #damagedEvent()} matches and counts what {@link #positionLost()} matches as a position it cannot
 * restore. A MongoDB event store with {@code requireRepairedEvents(true)} refuses to start while anything matches
 * {@link #damagedOrUnrecoverable()}, which includes both.
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
     * An event that was written by a DCB append, so it had a position, and no longer has one. The repair cannot put
     * it back, so this is what survives a completed run rather than what a run is looking for. The repair rebuilds
     * such an event's tag array, after which {@link #damagedEvent()} no longer matches it.
     *
     * @return the filter
     */
    public static Bson positionLost() {
        return and(exists(DcbCloudEvents.TAGS), exists(POSITION, false));
    }

    /**
     * An event whose {@code position} is a number at or below zero, which no store assigns. A position read starts
     * above zero, so such an event is missing from it. The repair never writes such a position, so an event gets here
     * only when something outside Occurrent wrote it. A string never matches, since MongoDB compares a number only
     * with numbers.
     *
     * @return the filter
     */
    public static Bson positionNotPositive() {
        return lte(POSITION, 0);
    }

    /**
     * Anything {@link #damagedEvent()}, {@link #positionLost()} or {@link #positionNotPositive()} matches, which is
     * what the repair would still fix and what it reports but cannot fix, other than a numeric position above the
     * store's position counter. Finding that one takes the counter as well, which a filter cannot read. No index
     * covers the tag array half, so this filter reads the whole collection when nothing matches.
     *
     * @return the filter
     */
    public static Bson damagedOrUnrecoverable() {
        return or(damagedEvent(), positionLost(), positionNotPositive());
    }
}
