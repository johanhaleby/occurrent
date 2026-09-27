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

package org.occurrent.testsupport.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.stream.Stream;

/**
 * Every shape a stored position counter can take, and whether a MongoDB event store with
 * {@code requireRepairedEvents(true)} starts over it. Every writer stores the counter as an int32 or an int64 and
 * {@code $inc} keeps either exact, while a {@code double} rounds once {@code $inc} takes it past 2^53 and a
 * {@code Decimal128} is read through a {@code double}. A missing counter document reads as zero. So the store starts only
 * when the counter is an int32 or int64 at or above zero and at least the highest position, with a missing document
 * counting as zero.
 * <p>
 * The cases in {@link #shapes()} start from a collection holding two events at positions 1 and 2, with the counter at
 * {@link StoredPositionShapes#COUNTER}, and the cases in {@link #shapesOverUnpositionedEvents()} from a collection whose
 * events have no position. Each gives the counter document its shape.
 */
public final class StoredCounterShapes {

    private static final Object NO_DOCUMENT = new Object();
    private static final Object NO_FIELD = new Object();
    private static final long TWO_TO_THE_53 = 1L << 53;

    private StoredCounterShapes() {
    }

    /**
     * One counter.
     *
     * @param description what the counter is
     * @param value       the value to store, or a private marker for no document or no field
     * @param starts      whether the store starts over it
     */
    public record Shape(String description, @Nullable Object value, boolean starts) {

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * @return every counter
     */
    public static Stream<Shape> shapes() {
        long counter = StoredPositionShapes.COUNTER;
        return Stream.of(
                new Shape("the long 2, as the native store writes it", counter, true),
                new Shape("the int 2, as the Spring stores write it", (int) counter, true),
                new Shape("the long 99, above the highest position", 99L, true),
                new Shape("the long 2^53 + 1, which $inc keeps exact", TWO_TO_THE_53 + 1, true),
                new Shape("the double 2.0, which $inc rounds past 2^53", (double) counter, false),
                new Shape("the double 2^53, where $inc of one leaves it unchanged", (double) TWO_TO_THE_53, false),
                new Shape("the decimal 2", new Decimal128(counter), false),
                new Shape("the decimal 2^53 + 1, which longValue reads through a double", new Decimal128(new BigDecimal(TWO_TO_THE_53 + 1)), false),
                new Shape("the decimal one above the largest long", new Decimal128(new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE)), false),
                new Shape("the double 2.5", 2.5d, false),
                new Shape("NaN", Double.NaN, false),
                new Shape("the string 2", String.valueOf(counter), false),
                new Shape("null", null, false),
                new Shape("no position field", NO_FIELD, false),
                new Shape("no counter document, which reads as zero", NO_DOCUMENT, false),
                new Shape("the long 1, below the highest position", 1L, false),
                new Shape("zero", 0L, false),
                new Shape("the long -1", -1L, false)
        );
    }

    /**
     * @return counters over events that have no position, where no position is above the counter and only the
     * counter itself can be wrong
     */
    public static Stream<Shape> shapesOverUnpositionedEvents() {
        return Stream.of(
                new Shape("no counter document, which reads as zero", NO_DOCUMENT, true),
                new Shape("zero", 0L, true),
                new Shape("the int 5", 5, true),
                new Shape("the long -1", -1L, false),
                new Shape("the double 0", 0d, false),
                new Shape("the string 0", "0", false)
        );
    }

    /**
     * Give the counter document the shape, creating the document when it has none.
     *
     * @param counters   the collection holding the counter document
     * @param documentId the counter document's {@code _id}
     * @param field      the field holding the counter
     * @param shape      the shape to store
     */
    public static void give(MongoCollection<Document> counters, Object documentId, String field, Shape shape) {
        Document id = new Document("_id", documentId);
        if (shape.value() == NO_DOCUMENT) {
            counters.deleteOne(id);
        } else if (shape.value() == NO_FIELD) {
            counters.updateOne(id, new Document("$unset", new Document(field, "")));
        } else {
            counters.updateOne(id, new Document("$set", new Document(field, shape.value())), new UpdateOptions().upsert(true));
        }
    }
}
