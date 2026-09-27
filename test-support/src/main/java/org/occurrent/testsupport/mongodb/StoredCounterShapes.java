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
import org.bson.Document;
import org.bson.types.Decimal128;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.stream.Stream;

/**
 * Every shape a stored position counter can take, and whether a MongoDB event store with
 * {@code requireRepairedEvents(true)} starts over it. Every store casts the counter to a {@link Number}, reads it with
 * {@link Number#longValue()} and reads a missing counter document as zero. So the store starts only when the counter is
 * a whole number that call reads exactly and at least the highest position, with a missing document counting as zero.
 * <p>
 * Each case starts from a collection holding two events at positions 1 and 2, with the counter at
 * {@link StoredPositionShapes#COUNTER}, and gives the counter document its shape.
 */
public final class StoredCounterShapes {

    private static final Object NO_DOCUMENT = new Object();
    private static final Object NO_FIELD = new Object();

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
                new Shape("the long 2, as written", counter, true),
                new Shape("the int 2", (int) counter, true),
                new Shape("the double 2.0", (double) counter, true),
                new Shape("the decimal 2", new Decimal128(counter), true),
                new Shape("the long 99, above the highest position", 99L, true),
                new Shape("the decimal 2^53 + 1, which longValue reads through a double", new Decimal128(new BigDecimal("9007199254740993")), false),
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
     * Give the counter document the shape.
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
            counters.updateOne(id, new Document("$set", new Document(field, shape.value())));
        }
    }
}
