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

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Every shape a stored {@code position} can take, meaning its BSON type and value, and whether a MongoDB event store
 * with {@code requireRepairedEvents(true)} starts over it. The store starts only when every position is a positive
 * integer no greater than its position counter, except that a non DCB event may have no position at all.
 * <p>
 * Each case starts from a collection holding two events at positions 1 and 2, with the counter at 2, and gives the
 * event at 2 its shape. A value below the counter is then refused for its own sake, so 1.5 is refused as a fraction
 * and not as a value above the counter. No shape holds 1, not even inside an array, since the {@code position} index
 * is unique and the event at 1 keeps its position.
 */
public final class StoredPositionShapes {

    /**
     * The position counter every case expects before it gives the event at {@link #SHAPED_POSITION} its shape.
     */
    public static final long COUNTER = 2;

    /**
     * The position of the event each case reshapes.
     */
    public static final long SHAPED_POSITION = 2;

    private static final Object MISSING = new Object();

    private StoredPositionShapes() {
    }

    /**
     * Whether the reshaped event is a DCB event, which has to have a position, or a plain stream event, which may
     * predate position.
     */
    public enum Kind {
        DCB("a DCB event"), PLAIN("a plain event");

        private final String description;

        Kind(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * One shape of stored position.
     *
     * @param description what the value is
     * @param position    the value to store, or the private marker for no {@code position} field at all
     * @param startsOnDcb whether the store starts when a DCB event has it
     * @param startsOnPlain whether the store starts when a plain event has it
     */
    public record Shape(String description, @Nullable Object position, boolean startsOnDcb, boolean startsOnPlain) {

        /**
         * @param kind the kind of event that has this shape
         * @return whether a store requiring repaired events starts over it
         */
        public boolean startsOn(Kind kind) {
            return kind == Kind.DCB ? startsOnDcb : startsOnPlain;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * @return every shape, each on a DCB event and on a plain event, as the arguments of a parameterized test that
     * takes a {@link Shape} and a {@link Kind}
     */
    public static Stream<Object[]> onADcbAndAPlainEvent() {
        return shapes().stream().flatMap(shape -> Stream.of(new Object[]{shape, Kind.DCB}, new Object[]{shape, Kind.PLAIN}));
    }

    /**
     * Give the event at {@link #SHAPED_POSITION} the shape, making it a DCB event first when {@code kind} says so.
     *
     * @param events    the event collection
     * @param shape     the shape to store
     * @param kind      the kind of event to make it
     * @param dcbFields the {@code dcbtags} and {@code dcbTags} fields a DCB event has
     */
    public static void give(MongoCollection<Document> events, Shape shape, Kind kind, Document dcbFields) {
        Object id = requireNonNull(events.find(new Document("position", SHAPED_POSITION)).first(), "no event at " + SHAPED_POSITION).get("_id");
        Document byId = new Document("_id", id);
        if (kind == Kind.DCB) {
            events.updateOne(byId, new Document("$set", dcbFields));
        }
        if (shape.position() == MISSING) {
            events.updateOne(byId, new Document("$unset", new Document("position", "")));
        } else {
            events.updateOne(byId, new Document("$set", new Document("position", shape.position())));
        }
    }

    private static List<Shape> shapes() {
        return List.of(
                new Shape("no position field", MISSING, false, true),
                new Shape("null", null, false, false),
                new Shape("the string \"2\"", "2", false, false),
                new Shape("a boolean", true, false, false),
                new Shape("an embedded document", new Document("value", 2L), false, false),
                new Shape("a date", new Date(2), false, false),
                new Shape("an empty array", List.of(), false, false),
                new Shape("the array [3]", List.of(3L), false, false),
                new Shape("the array [3, \"x\"]", Arrays.asList(3L, "x"), false, false),
                new Shape("the double 2.0", 2.0d, true, true),
                new Shape("the decimal 2", new Decimal128(2), true, true),
                new Shape("the int 2", 2, true, true),
                new Shape("the double 1.5", 1.5d, false, false),
                new Shape("the decimal 1.5", Decimal128.parse("1.5"), false, false),
                new Shape("the double 1e-300", 1e-300d, false, false),
                new Shape("zero", 0L, false, false),
                new Shape("a negative number", -1L, false, false),
                new Shape("the double NaN", Double.NaN, false, false),
                new Shape("the decimal NaN", Decimal128.NaN, false, false),
                new Shape("negative infinity", Double.NEGATIVE_INFINITY, false, false),
                new Shape("positive infinity", Double.POSITIVE_INFINITY, false, false),
                new Shape("the double 1e300", 1e300d, false, false),
                new Shape("the double 2^63, one above the largest long", Math.pow(2, 63), false, false),
                new Shape("negative zero", -0.0d, false, false),
                new Shape("the long 2, at the counter", SHAPED_POSITION, true, true),
                new Shape("the long 3, above the counter", COUNTER + 1, false, false)
        );
    }
}
