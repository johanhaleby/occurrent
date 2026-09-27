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
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Every pair of {@code dcbtags} string and {@code dcbTags} tag index a stored event can hold, and whether a MongoDB
 * event store with {@code requireRepairedEvents(true)} starts over it, before and after the update-event repair has
 * run. The store starts only over a plain event with neither field, or over a {@code dcbtags} string whose every line
 * is non-empty with nothing {@code String.strip} removes, next to a tag index holding the same set of tags as those
 * lines, none when {@code dcbtags} is empty. The repair rebuilds both fields from {@code dcbtags} whenever that string
 * decodes to a tag set, and reports the rest.
 * <p>
 * Each case gives the one stored event its pair, so its position stays whatever the store assigned.
 */
public final class StoredTagShapes {

    private static final Object MISSING = new Object();

    private StoredTagShapes() {
    }

    /**
     * One pair of tag fields.
     *
     * @param description       what the pair is
     * @param dcbtags           the {@code dcbtags} value to store, or the private marker for no such field
     * @param dcbTags           the {@code dcbTags} value to store, or the private marker for no such field
     * @param starts            whether the store starts over it as found
     * @param startsAfterRepair whether the store starts over it once the repair has run
     */
    public record Shape(String description, @Nullable Object dcbtags, @Nullable Object dcbTags, boolean starts, boolean startsAfterRepair) {

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * @return every pair
     */
    public static Stream<Shape> shapes() {
        return Stream.of(
                new Shape("an index holding the tags", "name:1", List.of("name:1"), true, true),
                new Shape("no tags and an empty index", "", List.of(), true, true),
                new Shape("an index holding the tags in another order", "name:1\nother:2", List.of("other:2", "name:1"), true, true),
                new Shape("an index holding a tag twice", "name:1", List.of("name:1", "name:1"), true, true),
                new Shape("a plain event", MISSING, MISSING, true, true),
                new Shape("a tag ending in a no-break space, which strip keeps", "name:1\u00A0", List.of("name:1\u00A0"), true, true),
                new Shape("no index", "name:1", MISSING, false, true),
                new Shape("a null index", "name:1", null, false, true),
                new Shape("an index that is a document", "name:1", new Document("x", 1), false, true),
                new Shape("an index that is a string", "name:1", "name:1", false, true),
                new Shape("an empty index for a tagged event", "name:1", List.of(), false, true),
                new Shape("an index holding another tag", "name:1", List.of("other:2"), false, true),
                new Shape("an index missing one of two tags", "name:1\nother:2", List.of("name:1"), false, true),
                new Shape("an index holding one tag too many", "name:1", List.of("name:1", "other:2"), false, true),
                new Shape("a tag in the index of an untagged event", "", List.of("name:1"), false, true),
                new Shape("an empty tag in the index of an untagged event", "", List.of(""), false, true),
                new Shape("dcbtags with whitespace around a tag", " name:1", List.of("name:1"), false, true),
                new Shape("dcbtags and index with whitespace around a tag", " name:1", List.of(" name:1"), false, true),
                new Shape("a tag ending in a unit separator, which strip removes", "name:1\u001F", List.of("name:1\u001F"), false, true),
                new Shape("a null dcbtags", null, List.of("name:1"), false, false),
                new Shape("a dcbtags that is a number", 1, List.of("name:1"), false, false),
                new Shape("a dcbtags that is an array", List.of("name:1"), List.of("name:1"), false, false),
                new Shape("a dcbtags with an empty line", "name:1\n", List.of("name:1"), false, false),
                new Shape("an empty last line in both fields", "name:1\n", List.of("name:1", ""), false, false),
                new Shape("an empty line between two tags in both fields", "name:1\n\nother:2", List.of("name:1", "", "other:2"), false, false),
                new Shape("a plain event with a stray index", MISSING, List.of("name:1"), false, false),
                new Shape("a plain event with a null index", MISSING, null, false, false)
        );
    }

    /**
     * Give the only event in {@code events} the pair.
     *
     * @param events the event collection
     * @param shape  the pair to store
     */
    public static void give(MongoCollection<Document> events, Shape shape) {
        Object id = requireNonNull(events.find().first(), "no event").get("_id");
        Document set = new Document();
        Document unset = new Document();
        put(set, unset, "dcbtags", shape.dcbtags());
        put(set, unset, "dcbTags", shape.dcbTags());
        Document update = new Document();
        if (!set.isEmpty()) {
            update.append("$set", set);
        }
        if (!unset.isEmpty()) {
            update.append("$unset", unset);
        }
        events.updateOne(new Document("_id", id), update);
    }

    private static void put(Document set, Document unset, String field, @Nullable Object value) {
        if (value == MISSING) {
            unset.append(field, "");
        } else {
            set.append(field, value);
        }
    }
}
