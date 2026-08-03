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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.util.Optional;

/**
 * Reads a field out of a CloudEvent's {@code data} payload, so an in-process store can answer
 * {@link org.occurrent.filter.Filter#data(String, org.occurrent.condition.Condition)}.
 * <p>
 * This is a seam rather than a built-in because reading a payload means parsing it, and the matching module has no
 * parser and no opinion about which one you use. Occurrent ships a Jackson-backed implementation. A store handed no
 * reader keeps refusing a data filter, which is what {@link #refusing()} does.
 * <p>
 * <strong>The path is a dotted path, the same one MongoDB resolves.</strong> {@code Filter.data("person.city", ..)}
 * arrives here as {@code person.city}, without the leading {@code data.}. An implementation should return:
 * <ul>
 *     <li>the value at that path, as a plain Java value: a {@link String}, a {@link Number}, a {@link Boolean}, or a
 *     {@link java.util.List} when the field holds an array</li>
 *     <li>{@link Optional#empty()} when the path reaches nothing, including a payload that is not a JSON object, a
 *     field that is absent, and a path that continues past a value that has no fields</li>
 * </ul>
 * Returning the list rather than a match decision is deliberate: the matcher compares an array field by asking whether
 * any element satisfies the condition, the way MongoDB does, and it can only do that if it sees the elements.
 * <p>
 * A value must not be converted to text on the way out. A payload holding {@code {"amount":42}} answers with a number,
 * because {@code Filter.data("amount", eq("42"))} does not match on MongoDB and must not match here either.
 */
@NullMarked
public interface DataFieldReader {

    /**
     * The value at {@code path} inside the event's payload, or empty when the path reaches nothing.
     *
     * @param cloudEvent the event whose payload to read
     * @param path       the dotted path, without the leading {@code data.}
     */
    Optional<Object> read(CloudEvent cloudEvent, String path);

    /**
     * A reader that refuses, which is how a store behaves when it was given none. It throws rather than answering
     * empty, because answering empty would report "no event matched" for a question the store cannot answer.
     */
    static DataFieldReader refusing() {
        return (cloudEvent, path) -> {
            throw new IllegalArgumentException("This store cannot query the data field, because it was not given a "
                    + DataFieldReader.class.getSimpleName() + ". Supply one to filter on a payload field, for example "
                    + "the Jackson-backed reader in occurrent-common-inmemory-filter-matching-jackson.");
        };
    }
}
