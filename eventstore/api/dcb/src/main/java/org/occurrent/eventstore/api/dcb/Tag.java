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

package org.occurrent.eventstore.api.dcb;

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * A DCB tag, an opaque string marker that scopes events to a consistency boundary.
 * <p>
 * The DCB specification treats a tag as a string that <em>may</em>, but need not, represent a
 * key/value pair such as {@code product:123}. Use {@link #of(String, String)} to build the key/value
 * form and {@link #of(String)} (or {@link #parse(String)}) for a tag from its string form, including a
 * value-less marker such as {@code premium}. A tag is stripped of surrounding whitespace and may not be
 * blank or contain a newline. Tags order by their string value so that a tag set has a stable canonical
 * encoding for partition placement.
 */
@NullMarked
public record Tag(String value) implements Comparable<Tag> {

    static final char SEPARATOR = ':';
    private static final char NEWLINE = '\n';

    public Tag {
        requireNonNull(value, "Tag cannot be null");
        value = value.strip();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Tag cannot be blank");
        }
        if (value.indexOf(NEWLINE) >= 0) {
            throw new IllegalArgumentException("Tag cannot contain a newline");
        }
    }

    /**
     * Creates a tag from its string form, for example {@code premium} or {@code course:c1}.
     */
    public static Tag of(String tag) {
        return new Tag(tag);
    }

    /**
     * Creates a {@code key:value} tag. The key may not be blank or contain a {@code ':'}, and the value
     * may not be blank. The value may itself contain {@code ':'}.
     */
    public static Tag of(String key, String value) {
        requireNonNull(key, "Tag key cannot be null");
        requireNonNull(value, "Tag value cannot be null");
        String strippedKey = key.strip();
        String strippedValue = value.strip();
        if (strippedKey.isEmpty()) {
            throw new IllegalArgumentException("Tag key cannot be blank");
        }
        if (strippedValue.isEmpty()) {
            throw new IllegalArgumentException("Tag value cannot be blank");
        }
        if (strippedKey.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException("Tag key cannot contain '" + SEPARATOR + "'");
        }
        return new Tag(strippedKey + SEPARATOR + strippedValue);
    }

    /**
     * Reconstructs a tag from its {@link #canonical()} string form. Equivalent to {@link #of(String)},
     * and accepts both the {@code key:value} form and a value-less marker.
     */
    public static Tag parse(String s) {
        return new Tag(s);
    }

    /**
     * Returns the canonical string form of this tag, identical to {@link #value()}.
     */
    public String canonical() {
        return value;
    }

    @Override
    public int compareTo(Tag other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
