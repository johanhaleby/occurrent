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
 * A DCB tag, a {@code key:value} pair that scopes events to a consistency boundary.
 * <p>
 * Keys and values are stripped of surrounding whitespace. A key may not be blank, contain a
 * {@code ':'} (the key/value separator), or contain a newline. A value may not be blank or contain
 * a newline, but it may contain {@code ':'}. Tags order by their {@link #canonical()} form so that a
 * tag set has a stable canonical encoding for partition placement.
 */
@NullMarked
public record Tag(String key, String value) implements Comparable<Tag> {

    static final char SEPARATOR = ':';
    private static final char NEWLINE = '\n';

    public Tag {
        requireNonNull(key, "Tag key cannot be null");
        requireNonNull(value, "Tag value cannot be null");
        key = key.strip();
        value = value.strip();
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Tag key cannot be blank");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Tag value cannot be blank");
        }
        if (key.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException("Tag key cannot contain '" + SEPARATOR + "'");
        }
        if (key.indexOf(NEWLINE) >= 0 || value.indexOf(NEWLINE) >= 0) {
            throw new IllegalArgumentException("Tag key/value cannot contain a newline");
        }
    }

    /**
     * Creates a tag from a key and value.
     */
    public static Tag of(String key, String value) {
        return new Tag(key, value);
    }

    /**
     * Parses a tag from its {@code key:value} canonical form, splitting on the first {@code ':'}.
     */
    public static Tag parse(String s) {
        requireNonNull(s, "Tag cannot be null");
        int idx = s.indexOf(SEPARATOR);
        if (idx < 0) {
            throw new IllegalArgumentException("Tag must be in 'key:value' form: " + s);
        }
        return new Tag(s.substring(0, idx), s.substring(idx + 1));
    }

    /**
     * Returns the canonical {@code key:value} string form of this tag.
     */
    public String canonical() {
        return key + SEPARATOR + value;
    }

    @Override
    public int compareTo(Tag other) {
        return canonical().compareTo(other.canonical());
    }

    @Override
    public String toString() {
        return canonical();
    }
}
