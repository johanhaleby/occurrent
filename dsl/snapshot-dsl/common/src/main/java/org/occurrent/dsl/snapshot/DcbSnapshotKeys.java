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

package org.occurrent.dsl.snapshot;

import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.Tag;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Derives a stable snapshot key from a {@link DcbCriteria}, the default keying for the DCB snapshot executors.
 * <p>
 * {@link DcbCriteria#toString()} is not usable as a key because its types, tags, and alternatives live in unordered
 * sets, so the same logical boundary can render in different orders and miss its snapshot. This canonicalizes by sorting
 * every part, so a boundary always yields the same key regardless of the order tags or types were supplied in.
 * <p>
 * The sorted, delimited representation is then SHA-256 hashed: types, tags, and excluded types are joined with plain
 * {@code ,}/{@code [}/{@code ]} characters that are not escaped, so two structurally distinct criteria could otherwise
 * render to the identical string (e.g. a single type named {@code "A,B"} versus the two types {@code "A"} and
 * {@code "B"} both render as {@code types[A,B]}). Hashing the fully-qualified, length-prefixed representation instead
 * of using the delimited string directly turns that silent snapshot-corrupting collision into a (practically
 * impossible) hash collision.
 */
public final class DcbSnapshotKeys {

    private DcbSnapshotKeys() {
    }

    /**
     * A deterministic, order-insensitive key for {@code criteria}.
     */
    public static String canonicalKey(DcbCriteria criteria) {
        requireNonNull(criteria, "criteria cannot be null");
        String canonical = switch (criteria) {
            case DcbCriteria.MatchAll ignored -> "all";
            case DcbCriterion criterion -> criterion(criterion);
            case DcbCriteria.Items items -> items.items().stream().map(DcbSnapshotKeys::criterion).sorted().collect(joining(",", "anyOf[", "]"));
        };
        return sha256Hex(canonical);
    }

    private static String criterion(DcbCriterion criterion) {
        String types = lengthPrefixedJoin(criterion.types().stream());
        String tags = lengthPrefixedJoin(criterion.tags().stream().map(Tag::value));
        String excludedTypes = lengthPrefixedJoin(criterion.excludedTypes().stream());
        return "types[" + types + "]tags[" + tags + "]excludingTypes[" + excludedTypes + "]";
    }

    /**
     * Joins {@code values}, sorted, with each element prefixed by its length so that a delimiter occurring inside an
     * element cannot be mistaken for the boundary between two elements (the collision {@link DcbSnapshotKeys} guards
     * against).
     */
    private static String lengthPrefixedJoin(Stream<String> values) {
        return values.sorted().map(value -> value.length() + ":" + value).collect(joining(","));
    }

    private static String sha256Hex(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available on every conforming JVM (java.security.MessageDigest javadoc).
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
