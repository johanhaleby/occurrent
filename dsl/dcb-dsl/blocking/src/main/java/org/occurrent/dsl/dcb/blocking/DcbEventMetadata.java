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

package org.occurrent.dsl.dcb.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;

import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Java-friendly view over an {@link EventMetadata} that exposes DCB-specific metadata, namely the DCB sequence
 * position and the DCB tags.
 * <p>
 * The generic {@link EventMetadata} lives in the subscription DSL and intentionally does not depend on the DCB API,
 * so these accessors live here in the DCB DSL module. Kotlin callers can use the {@code EventMetadata.dcbPosition}
 * and {@code EventMetadata.dcbTags} extension properties instead.
 */
@NullMarked
public final class DcbEventMetadata {

    private final EventMetadata metadata;

    public DcbEventMetadata(EventMetadata metadata) {
        this.metadata = requireNonNull(metadata, EventMetadata.class.getSimpleName() + " cannot be null");
    }

    /**
     * Wraps an {@link EventMetadata} so its DCB metadata can be read.
     */
    public static DcbEventMetadata from(EventMetadata metadata) {
        return new DcbEventMetadata(metadata);
    }

    /**
     * The DCB sequence position of the event, or empty when the event has no DCB position (for example a
     * regular stream-written event).
     */
    public OptionalLong dcbPosition() {
        return decodePosition(metadata.getData().get(DcbCloudEvents.POSITION));
    }

    /**
     * The canonical DCB tags of the event, or an empty set when the event has no DCB tags.
     */
    public Set<String> dcbTags() {
        return decodeTags(metadata.getData().get(DcbCloudEvents.TAGS));
    }

    /**
     * The id of the Occurrent storage stream the event was written to.
     */
    public String streamId() {
        return metadata.getStreamId();
    }

    /**
     * The version of the event within its Occurrent storage stream.
     */
    public long streamVersion() {
        return metadata.getStreamVersion();
    }

    /**
     * The wrapped {@link EventMetadata}.
     */
    public EventMetadata eventMetadata() {
        return metadata;
    }

    static OptionalLong decodePosition(@Nullable Object value) {
        if (value == null) {
            return OptionalLong.empty();
        }
        if (value instanceof Number number) {
            return OptionalLong.of(number.longValue());
        }
        if (value instanceof String string) {
            return OptionalLong.of(Long.parseLong(string));
        }
        throw new IllegalArgumentException("DCB position extension must be a Number or String");
    }

    static Set<String> decodeTags(@Nullable Object value) {
        if (value == null) {
            return Set.of();
        }
        if (value instanceof String string) {
            return DcbCloudEvents.decodeTags(string);
        }
        throw new IllegalArgumentException("DCB tags extension must be a String");
    }
}
