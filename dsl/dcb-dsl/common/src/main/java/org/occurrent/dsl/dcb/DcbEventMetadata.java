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

package org.occurrent.dsl.dcb;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.OptionalLong;
import java.util.Set;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import static java.util.Objects.requireNonNull;

/**
 * Java-friendly view over an {@link EventMetadata} that exposes the event's global position and its DCB tags as
 * an {@link OptionalLong} and a {@link Set}.
 * <p>
 * The generic {@link EventMetadata} lives in the subscription DSL and does not depend on the DCB API, so the tag
 * accessor lives here. Both the blocking and the reactive DCB DSLs build an {@link EventMetadata} from the delivered
 * CloudEvent and wrap it with this. Kotlin callers can read {@code EventMetadata.position} directly and use the
 * {@code EventMetadata.dcbTags} extension property for tags.
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
     * The global position of the event, or empty when the event has no position (for example a stream-written event
     * on a store that does not write stream position). A Java-friendly view of {@code EventMetadata.position}.
     */
    public OptionalLong position() {
        return decodePosition(metadata.getData().get(OccurrentCloudEventExtension.POSITION));
    }

    /**
     * The canonical DCB tags of the event, or an empty set when the event has no DCB tags.
     */
    public Set<Tag> dcbTags() {
        return decodeTags(metadata.getData().get(DcbCloudEvents.TAGS));
    }

    /**
     * The wrapped {@link EventMetadata}, for the generic subscription metadata (such as the storage stream id and
     * version) that is not specific to DCB.
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
        throw new IllegalArgumentException("Position extension must be a Number or String");
    }

    static Set<Tag> decodeTags(@Nullable Object value) {
        if (value == null) {
            return Set.of();
        }
        if (value instanceof String string) {
            return DcbCloudEvents.decodeTags(string);
        }
        throw new IllegalArgumentException("DCB tags extension must be a String");
    }
}
