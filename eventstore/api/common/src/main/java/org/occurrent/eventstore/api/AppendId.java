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

package org.occurrent.eventstore.api;

import org.jspecify.annotations.NullMarked;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Identifies a single write or append call that persisted at least one event. Every event a call persists has the
 * same {@code AppendId}, stamped as the {@value OccurrentCloudEventExtension#APPEND_ID} CloudEvent extension, so a
 * projection can record which appends it has applied and a caller can later ask whether that particular append has
 * been applied (read-your-writes as a membership question rather than a position). See ADR 132.
 *
 * @param value the wrapped, randomly generated identifier
 */
@NullMarked
public record AppendId(UUID value) {

    public AppendId {
        Objects.requireNonNull(value, "Append id value cannot be null");
    }

    /**
     * Mints a fresh {@code AppendId} for a write or append call that is about to persist at least one event.
     */
    public static AppendId mint() {
        return new AppendId(UUID.randomUUID());
    }

    /**
     * Wraps an existing, already-minted identifier value, for example one read back from storage.
     */
    public static AppendId of(UUID value) {
        return new AppendId(value);
    }

    /**
     * Reads the append id {@code metadata} has, or returns {@link Optional#empty()} when its event has none,
     * either because it predates this feature or because the store or producer that supplied it did not stamp
     * one.
     */
    public static Optional<AppendId> from(EventMetadata metadata) {
        Objects.requireNonNull(metadata, "EventMetadata cannot be null");
        String raw = metadata.getAppendId();
        return raw == null ? Optional.empty() : Optional.of(new AppendId(UUID.fromString(raw)));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
