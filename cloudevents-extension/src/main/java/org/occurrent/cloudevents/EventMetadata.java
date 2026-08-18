/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.cloudevents;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata associated with an event, such as its stream id and version and any other CloudEvent extension carried on
 * the event. Built from a {@link CloudEvent}'s extensions with {@link #from(CloudEvent)}, and shared across the
 * subscription, saga, projection, view, and DCB DSLs so they all read the same thing. Lives next to
 * {@link OccurrentCloudEventExtension}, whose keys it reads, rather than in any single DSL, since it is metadata about
 * a stored event and not specific to how the event is consumed.
 */
@NullMarked
public final class EventMetadata {

    private static final EventMetadata EMPTY = new EventMetadata(Map.of());

    private final Map<String, @Nullable Object> data;

    public EventMetadata(Map<String, @Nullable Object> data) {
        Objects.requireNonNull(data, "data cannot be null");
        // A plain HashMap copy rather than Map.copyOf, since an extension value may legitimately be null.
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    /**
     * The streamId of the event.
     * <p>
     * Note that for an event delivered through the capability-agnostic or DCB path, this is the internal generated
     * partition id (for example "dcb:partition:...") rather than a domain stream id. It is always non-null there, it is
     * just semantically an internal id in that case.
     */
    public String getStreamId() {
        return (String) Objects.requireNonNull(data.get(OccurrentCloudEventExtension.STREAM_ID), "streamId extension is absent");
    }

    /**
     * The version of the event in the stream.
     * <p>
     * Accepts a {@code Number} or a {@code String}, the same way {@link #getPosition()} does, because an event that has
     * been through a CloudEvents JSON round trip carries this as a string.
     */
    public long getStreamVersion() {
        Object value = Objects.requireNonNull(data.get(OccurrentCloudEventExtension.STREAM_VERSION), "streamVersion extension is absent");
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string) {
            return Long.parseLong(string);
        }
        throw new IllegalArgumentException("StreamVersion extension must be a Number or String");
    }

    /**
     * The global, monotonic sequence position of the event, or {@code null} when the event has no position (for example
     * a stream-written event on a store that does not write stream position). DCB-written events always have a position.
     */
    public @Nullable Long getPosition() {
        Object value = data.get(OccurrentCloudEventExtension.POSITION);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string) {
            return Long.valueOf(string);
        }
        throw new IllegalArgumentException("Position extension must be a Number or String");
    }

    /**
     * The identifier of the write or append call that persisted this event, or {@code null} when the event has none.
     * Absence has two causes. The event predates this feature, or it arrived through a push feed whose producer
     * supplied no {@value OccurrentCloudEventExtension#APPEND_ID} extension. Either way, the event was persisted,
     * just with no identifier to show for it. A caller in {@code eventstore-api-common} reads this as a typed
     * {@code AppendId} via {@code AppendId.from(EventMetadata)}.
     */
    public @Nullable String getAppendId() {
        Object value = data.get(OccurrentCloudEventExtension.APPEND_ID);
        return value == null ? null : value.toString();
    }

    /**
     * Reads an arbitrary extension {@code key} from the metadata and casts it to {@code T}. The cast is unchecked, so an
     * extension whose stored value is not a {@code T} throws a {@link ClassCastException} at the use site. Prefer the
     * typed accessors ({@link #getStreamId()}, {@link #getStreamVersion()}, {@link #getPosition()}) where they exist.
     */
    @SuppressWarnings("unchecked")
    public <T> @Nullable T get(String key) {
        return (T) data.get(key);
    }

    /**
     * The raw extension data backing this metadata (stream id and version, the DCB position and tags, and any others).
     */
    public Map<String, @Nullable Object> getData() {
        return data;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return o instanceof EventMetadata that && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return "EventMetadata{data=" + data + '}';
    }

    /**
     * Metadata with no extensions, for a fold or reaction invoked without a CloudEvent (for example on-demand replay
     * from a query, where events never carried a CloudEvent). {@link #getPosition()} is {@code null} and the stream
     * accessors throw, since there is no stream id or version to read.
     */
    public static EventMetadata empty() {
        return EMPTY;
    }

    /**
     * Build the metadata from a {@link CloudEvent}, capturing its extensions (stream id and version, the DCB position and
     * tags, and any others). Used by both the blocking and the reactive subscription DSLs so the two read the same thing.
     */
    public static EventMetadata from(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Map<String, @Nullable Object> data = new HashMap<>();
        for (String name : cloudEvent.getExtensionNames()) {
            Object value = cloudEvent.getExtension(name);
            if (value != null) {
                data.put(name, value);
            }
        }
        return new EventMetadata(data);
    }
}
