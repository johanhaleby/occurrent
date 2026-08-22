/*
 * Copyright 2020 Johan Haleby
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
import io.cloudevents.CloudEventExtension;
import io.cloudevents.CloudEventExtensions;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * A {@link CloudEvent} {@link CloudEventExtension} that adds required extensions for Occurrent. These are:<br><br>
 *
 * <table>
 *     <tr><th>Key</th><th>Description</th></tr>
 *     <tr><td>{@value #STREAM_ID}</td><td>The id of a particular event stream</td></tr>
 *     <tr><td>{@value #STREAM_VERSION}</td><td>The version of an event in a particular event stream</td></tr>
 * </table>
 */
public class OccurrentCloudEventExtension implements CloudEventExtension {
    public static final String STREAM_ID = "streamid";
    public static final String STREAM_VERSION = "streamversion";
    /**
     * CloudEvent extension name that contains an event's global, monotonic, comparable sequence position.
     */
    public static final String POSITION = "position";
    /**
     * CloudEvent extension name that contains the identifier of the write or append call that persisted this event.
     * Every event persisted by the same call has the same value. See ADR 132.
     */
    public static final String APPEND_ID = "appendid";

    static final Set<String> KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(STREAM_ID, STREAM_VERSION)));
    private String streamId;
    private long streamVersion;

    public OccurrentCloudEventExtension(String streamId, long streamVersion) {
        Objects.requireNonNull(streamId, "StreamId cannot be null");
        if (streamVersion < 1) {
            throw new IllegalArgumentException("Stream version cannot be less than 1");
        }
        this.streamId = streamId;
        this.streamVersion = streamVersion;
    }

    public static OccurrentCloudEventExtension occurrent(String streamId, long streamVersion) {
        return new OccurrentCloudEventExtension(streamId, streamVersion);
    }

    @Override
    public void readFrom(CloudEventExtensions extensions) {
        Object streamId = extensions.getExtension(STREAM_ID);
        if (streamId != null) {
            this.streamId = streamId.toString();
        }

        Object streamVersion = extensions.getExtension(STREAM_VERSION);
        if (streamVersion != null) {
            this.streamVersion = (long) streamVersion;
        }
    }

    @Override
    public Object getValue(String key) throws IllegalArgumentException {
        if (STREAM_ID.equals(key)) {
            return this.streamId;
        } else if (STREAM_VERSION.equals(key)) {
            return this.streamVersion;
        }
        throw new IllegalArgumentException(this.getClass().getSimpleName() + " doesn't expect the attribute key \"" + key + "\"");
    }

    @Override
    public Set<String> getKeys() {
        return KEYS;
    }

    /**
     * Returns a copy of {@code cloudEvent} with the global sequence position in the {@value #POSITION} extension.
     */
    public static CloudEvent withPosition(CloudEvent cloudEvent, long position) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        return CloudEventBuilder.v1(cloudEvent).withExtension(POSITION, position).build();
    }

    /**
     * Reads the global sequence position from a CloudEvent, or {@code 0} when it has no position.
     */
    public static long getPosition(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        Object position = cloudEvent.getExtension(POSITION);
        if (position == null) {
            return 0;
        }
        if (position instanceof Number number) {
            return number.longValue();
        }
        if (position instanceof String string) {
            return Long.parseLong(string);
        }
        throw new IllegalArgumentException("Position extension must be a Number or String");
    }

    /**
     * Returns a copy of {@code cloudEvent} with {@code appendId} in the {@value #APPEND_ID} extension.
     */
    public static CloudEvent withAppendId(CloudEvent cloudEvent, String appendId) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        requireNonNull(appendId, "Append id cannot be null");
        return CloudEventBuilder.v1(cloudEvent).withExtension(APPEND_ID, appendId).build();
    }

    /**
     * Reads the append id from a CloudEvent, or {@code null} when the event has none.
     */
    public static @Nullable String getAppendId(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        Object appendId = cloudEvent.getExtension(APPEND_ID);
        return appendId == null ? null : appendId.toString();
    }

    /**
     * Returns a copy of {@code updated} with {@code original}'s {@value #STREAM_ID} and {@value #STREAM_VERSION}. A
     * store's {@code updateEvent} calls this so a replacement event an updater builds from scratch cannot silently
     * lose which stream and version it belongs to, or move it to a different one. Every stored event belongs to
     * exactly one stream at exactly one version, so unlike the append id or the position, there is no absent case
     * to preserve here.
     */
    public static CloudEvent preserveStreamIdentity(CloudEvent original, CloudEvent updated) {
        requireNonNull(original, "Original CloudEvent cannot be null");
        requireNonNull(updated, "Updated CloudEvent cannot be null");
        String streamId = OccurrentExtensionGetter.getStreamId(original);
        long streamVersion = OccurrentExtensionGetter.getStreamVersion(original);
        return CloudEventBuilder.v1(updated).withExtension(STREAM_ID, streamId).withExtension(STREAM_VERSION, streamVersion).build();
    }

    /**
     * Returns a copy of {@code updated} with {@code original}'s exact append id state, present or absent. A
     * store's {@code updateEvent} calls this so a replacement event an updater builds from scratch cannot silently
     * drop the append id an earlier write stamped, and cannot pick one up that it never had. Either mistake would
     * move the event into an append it does not belong to. The store owns this value the same way it owns
     * {@value #STREAM_ID}, {@value #STREAM_VERSION} and {@value #POSITION}, so it is reapplied rather than left to
     * the updater.
     */
    public static CloudEvent preserveAppendId(CloudEvent original, CloudEvent updated) {
        requireNonNull(original, "Original CloudEvent cannot be null");
        requireNonNull(updated, "Updated CloudEvent cannot be null");
        String appendId = getAppendId(original);
        return appendId == null ? CloudEventBuilder.v1(updated).withoutExtension(APPEND_ID).build() : withAppendId(updated, appendId);
    }

    /**
     * Returns a copy of {@code updated} with {@code original}'s exact position, present or absent, the same
     * present-or-absent treatment {@link #preserveAppendId} gives the append id. A store's {@code updateEvent}
     * calls this so a replacement event an updater builds from scratch cannot silently drop the position an
     * earlier write stamped, and cannot forge one it never had. Either mistake would move the event to a
     * different point in, or out of, the store's global sequence. This only reapplies the extension on the
     * returned {@link CloudEvent}, not the stored document's BSON type, which a store fixes separately.
     */
    public static CloudEvent preservePosition(CloudEvent original, CloudEvent updated) {
        requireNonNull(original, "Original CloudEvent cannot be null");
        requireNonNull(updated, "Updated CloudEvent cannot be null");
        long position = getPosition(original);
        return position > 0 ? withPosition(updated, position) : CloudEventBuilder.v1(updated).withoutExtension(POSITION).build();
    }
}