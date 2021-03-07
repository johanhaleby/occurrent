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

import java.util.*;

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
}