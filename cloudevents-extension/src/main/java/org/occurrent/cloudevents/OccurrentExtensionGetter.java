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

import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;

/**
 * Utility class that helps get occurrent extension values, and converts them to the correct type, from a {@link CloudEvent}.
 */
public class OccurrentExtensionGetter {

    /**
     * Get the stream version from a {@link CloudEvent} that has {@link OccurrentCloudEventExtension} applied.
     *
     * @param cloudEvent The cloud event
     * @return the stream version
     */
    public static long getStreamVersion(CloudEvent cloudEvent) {
        if (!cloudEvent.getExtensionNames().contains(STREAM_VERSION)) {
            throw new IllegalArgumentException(CloudEvent.class.getSimpleName() + " does not contain the " + STREAM_VERSION + " key");
        }

        Object streamVersion = cloudEvent.getExtension(STREAM_VERSION);
        if (!(streamVersion instanceof Long)) {
            throw new IllegalArgumentException(CloudEvent.class.getSimpleName() + " does not contain a " + STREAM_VERSION + " value that is an instance of " + long.class.getSimpleName());
        }
        return (long) streamVersion;
    }

    /**
     * Get the stream id from a {@link CloudEvent} that has {@link OccurrentCloudEventExtension} applied.
     *
     * @param cloudEvent The cloud event
     * @return the stream id
     */
    public static String getStreamId(CloudEvent cloudEvent) {
        if (!cloudEvent.getExtensionNames().contains(STREAM_ID)) {
            throw new IllegalArgumentException(CloudEvent.class.getSimpleName() + " does not contain the " + STREAM_ID + " key");
        }

        Object streamId = cloudEvent.getExtension(STREAM_ID);
        if (!(streamId instanceof String)) {
            throw new IllegalArgumentException(CloudEvent.class.getSimpleName() + " does not contain a " + STREAM_ID + " value that is an instance of " + String.class.getSimpleName());
        }
        return (String) streamId;
    }
}