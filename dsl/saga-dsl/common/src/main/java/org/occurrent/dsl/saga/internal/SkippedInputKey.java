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

package org.occurrent.dsl.saga.internal;

import static java.util.Objects.requireNonNull;

/**
 * The key a quarantined saga instance records an input it skips under, see
 * {@link org.occurrent.dsl.saga.SkippedInput}, and the two kinds of event it names.
 * <p>
 * An event with a stream id and a stream version is keyed {@code streamId@streamVersion}, the redelivery key
 * {@link org.occurrent.dsl.saga.SagaFailure#input()} holds. Any other event is keyed by its CloudEvent id and source as
 * {@code cloudevent:<length of id>:<id>:<source>@cloudevent}. The length lets an id that contains a colon be read back,
 * and the {@code @cloudevent} suffix is not a number, so a stream key, which always ends in {@code @} and a version,
 * can never be read as a CloudEvent key or the other way round.
 */
public sealed interface SkippedInputKey {

    /** An event named by its stream id and stream version. */
    record StreamKey(String streamId, long streamVersion) implements SkippedInputKey {
        public StreamKey {
            requireNonNull(streamId, "streamId cannot be null");
        }
    }

    /** An event named by its CloudEvent id and source. */
    record CloudEventKey(String id, String source) implements SkippedInputKey {
        public CloudEventKey {
            requireNonNull(id, "id cannot be null");
            requireNonNull(source, "source cannot be null");
        }
    }

    String CLOUD_EVENT_PREFIX = "cloudevent:";
    String CLOUD_EVENT_SUFFIX = "@cloudevent";

    /** The key of an event that has no stream id and stream version. */
    static String cloudEventKey(String id, String source) {
        requireNonNull(id, "id cannot be null");
        requireNonNull(source, "source cannot be null");
        return CLOUD_EVENT_PREFIX + id.length() + ":" + id + ":" + source + CLOUD_EVENT_SUFFIX;
    }

    /**
     * Read a recorded key back.
     *
     * @throws IllegalArgumentException when {@code key} is neither kind
     */
    static SkippedInputKey parse(String key) {
        requireNonNull(key, "key cannot be null");
        if (key.startsWith(CLOUD_EVENT_PREFIX) && key.endsWith(CLOUD_EVENT_SUFFIX)) {
            String body = key.substring(CLOUD_EVENT_PREFIX.length(), key.length() - CLOUD_EVENT_SUFFIX.length());
            int colon = body.indexOf(':');
            if (colon > 0) {
                try {
                    int idLength = Integer.parseInt(body.substring(0, colon));
                    int idEnd = colon + 1 + idLength;
                    if (idLength >= 0 && idEnd < body.length() && body.charAt(idEnd) == ':') {
                        return new CloudEventKey(body.substring(colon + 1, idEnd), body.substring(idEnd + 1));
                    }
                } catch (NumberFormatException notALength) {
                    // Falls through to the stream key, which is what a stream id that starts like this is
                }
            }
        }
        int at = key.lastIndexOf('@');
        if (at >= 0) {
            try {
                return new StreamKey(key.substring(0, at), Long.parseLong(key.substring(at + 1)));
            } catch (NumberFormatException notAVersion) {
                // Reported below
            }
        }
        throw new IllegalArgumentException("'" + key + "' is neither a stream key (streamId@streamVersion) nor a CloudEvent key (cloudevent:<length of id>:<id>:<source>@cloudevent)");
    }
}
