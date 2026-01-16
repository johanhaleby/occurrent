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

package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.StreamReadFilter;

/**
 * An interface that should be implemented by event stores that supports reading an {@link EventStream} with a filter.
 */
@NullMarked
public interface ReadEventStreamWithFilter {

    /**
     * Read events from a particular event stream that matches the supplied filter.
     *
     * @param streamId The id of the stream to read.
     * @param filter   The filter to apply when reading events
     * @return An {@link EventStream} containing the events of the stream. Will return an {@link EventStream} with version {@code 0} if event stream doesn't exists.
     */
    default EventStream<CloudEvent> read(String streamId, StreamReadFilter filter) {
        return read(streamId, filter, 0, Integer.MAX_VALUE);
    }

    /**
     * Read events from a particular event stream that matches the supplied filter from a particular position.
     *
     * @param streamId The id of the stream to read.
     * @param filter   The filter to apply when reading events
     * @return An {@link EventStream} containing the events of the stream. Will return an {@link EventStream} with version {@code 0} if event stream doesn't exists.
     */
    EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit);
}
