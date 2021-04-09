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
import org.occurrent.eventstore.api.WriteResult;

import java.util.stream.Stream;

/**
 * An interface that should be implemented by event streams that supports writing events to a stream without specifying a write condition.
 */
public interface UnconditionallyWriteToEventStream {
    /**
     * Write {@code events} to a stream
     *
     * @param streamId The stream id of the stream to write to
     * @param events   The events to write
     * @return The result of the write, includes useful metadata such as stream version.
     */
    WriteResult write(String streamId, Stream<CloudEvent> events);
}