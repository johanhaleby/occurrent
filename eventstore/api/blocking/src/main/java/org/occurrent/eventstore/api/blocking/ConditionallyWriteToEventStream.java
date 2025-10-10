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
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;

import java.util.Objects;
import java.util.stream.Stream;

import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;

/**
 * Event stores that supports conditional writes to an event stream should implement this interface.
 */
@NullMarked
public interface ConditionallyWriteToEventStream {

    /**
     * A convenience function that writes a single event to an event store if the stream version is equal to {@code expectedStreamVersion}.
     *
     * @param streamId              The id of the stream
     * @param expectedStreamVersion The stream must be equal to this version in order for the events to be written
     * @param event                 The event to be appended/written to the stream
     * @return The result of the write operation, includes useful metadata such as stream version.
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     * @see #write(String, WriteCondition, CloudEvent) for more advanced write conditions
     */
    default WriteResult write(String streamId, long expectedStreamVersion, CloudEvent event) {
        Objects.requireNonNull(event, CloudEvent.class.getSimpleName() + " cannot be null");
        return write(streamId, streamVersionEq(expectedStreamVersion), Stream.of(event));
    }

    /**
     * A convenience function that writes a single event to an event store if the stream version is equal to {@code expectedStreamVersion}.
     *
     * @param streamId       The id of the stream
     * @param writeCondition The write condition that must be fulfilled for the events to be written
     * @param event          The event to be appended/written to the stream
     * @return The result of the write operation, includes useful metadata such as stream version.
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     */
    default WriteResult write(String streamId, WriteCondition writeCondition, CloudEvent event) {
        Objects.requireNonNull(event, CloudEvent.class.getSimpleName() + " cannot be null");
        return write(streamId, writeCondition, Stream.of(event));
    }

    /**
     * A convenience function that writes events to an event store if the stream version is equal to {@code expectedStreamVersion}.
     *
     * @param streamId              The id of the stream
     * @param expectedStreamVersion The stream must be equal to this version in order for the events to be written
     * @param events                The events to be appended/written to the stream
     * @return The result of the write operation, includes useful metadata such as stream version.
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     * @see #write(String, WriteCondition, Stream) for more advanced write conditions
     */
    default WriteResult write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        return write(streamId, streamVersionEq(expectedStreamVersion), events);
    }

    /**
     * Conditionally write to an event store
     *
     * @param streamId       The id of the stream
     * @param writeCondition The write condition that must be fulfilled for the events to be written
     * @param events         The events to be appended/written to the stream
     * @return The result of the write operation, includes useful metadata such as stream version.
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     */
    WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events);
}