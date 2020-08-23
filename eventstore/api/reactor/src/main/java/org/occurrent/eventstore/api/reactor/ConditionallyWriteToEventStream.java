package org.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;

import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;

/**
 * Event stores that supports conditional writes to an event stream should implement this interface.
 */
public interface ConditionallyWriteToEventStream {
    /**
     * A convenience function that writes events to an event store if the stream version is equal to {@code expectedStreamVersion}.
     * May return the following exceptions on the error track:
     *
     * <table>
     *     <tr><th>Exception</th></tr><th>Description<th></th></tr>
     *     <tr><td>{@link WriteConditionNotFulfilledException}</td></tr><td>When the <code>writeCondition</code> was not fulfilled and the events couldn't be written<td></th></tr>
     *     <tr><td>{@link DuplicateCloudEventException}</td></tr><td>If a cloud event in the supplied <code>events</code> stream already exists in the event store<td></th></tr>
     * </table>
     *
     * @param streamId              The id of the stream
     * @param expectedStreamVersion The stream must be equal to this version in order for the events to be written
     * @param events                The events to be appended/written to the stream
     * @see #write(String, WriteCondition, Flux) for more advanced write conditions
     */
    default Mono<Void> write(String streamId, long expectedStreamVersion, Flux<CloudEvent> events) {
        return write(streamId, streamVersionEq(expectedStreamVersion), events);
    }


    /**
     * A convenience function that writes events to an event store if the stream version is equal to {@code expectedStreamVersion}.
     * May return the following exceptions on the error track:
     *
     * <table>
     *     <tr><th>Exception</th></tr><th>Description<th></th></tr>
     *     <tr><td>{@link WriteConditionNotFulfilledException}</td></tr><td>When the <code>writeCondition</code> was not fulfilled and the events couldn't be written<td></th></tr>
     *     <tr><td>{@link DuplicateCloudEventException}</td></tr><td>If a cloud event in the supplied <code>events</code> stream already exists in the event store<td></th></tr>
     * </table>
     *
     * @param streamId       The id of the stream
     * @param writeCondition The write condition that must be fulfilled for the events to be written
     * @param events         The events to be appended/written to the stream
     */
    Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events);
}