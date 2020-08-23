package se.haleby.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An interface that should be implemented by event streams that supports writing events to a stream without specifying a write condition.
 */
public interface UnconditionallyWriteToEventStream {
    /**
     * Write {@code events} to a stream
     *
     * @param streamId The stream id of the stream to write to
     * @param events   The events to write
     */
    Mono<Void> write(String streamId, Flux<CloudEvent> events);
}