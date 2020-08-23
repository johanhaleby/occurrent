package org.occurrent.eventstore.api.reactor;

import reactor.core.publisher.Mono;

/**
 * Interface the is implemented by event stores that supports checking whether or not an event stream exists.
 */
public interface EventStreamExists {
    /**
     * Check whether or not an event stream exists
     *
     * @param streamId The stream id to check
     * @return {@code true} if the stream exists, {@code false} otherwise.
     */
    Mono<Boolean> exists(String streamId);
}
