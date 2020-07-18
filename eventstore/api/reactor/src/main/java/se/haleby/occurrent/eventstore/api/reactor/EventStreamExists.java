package se.haleby.occurrent.eventstore.api.reactor;

import reactor.core.publisher.Mono;

public interface EventStreamExists {
    Mono<Boolean> exists(String streamId);
}
