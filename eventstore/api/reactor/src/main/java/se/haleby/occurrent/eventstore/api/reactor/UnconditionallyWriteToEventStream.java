package se.haleby.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UnconditionallyWriteToEventStream {
    Mono<Void> write(String streamId, Flux<CloudEvent> events);
}