package se.haleby.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.eventstore.api.WriteCondition;

import static se.haleby.occurrent.eventstore.api.WriteCondition.streamVersionEq;

public interface ConditionallyWriteToEventStream {
    default Mono<Void> write(String streamId, long expectedStreamVersion, Flux<CloudEvent> events) {
        return write(streamId, streamVersionEq(expectedStreamVersion), events);
    }


    Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events);
}