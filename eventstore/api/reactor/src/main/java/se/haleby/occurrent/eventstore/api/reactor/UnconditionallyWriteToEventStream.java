package se.haleby.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Mono;

import java.util.stream.Stream;

public interface UnconditionallyWriteToEventStream {
    Mono<Void> write(String streamId, Stream<CloudEvent> events);
}