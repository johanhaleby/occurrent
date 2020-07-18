package se.haleby.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Mono;

public interface ReadEventStream {
    default Mono<EventStream<CloudEvent>> read(String streamId) {
        return read(streamId, 0, Integer.MAX_VALUE);
    }

    Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit);
}
