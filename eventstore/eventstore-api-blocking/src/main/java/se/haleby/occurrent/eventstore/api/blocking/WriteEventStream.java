package se.haleby.occurrent.eventstore.api.blocking;


import io.cloudevents.CloudEvent;

import java.util.stream.Stream;

public interface WriteEventStream {
    void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events);
}
