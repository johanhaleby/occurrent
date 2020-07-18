package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

import java.util.stream.Stream;

public interface UnconditionallyWriteToEventStream {
    void write(String streamId, Stream<CloudEvent> events);
}