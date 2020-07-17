package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

import java.util.stream.Stream;

import static se.haleby.occurrent.eventstore.api.blocking.WriteCondition.streamVersionEq;

public interface ConditionallyWriteToEventStream {
    default void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        write(streamId, streamVersionEq(expectedStreamVersion), events);
    }


    void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events);
}