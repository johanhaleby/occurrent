package se.haleby.occurrent;

import io.cloudevents.v1.CloudEventImpl;

import java.util.stream.Stream;

public interface WriteEventStream {
    <T> void write(String streamId, long expectedStreamVersion, Stream<CloudEventImpl<T>> events);
}
