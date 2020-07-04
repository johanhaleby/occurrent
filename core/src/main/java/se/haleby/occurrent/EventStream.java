package se.haleby.occurrent;

import io.cloudevents.v1.CloudEventImpl;

import java.util.stream.Stream;

public interface EventStream<T> {

    String id();

    long version();

    Stream<CloudEventImpl<T>> events();
}