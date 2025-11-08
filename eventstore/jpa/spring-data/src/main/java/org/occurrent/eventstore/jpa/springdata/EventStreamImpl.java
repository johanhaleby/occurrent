package org.occurrent.eventstore.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStream;

import java.util.List;
import java.util.stream.Stream;

public record EventStreamImpl(
        String streamId,
        long streamVersion,
        List<CloudEvent> _events
) implements EventStream<CloudEvent> {
    @Override
    public String id() {
        return streamId;
    }

    @Override
    public long version() {
        return streamVersion;
    }

    @Override
    public Stream<CloudEvent> events() {
        return _events.stream();
    }
}
