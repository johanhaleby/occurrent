package se.haleby.occurrent.inmemory;

import io.cloudevents.v1.CloudEventImpl;
import se.haleby.occurrent.EventStore;
import se.haleby.occurrent.EventStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryEventStore implements EventStore {

    private final ConcurrentMap<String, VersionAndEvents> state = new ConcurrentHashMap<>();

    @Override
    public <T> EventStream<T> read(String streamId, int skip, int limit, Class<T> t) {
        VersionAndEvents versionAndEvents = state.get(streamId);
        if (versionAndEvents == null) {
            return null;
        } else if (skip == 0 && limit == Integer.MAX_VALUE) {
            return new EventStreamImpl<>(streamId, versionAndEvents);
        }
        return new EventStreamImpl<>(streamId, versionAndEvents.flatMap(v -> new VersionAndEvents(v.version, v.events.subList(skip, limit))));
    }

    @Override
    public <T> void write(String streamId, long expectedStreamVersion, Stream<CloudEventImpl<T>> events) {
        state.compute(streamId, (__, currentVersionAndEvents) -> {
            if (currentVersionAndEvents == null) {
                return new VersionAndEvents(0, events.collect(Collectors.toList()));
            } else if (currentVersionAndEvents.version == expectedStreamVersion) {
                List<CloudEventImpl<?>> newEvents = new ArrayList<>(currentVersionAndEvents.events);
                newEvents.addAll(events.collect(Collectors.toList()));
                return new VersionAndEvents(expectedStreamVersion + 1, newEvents);
            } else {
                throw new IllegalStateException(String.format("Optimistic locking exception! Expected version %s but was %s.", expectedStreamVersion, currentVersionAndEvents.version));
            }
        });
    }

    private static class VersionAndEvents {
        long version;
        List<CloudEventImpl<?>> events;


        VersionAndEvents(long version, List<CloudEventImpl<?>> events) {
            this.version = version;
            this.events = events;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VersionAndEvents)) return false;
            VersionAndEvents that = (VersionAndEvents) o;
            return version == that.version &&
                    Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, events);
        }

        VersionAndEvents flatMap(Function<VersionAndEvents, VersionAndEvents> fn) {
            return fn.apply(this);
        }
    }

    private static class EventStreamImpl<T> implements EventStream<T> {
        private final String streamId;
        private final VersionAndEvents versionAndEvents;

        public EventStreamImpl(String streamId, VersionAndEvents versionAndEvents) {
            this.streamId = streamId;
            this.versionAndEvents = versionAndEvents;
        }

        @Override
        public String id() {
            return streamId;
        }

        @Override
        public long version() {
            return versionAndEvents.version;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Stream<CloudEventImpl<T>> events() {
            return versionAndEvents.events.stream().map(e -> (CloudEventImpl<T>) e);
        }
    }
}
