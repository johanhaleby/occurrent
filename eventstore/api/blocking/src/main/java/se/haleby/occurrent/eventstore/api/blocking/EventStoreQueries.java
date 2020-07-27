package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Filter;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.eventstore.api.Filter.filter;

/**
 * Additional querying capabilities that may be supported by an {@link EventStore} implementation that is not typically part of a
 * "transactional" use case.
 */
public interface EventStoreQueries {

    /**
     * @return All cloud events matching the specified filters
     */
    Stream<CloudEvent> query(Filter filter, int skip, int limit);

    /**
     * @return All cloud events in insertion order
     */
    default Stream<CloudEvent> all(int skip, int limit) {
        return query(Filter.all(), skip, limit);
    }

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    default Stream<CloudEvent> all() {
        return all(0, Integer.MAX_VALUE);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default <T> Stream<CloudEvent> query(String fieldName, Condition<T> condition) {
        return query(filter(fieldName, condition));
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default Stream<CloudEvent> query(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        return query(filter, 0, Integer.MAX_VALUE);
    }
}