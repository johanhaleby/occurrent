package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Filter;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.eventstore.api.Filter.filter;

/**
 * Additional querying capabilities that may be supported by an {@link EventStore} implementation that is not typically part of a
 * "transactional" use case.
 */
public interface EventStoreQueries {

    /**
     * @return All cloud events in insertion order
     */
    Stream<CloudEvent> all(int skip, int limit);

    /**
     * @return All cloud events matching the specified filters
     */
    Stream<CloudEvent> query(List<Filter> filters, int skip, int limit);

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    default Stream<CloudEvent> all() {
        return all(0, Integer.MAX_VALUE);
    }

    /**
     * @return All cloud events matching the specified filters
     */
    default Stream<CloudEvent> query(List<Filter> filters) {
        return query(filters, 0, Integer.MAX_VALUE);
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
        return query(Collections.singletonList(filter));
    }
}