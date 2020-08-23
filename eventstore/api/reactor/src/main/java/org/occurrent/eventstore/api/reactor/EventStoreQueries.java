package org.occurrent.eventstore.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import org.occurrent.filter.Filter;

import static java.util.Objects.requireNonNull;

/**
 * Additional querying capabilities that may be supported by an {@link EventStore} implementation that is not typically part of a
 * "transactional" use case.
 */
public interface EventStoreQueries {

    /**
     * Note that it's recommended to create an index on the "time" field in the event store in order to make
     * {@link SortBy#TIME_ASC} and {@link SortBy#TIME_DESC} efficient.
     *
     * @return All cloud events matching the specified filter, skip, limit and sort by <code>sortBy</code>.
     */
    Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy);

    /**
     * @return All cloud events matching the specified filter sorted by <code>sortBy</code>.
     */
    default Flux<CloudEvent> query(Filter filter, SortBy sortBy) {
        return query(filter, 0, Integer.MAX_VALUE, sortBy);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default Flux<CloudEvent> query(Filter filter, int skip, int limit) {
        return query(filter, skip, limit, SortBy.NATURAL_ASC);
    }

    /**
     * @return All cloud events in insertion order
     */
    default Flux<CloudEvent> all(int skip, int limit, SortBy sortBy) {
        return query(Filter.all(), skip, limit, sortBy);
    }


    /**
     * @return All cloud events sorted by <code>sortBy</code>
     */
    default Flux<CloudEvent> all(SortBy sortBy) {
        return query(Filter.all(), sortBy);
    }


    /**
     * @return All cloud events in insertion order
     */
    default Flux<CloudEvent> all(int skip, int limit) {
        return query(Filter.all(), skip, limit);
    }

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    default Flux<CloudEvent> all() {
        return all(0, Integer.MAX_VALUE);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default Flux<CloudEvent> query(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        return query(filter, 0, Integer.MAX_VALUE);
    }

    enum SortBy {
        TIME_ASC, TIME_DESC, NATURAL_ASC, NATURAL_DESC
    }
}