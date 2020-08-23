package org.occurrent.eventstore.api.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Represents an event stream.
 */
public interface EventStream<T> extends Iterable<T> {

    /**
     * @return The id of the event stream
     */
    String id();

    /**
     * The event stream version. It is equal to {@code 0} if event stream is empty.
     *
     * @return The current version of the event stream
     * @see #isEmpty()
     */
    long version();

    /**
     * @return The events as a {@link Stream}.
     */
    Flux<T> events();

    /**
     * @return {@code true} if event stream is empty, {@code false} otherwise.
     */
    default boolean isEmpty() {
        return version() == 0;
    }

    /**
     * Warning!!! This is a blocking operation!
     */
    @SuppressWarnings("NullableProblems")
    @Override
    default Iterator<T> iterator() {
        return events().toIterable().iterator();
    }

    /**
     * Warning!!! This is a blocking operation!
     *
     * @return The events in this stream as a list
     */
    default Mono<List<T>> eventList() {
        return events().collectList();
    }


    /**
     * Apply a mapping function to the {@link EventStream}
     *
     * @param fn   The function to apply for each event.
     * @param <T2> The return type
     * @return A new {@link EventStream} where events are converted to {@code T2}.
     */
    default <T2> EventStream<T2> map(Function<T, T2> fn) {
        return new EventStream<T2>() {

            @Override
            public String id() {
                return EventStream.this.id();
            }

            @Override
            public long version() {
                return EventStream.this.version();
            }

            @Override
            public Flux<T2> events() {
                return EventStream.this.events().map(fn);
            }

            @Override
            public String toString() {
                return "EventStream{" +
                        "id='" + id() + '\'' +
                        ", version=" + version() +
                        ", events=" + events() +
                        '}';
            }
        };
    }
}