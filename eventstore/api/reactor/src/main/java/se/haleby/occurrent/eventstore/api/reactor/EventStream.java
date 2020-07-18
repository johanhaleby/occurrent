package se.haleby.occurrent.eventstore.api.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface EventStream<T> extends Iterable<T> {

    String id();

    long version();

    Flux<T> events();

    default boolean isEmpty() {
        return version() == 0;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    default Iterator<T> iterator() {
        return events().toIterable().iterator();
    }

    default Mono<List<T>> eventList() {
        return events().collectList();
    }


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