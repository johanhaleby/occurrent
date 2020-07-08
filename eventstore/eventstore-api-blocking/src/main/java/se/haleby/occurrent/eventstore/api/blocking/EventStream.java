package se.haleby.occurrent.eventstore.api.blocking;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("NullableProblems")
public interface EventStream<T> extends Iterable<T> {

    String id();

    long version();

    Stream<T> events();

    @Override
    default Iterator<T> iterator() {
        return events().iterator();
    }

    default List<T> eventList() {
        return events().collect(Collectors.toList());
    }

    default <T2> EventStream<T2> map(Function<T, T2> fn) {
        return new EventStream<T2>() {

            @Override
            public Iterator<T2> iterator() {
                return events().iterator();
            }

            @Override
            public String id() {
                return EventStream.this.id();
            }

            @Override
            public long version() {
                return EventStream.this.version();
            }

            @Override
            public Stream<T2> events() {
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