package se.haleby.occurrent;

import java.util.function.Function;
import java.util.stream.Stream;

public interface EventStream<T> {

    String id();

    long version();

    Stream<T> events();

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
            public Stream<T2> events() {
                return EventStream.this.events().map(fn);
            }
        };
    }
}