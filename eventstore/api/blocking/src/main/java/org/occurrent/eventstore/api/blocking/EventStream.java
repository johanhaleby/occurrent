/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.eventstore.api.blocking;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents an event stream.
 */
@SuppressWarnings("NullableProblems")
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
    Stream<T> events();

    @Override
    default Iterator<T> iterator() {
        return events().iterator();
    }

    /**
     * @return {@code true} if event stream is empty, {@code false} otherwise.
     */
    default boolean isEmpty() {
        return version() == 0;
    }

    /**
     * @return The events in this stream as a list
     */
    default List<T> eventList() {
        return events().collect(Collectors.toList());
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