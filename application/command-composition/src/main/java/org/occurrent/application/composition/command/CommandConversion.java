package org.occurrent.application.composition.command;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions to convert functions expecting a list of domain events into a functions expecting a stream of domain events,
 * and vice versa.
 */
public class CommandConversion {

    /**
     * Convert a command taking <code>Stream&lt;T&gt;</code> into one that takes a <code>List&lt;T&gt;</code>.
     *
     * @param command The command to convert
     * @param <T>     The type of the domain event
     * @return A function that does the conversion described above.
     */
    public static <T> Function<List<T>, List<T>> toListCommand(Function<Stream<T>, Stream<T>> command) {
        return events -> command.apply(events.stream()).collect(Collectors.toList());
    }

    /**
     * Convert a command taking <code>List&lt;T&gt;</code> into one that takes a <code>Stream&lt;T&gt;</code>.
     *
     * @param command The command to convert
     * @param <T>     The type of the domain event
     * @return A function that does the conversion described above.
     */
    public static <T> Function<Stream<T>, Stream<T>> toStreamCommand(Function<List<T>, List<T>> command) {
        return events -> command.apply(events.collect(Collectors.toList())).stream();
    }
}