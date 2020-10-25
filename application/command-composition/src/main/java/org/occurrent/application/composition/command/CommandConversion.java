package org.occurrent.application.composition.command;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommandConversion {

    public static <T> Function<List<T>, List<T>> toListCommand(Function<Stream<T>, Stream<T>> command) {
        return events -> command.apply(events.stream()).collect(Collectors.toList());
    }

    public static <T> Function<Stream<T>, Stream<T>> toStreamCommand(Function<List<T>, List<T>> command) {
        return events -> command.apply(events.collect(Collectors.toList())).stream();
    }

}