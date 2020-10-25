
package org.occurrent.application.composition.command;


import org.occurrent.application.composition.command.internal.CreateListFromVarArgs;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class StreamCommandComposition {

    @SafeVarargs
    public static <T> Function<Stream<T>, Stream<T>> composeCommands(Function<Stream<T>, Stream<T>> firstCommand, Function<Stream<T>, Stream<T>> secondCommand, Function<Stream<T>, Stream<T>>... additionalCommands) {
        return composeCommands(CreateListFromVarArgs.createList(firstCommand, secondCommand, additionalCommands));
    }

    public static <T> Function<Stream<T>, Stream<T>> composeCommands(Stream<Function<Stream<T>, Stream<T>>> commands) {
        List<Function<List<T>, List<T>>> collect = commands.map(CommandConversion::toListCommand).collect(Collectors.toList());
        return CommandConversion.toStreamCommand(ListCommandComposition.composeCommands(collect));
    }

    public static <T> Function<Stream<T>, Stream<T>> composeCommands(List<Function<Stream<T>, Stream<T>>> commands) {
        return composeCommands(commands.stream());
    }
}