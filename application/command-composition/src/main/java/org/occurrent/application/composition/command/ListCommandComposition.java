
package org.occurrent.application.composition.command;


import org.occurrent.application.composition.command.internal.SequentialFunctionComposer;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.occurrent.application.composition.command.internal.CreateListFromVarArgs;

public class ListCommandComposition {

    @SafeVarargs
    public static <T> Function<List<T>, List<T>> composeCommands(Function<List<T>, List<T>> firstCommand, Function<List<T>, List<T>> secondCommand, Function<List<T>, List<T>>... additionalCommands) {
        return composeCommands(CreateListFromVarArgs.createList(firstCommand, secondCommand, additionalCommands));
    }

    public static <T> Function<List<T>, List<T>> composeCommands(List<Function<List<T>, List<T>>> commands) {
        SequentialFunctionComposer<T> sequentialFunctionComposer = new SequentialFunctionComposer<>(commands);
        return sequentialFunctionComposer.compose();
    }

    public static <T> Function<List<T>, List<T>> composeCommands(Stream<Function<List<T>, List<T>>> commands) {
        return composeCommands(commands.collect(Collectors.toList()));
    }
}