
package org.occurrent.application.command.composition;


import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.occurrent.application.command.composition.InternalCommandComposer.createList;

public class ListCommandComposition {

    @SafeVarargs
    public static <T> Function<List<T>, List<T>> composeCommands(Function<List<T>, List<T>> firstCommand, Function<List<T>, List<T>> secondCommand, Function<List<T>, List<T>>... additionalCommands) {
        return composeCommands(createList(firstCommand, secondCommand, additionalCommands));
    }

    public static <T> Function<List<T>, List<T>> composeCommands(List<Function<List<T>, List<T>>> commands) {
        InternalCommandComposer<T> internalCommandComposer = new InternalCommandComposer<>(commands);
        return internalCommandComposer.compose();
    }

    public static <T> Function<List<T>, List<T>> composeCommands(Stream<Function<List<T>, List<T>>> commands) {
        return composeCommands(commands.collect(Collectors.toList()));
    }
}