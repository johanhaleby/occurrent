
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

package org.occurrent.application.composition.command;


import org.occurrent.application.composition.command.internal.CreateListFromVarArgs;
import org.occurrent.application.composition.command.partial.PartialFunctionApplication;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Compose several "stream commands" ({@code Function<Stream<T>, Stream<T>>}) into one by leveraging function composition.
 * Commands will be executed in left-to-right order, for example:
 * <br>
 * <br>
 * <pre>
 * Function&lt;Stream&lt;T&gt;, Stream&lt;T&gt;&gt; domainFunction1 = ..
 * Function&lt;Stream&lt;T&gt;, Stream&lt;T&gt;&gt; domainFunction2 = ..
 *
 * applicationService.execute("streamid", composeCommands(domainFunction1, domainFunction2));
 * </pre>
 * <p>
 * In this example, {@code domainFunction1} will execute before {@code domainFunction2} and the events returned from {@code domainFunction2}
 * will be appended as input to {@code domainFunction2}. All events will then be written atomically to an event store.
 * <br>
 * <br>
 * Note that in most cases the domain function will not have the form {@code Function<Stream<T>, Stream<T>>}. You can then
 * use {@link PartialFunctionApplication} to create partially applied functions that you can then compose.
 */
public class StreamCommandComposition {

    /**
     * Compose the supplied commands into a single function.
     *
     * @param firstCommand       The first command to compose
     * @param secondCommand      The second command to compose
     * @param additionalCommands Additional commands to compose
     * @param <T>                The domain event type
     * @return A single function that is a composition of all supplied commands
     */
    @SafeVarargs
    public static <T> Function<Stream<T>, Stream<T>> composeCommands(Function<Stream<T>, Stream<T>> firstCommand, Function<Stream<T>, Stream<T>> secondCommand, Function<Stream<T>, Stream<T>>... additionalCommands) {
        return composeCommands(CreateListFromVarArgs.createList(firstCommand, secondCommand, additionalCommands));
    }

    /**
     * Compose the supplied list of commands into a single function.
     *
     * @param commands The commands to compose
     * @param <T>      The domain event type
     * @return A single function that is a composition of all supplied commands
     */
    public static <T> Function<Stream<T>, Stream<T>> composeCommands(Stream<Function<Stream<T>, Stream<T>>> commands) {
        List<Function<List<T>, List<T>>> collect = commands.map(CommandConversion::toListCommand).collect(Collectors.toList());
        return CommandConversion.toStreamCommand(ListCommandComposition.composeCommands(collect));
    }

    /**
     * Compose the supplied stream of commands into a single function.
     *
     * @param commands The commands to compose
     * @param <T>      The domain event type
     * @return A single function that is a composition of all supplied commands
     */
    public static <T> Function<Stream<T>, Stream<T>> composeCommands(List<Function<Stream<T>, Stream<T>>> commands) {
        return composeCommands(commands.stream());
    }
}