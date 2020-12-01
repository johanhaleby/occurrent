
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
import org.occurrent.application.composition.command.internal.SequentialFunctionComposer;
import org.occurrent.application.composition.command.partial.PartialListCommandApplication;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Compose several "list commands" ({@code Function<List<T>, List<T>>}) into one by leveraging function composition.
 * Commands will be executed in left-to-right order, for example:
 * <br>
 * <br>
 * <pre>
 * Function&lt;List&lt;T&gt;, List&lt;T&gt;&gt; domainFunction1 = ..
 * Function&lt;List&lt;T&gt;, List&lt;T&gt;&gt; domainFunction2 = ..
 *
 * applicationService.execute("streamid", composeCommands(domainFunction1, domainFunction2));
 * </pre>
 * <p>
 * In this example, {@code domainFunction1} will execute before {@code domainFunction2} and the events returned from {@code domainFunction2}
 * will be appended as input to {@code domainFunction2}. All events will then be written atomically to an event store.
 * <br>
 * <br>
 * Note that in most cases the domain function will not have the form {@code Function<List<T>, List<T>>}. You can then
 * use {@link PartialListCommandApplication} to create partially applied functions that you can then compose.
 */
public class ListCommandComposition {

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
    public static <T> Function<List<T>, List<T>> composeCommands(Function<List<T>, List<T>> firstCommand, Function<List<T>, List<T>> secondCommand, Function<List<T>, List<T>>... additionalCommands) {
        return composeCommands(CreateListFromVarArgs.createList(firstCommand, secondCommand, additionalCommands));
    }

    /**
     * Compose the supplied list of commands into a single function.
     *
     * @param commands The commands to compose
     * @param <T>      The domain event type
     * @return A single function that is a composition of all supplied commands
     */
    public static <T> Function<List<T>, List<T>> composeCommands(List<Function<List<T>, List<T>>> commands) {
        SequentialFunctionComposer<T> sequentialFunctionComposer = new SequentialFunctionComposer<>(commands);
        return sequentialFunctionComposer.compose();
    }

    /**
     * Compose the supplied stream of commands into a single function.
     *
     * @param commands The commands to compose
     * @param <T>      The domain event type
     * @return A single function that is a composition of all supplied commands
     */
    public static <T> Function<List<T>, List<T>> composeCommands(Stream<Function<List<T>, List<T>>> commands) {
        return composeCommands(commands.collect(Collectors.toList()));
    }
}