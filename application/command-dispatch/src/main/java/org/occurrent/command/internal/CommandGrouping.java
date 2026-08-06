/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.command.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Splits a dispatch batch into runs that share a target, so a {@code CommandDispatcher} can write each run as one
 * append.
 */
public final class CommandGrouping {

    private CommandGrouping() {
    }

    /**
     * Calls {@code action} once per run of adjacent items whose {@code keyOf} keys are equal, in order.
     * <p>
     * Runs are <i>consecutive</i> only. Items are never reordered to make a run longer, because dispatch is
     * contractually in order, so {@code [a, b, a]} is three runs rather than two.
     * <p>
     * {@code keyOf} is applied to every item up front, exactly once each, before {@code action} runs for the first
     * time. Resolving a key can fail (a missing {@code @TargetStreamId}, a decider that does not recognise a
     * command), and resolving lazily would throw partway through, after earlier runs had already been written. Doing
     * it up front instead means such a batch fails having written nothing. It also keeps a key derivation that
     * allocates, or is otherwise not free, to one call per item.
     * <p>
     * An empty list does nothing. Callers rely on that. A saga reaction that only arms a timer still reaches
     * {@code dispatchAll} with an empty list.
     *
     * @param items  the batch to split, must not contain null
     * @param keyOf  derives the target an item is written to
     * @param action receives each run's key and the run itself, an unmodifiable copy of that stretch of {@code items},
     *               so a later change to {@code items} is not visible through it
     * @param <T>    the item type
     * @param <K>    the target key type, which must have value equality
     */
    public static <T, K> void forEachRun(List<T> items, Function<T, K> keyOf, BiConsumer<K, List<T>> action) {
        requireNonNull(items, "items cannot be null");
        requireNonNull(keyOf, "keyOf cannot be null");
        requireNonNull(action, "action cannot be null");

        List<K> keys = new ArrayList<>(items.size());
        for (T item : items) {
            keys.add(requireNonNull(keyOf.apply(requireNonNull(item, "items cannot contain null")), "key cannot be null"));
        }

        int runStart = 0;
        while (runStart < items.size()) {
            K key = keys.get(runStart);
            int runEnd = runStart + 1;
            while (runEnd < items.size() && keys.get(runEnd).equals(key)) {
                runEnd++;
            }
            action.accept(key, List.copyOf(items.subList(runStart, runEnd)));
            runStart = runEnd;
        }
    }
}
