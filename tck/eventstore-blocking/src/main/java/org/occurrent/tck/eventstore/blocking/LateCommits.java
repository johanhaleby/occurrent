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

package org.occurrent.tck.eventstore.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * How a store answers the late-commit suites. A store has one of two answers, and the suites run for real either way.
 * <ul>
 *     <li><strong>It can hold an append between assigning its position and committing it</strong>, which is how a store
 *     that reserves a position before its transaction commits lets a lower position commit after a higher one. The
 *     fixture builds the hold from a public injection point of the store, and the suite checks that the hold really
 *     entered that window.</li>
 *     <li><strong>It commits in position order</strong>, because it assigns a position and commits it under one lock or
 *     one atomic write, so a lower position can never become visible after a higher one. The suite then has nothing to
 *     hold and checks the declaration instead, by requiring the append to be visible as soon as it has returned.</li>
 * </ul>
 * A fixture that gives neither answer fails the suite before its first assertion, because
 * {@link EventStoreFixture#lateCommits()} has no default.
 */
@NullMarked
public final class LateCommits {

    private static final LateCommits IN_POSITION_ORDER = new LateCommits(null);

    private final @Nullable Supplier<HeldAppend> holds;

    private LateCommits(@Nullable Supplier<HeldAppend> holds) {
        this.holds = holds;
    }

    /**
     * This store assigns a position and commits it as one step, so a lower position is never visible after a higher
     * one.
     */
    public static LateCommits inPositionOrder() {
        return IN_POSITION_ORDER;
    }

    /**
     * This store can hold an append after its position is assigned and before it commits.
     *
     * @param holdNextAppend builds a fresh {@link HeldAppend} every time it is called
     */
    public static LateCommits holding(Supplier<HeldAppend> holdNextAppend) {
        return new LateCommits(requireNonNull(holdNextAppend, "holdNextAppend cannot be null"));
    }

    /**
     * Whether this store declared that it commits in position order, in which case there is nothing to hold.
     */
    public boolean commitsInPositionOrder() {
        return holds == null;
    }

    /**
     * Builds a hold on the next append.
     *
     * @throws IllegalStateException if the store declared that it commits in position order
     */
    public HeldAppend holdNextAppend() {
        Supplier<HeldAppend> supplier = holds;
        if (supplier == null) {
            throw new IllegalStateException("This store declared that it commits in position order, so there is "
                    + "nothing to hold");
        }
        return requireNonNull(supplier.get(), "The fixture's holdNextAppend returned null");
    }
}
