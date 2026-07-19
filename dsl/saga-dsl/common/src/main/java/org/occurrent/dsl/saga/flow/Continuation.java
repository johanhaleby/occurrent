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

package org.occurrent.dsl.saga.flow;

import static java.util.Objects.requireNonNull;

/**
 * Where a flow saga goes after a branch, join, or timeout fires. It is data, not a value returned from a user lambda, so
 * the builder can validate the whole step graph (every {@link GoTo} target exists) and the graph can be rendered.
 */
public sealed interface Continuation {

    /** Advance to the next step in declaration order, completes the saga if the current step is the last one. */
    record Next() implements Continuation {
    }

    /** Jump to the named step. A back-edge (including to the current step) models a loop or a retry. */
    record GoTo(String stepName) implements Continuation {
        public GoTo {
            requireNonNull(stepName, "stepName cannot be null");
        }
    }

    /** Complete the saga. */
    record End() implements Continuation {
    }

    /** Advance to the next declared step (or complete if there is none). */
    static Continuation next() {
        return new Next();
    }

    /** Jump to the named step. */
    static Continuation goTo(String stepName) {
        return new GoTo(stepName);
    }

    /** Complete the saga. */
    static Continuation end() {
        return new End();
    }
}
