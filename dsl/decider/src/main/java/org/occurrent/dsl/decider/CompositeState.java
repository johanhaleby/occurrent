/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.decider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The state of a composed decider (see {@link Decider#compose}). It holds the state of each composed decider positionally:
 * slice {@code i} is the state of the {@code i}:th decider passed to {@code compose}. Read a slice with its concrete type
 * via {@link #slice(int)}.
 * <p>
 * The vararg {@code compose} produces this type. The two and three decider overloads in the Kotlin DSL produce typed
 * {@code Pair} and {@code Triple} states instead, so reach for this only when composing four or more deciders.
 */
public record CompositeState(List<Object> states) {
    public CompositeState(List<Object> states) {
        this.states = Collections.unmodifiableList(new ArrayList<>(states));
    }

    /**
     * Return the state of the decider at {@code index} (the order it was passed to {@link Decider#compose}). The cast is
     * safe by construction because slice {@code i} always holds the state of decider {@code i}.
     */
    @SuppressWarnings("unchecked")
    public <T> T slice(int index) {
        return (T) states.get(index);
    }
}
