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

package org.occurrent.benchmark.dcbstripes;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * A thread that makes type-only conditional appends, and per second how many were appended, how many its condition
 * refused, and the transient transaction errors they hit. The benchmark's own score counts attempts, both outcomes.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.OPERATIONS)
public class TypeOnlyWriter {

    public long typeOnlyAppended;
    public long typeOnlyRefused;
    public long typeOnlyConflicts;

    @Setup(Level.Iteration)
    public void reset() {
        typeOnlyAppended = 0;
        typeOnlyRefused = 0;
        typeOnlyConflicts = 0;
        MongoDcbStore.takeConflicts();
    }

    void append(MongoDcbStore store, String type) {
        if (store.typeOnlyConditionalAppend(type)) {
            typeOnlyAppended++;
        } else {
            typeOnlyRefused++;
        }
        typeOnlyConflicts += MongoDcbStore.takeConflicts();
    }
}
