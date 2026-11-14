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
 * A thread that makes tagged conditional appends, and the transient transaction errors they hit per second.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.OPERATIONS)
public class TaggedWriter {

    public long taggedConflicts;

    @Setup(Level.Iteration)
    public void reset() {
        taggedConflicts = 0;
        MongoDcbStore.takeConflicts();
    }

    void append(MongoDcbStore store, String type) {
        store.taggedConditionalAppend(type);
        taggedConflicts += MongoDcbStore.takeConflicts();
    }
}
