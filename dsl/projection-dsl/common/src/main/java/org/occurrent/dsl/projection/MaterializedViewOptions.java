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

package org.occurrent.dsl.projection;

/**
 * Tuning knobs for a framework-built materialized view or update, shared by the blocking and reactor projection DSLs.
 * During a catch-up replay the view coalesces updates per key rather than reading and writing once per event
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * {@code batchSize} bounds how many replayed events are buffered, across every key, before that buffer is read,
 * folded and written. It exists to cap memory, not to change what gets written: a batch boundary in the middle of one
 * key's events still folds them in arrival order onto that key's current state.
 * <p>
 * The live path (no replay in progress) is never batched, this setting only ever affects a catch-up replay.
 *
 * @param batchSize The number of buffered replayed events, across every key, that triggers a flush. {@code 1} folds
 *                  and writes through per event, the same as no coalescing at all, and is the way out for anyone this
 *                  default surprises.
 */
public record MaterializedViewOptions(int batchSize) {

    /**
     * The starting default. To be confirmed against the benchmark harness in
     * <a href="https://github.com/johanhaleby/occurrent/issues/624">#624</a> rather than guessed at twice.
     */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    public MaterializedViewOptions {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1");
        }
    }

    /** The default options: {@link #DEFAULT_BATCH_SIZE}. */
    public static MaterializedViewOptions defaults() {
        return new MaterializedViewOptions(DEFAULT_BATCH_SIZE);
    }
}
