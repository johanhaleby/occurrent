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

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.codec.StoredSaga;

import static java.util.Objects.requireNonNull;

/**
 * A saga instance and the {@code seq} and {@code key} of the first record under its quarantine id, as one write left
 * them, see {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInputKey(String)}. The record's event is not read.
 *
 * @param saga  the instance, which the store may return without its state
 * @param first the {@code seq} and {@code key} of the first record in {@link SkippedInput#ORDER} under the quarantine
 *              id of {@code saga}, or {@code null} when {@code saga} has no quarantine id or no record is under it
 */
public record StoredSagaWithFirstSkippedInputKey(StoredSaga saga, @Nullable RecordKey first) {

    public StoredSagaWithFirstSkippedInputKey {
        requireNonNull(saga, "saga cannot be null");
    }

    /**
     * What tells one record from another, see {@link SagaStateStoreSkippedInputs#findSkippedInputs(String, String, int)}.
     *
     * @param seq the record's {@link SkippedInput#seq()}
     * @param key the record's {@link SkippedInput#key()}
     */
    public record RecordKey(long seq, String key) {

        public RecordKey {
            requireNonNull(key, "key cannot be null");
        }

        /** The {@code seq} and {@code key} of {@code input}. */
        public static RecordKey of(SkippedInput input) {
            requireNonNull(input, "input cannot be null");
            return new RecordKey(input.seq(), input.key());
        }
    }
}
