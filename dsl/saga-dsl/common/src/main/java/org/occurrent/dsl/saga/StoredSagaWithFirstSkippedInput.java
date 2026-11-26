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
 * A saga instance and the first record under its quarantine id, as one write left them, see
 * {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInput(String)}.
 *
 * @param saga              the whole instance, with its state
 * @param firstSkippedInput the first record in {@link SkippedInput#ORDER} under the quarantine id of {@code saga}, or
 *                          {@code null} when {@code saga} has no quarantine id or no record is under it
 */
public record StoredSagaWithFirstSkippedInput(StoredSaga saga, @Nullable SkippedInput firstSkippedInput) {

    public StoredSagaWithFirstSkippedInput {
        requireNonNull(saga, "saga cannot be null");
    }
}
