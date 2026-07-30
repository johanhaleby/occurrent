/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.eventstore.api.internal;

import org.jspecify.annotations.NullMarked;

/**
 * Shared validation for the {@code updateEvent(..)} operation across all event stores, so that they throw with
 * identical wording when the caller-supplied update function returns {@code null}.
 */
@NullMarked
public final class UpdateEventFunctionValidator {

    private UpdateEventFunctionValidator() {
    }

    /**
     * Create the {@link IllegalArgumentException} to throw when the update function passed to
     * {@code updateEvent(..)} returns {@code null}, with a message consistent across all event stores.
     *
     * @return the exception to throw
     */
    public static IllegalArgumentException updateFunctionReturnedNull() {
        return new IllegalArgumentException("Cloud event update function is not allowed to return null");
    }
}
