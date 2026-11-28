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

package org.occurrent.dsl.saga.codec;

import org.jspecify.annotations.Nullable;

/**
 * What a store queries a saga instance on, which {@link SagaCodec} derives from the instance.
 *
 * @param storedStatus             the status as stored, see {@link SagaCodec#storedStatuses(org.occurrent.dsl.saga.SagaStatus)}
 * @param nextTimerDueAtEpochMilli when the earliest pending timer is due, or {@code null} when there is none
 * @param updatedAtEpochMilli      when the instance was last updated, or {@code null} when it never was
 * @param releasing                whether a release of the instance is running
 * @param quarantineId             the quarantine id of the instance, or {@code null} when it has none
 */
public record SagaIndex(String storedStatus, @Nullable Long nextTimerDueAtEpochMilli, @Nullable Long updatedAtEpochMilli,
                        boolean releasing, @Nullable String quarantineId) {
}
