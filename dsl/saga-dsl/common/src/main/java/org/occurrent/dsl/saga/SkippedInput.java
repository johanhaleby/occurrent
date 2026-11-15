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

import java.util.Comparator;

import static java.util.Objects.requireNonNull;

/**
 * One input a quarantined saga instance skipped and recorded, see
 * {@link SagaStateStoreSkippedInputs#recordSkippedInput(String, String, SkippedInput)}. A release replays the records of the
 * instance's current quarantine in {@link #ORDER}.
 * <p>
 * A key is the input's redelivery key, {@code streamId@streamVersion}, when the event has a stream id and a stream
 * version, the same string {@link SagaFailure#input()} holds. Otherwise it is
 * {@code cloudevent:<length of id>:<id>:<source>@cloudevent}, built from the CloudEvent id and source. The
 * {@code @cloudevent} suffix is never a stream version, so the two kinds cannot be mistaken for each other.
 *
 * @param seq the instance's {@link SagaEnvelope#quarantineSkipCount()} that the skip of the input raised before it
 *            recorded the input, so no two records a skip wrote share it. The input the instance was quarantined
 *            on is recorded at {@code -1} when a release starts, so it comes first
 * @param key the input's key
 */
public record SkippedInput(long seq, String key) {

    /** The order a release replays records in, by {@code seq} ascending and then by {@code key}. */
    public static final Comparator<SkippedInput> ORDER = Comparator.comparingLong(SkippedInput::seq).thenComparing(SkippedInput::key);

    public SkippedInput {
        requireNonNull(key, "key cannot be null");
    }
}
