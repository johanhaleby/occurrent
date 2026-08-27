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

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * One saga instance's record of the input it is failing on: which input, where in the subscription it sits, when the
 * failing started, and what came out of the saga. It is written on the first failure of that input and it survives
 * every later failure of the same one, which is what lets the executor measure how long the failure has lasted rather
 * than count attempts.
 * <p>
 * The record outliving a single attempt is the point. An input that fails once and succeeds on redelivery clears it,
 * an input that keeps failing past the runner's quarantine budget turns the instance {@link SagaStatus#QUARANTINED},
 * and until that budget elapses the instance is still {@link SagaStatus#ACTIVE} with a record attached. So a present
 * record does not by itself mean a quarantined instance. Read {@link SagaInstance#status()} for that, and read this
 * for why.
 * <p>
 * What is here and what is not follows the same rule as the rest of {@link SagaInstance}. An operator asking why an
 * instance stopped needs the exception's type and message, and enough to find the event by. The event payload and the
 * stack trace are not lifecycle, so they stay in the log the executor already wrote them to.
 * <p>
 * {@code input} is what identifies the failing event, and {@code position} is a convenience beside it. An event store
 * that assigns no global position, one built with {@code withoutStreamPosition()} for instance, still gives its events
 * a stream id and a stream version, so such an event is recorded and quarantined like any other and answers
 * {@code null} here. Read {@code input} when you want the identity and {@code position} when you have a feed that
 * numbers its events and you want the number.
 *
 * @param input          the failing input's redelivery key, which is its stream id with its version, or its global
 *                       position. The same string the executor compares against to tell one failing input from the next
 * @param position       the global subscription position of the failing event, or {@code null} when the event carries
 *                       none, in which case {@code input} holds its stream id and version instead
 * @param firstFailedAt  when this input first failed, which is when the quarantine budget started running
 * @param failureType    the class name of the exception the saga or its dispatcher threw
 * @param failureMessage that exception's message, or {@code null} when it had none
 */
public record SagaFailure(String input,
                          @Nullable Long position,
                          Instant firstFailedAt,
                          String failureType,
                          @Nullable String failureMessage) {

    public SagaFailure {
        requireNonNull(input, "input cannot be null");
        requireNonNull(firstFailedAt, "firstFailedAt cannot be null");
        requireNonNull(failureType, "failureType cannot be null");
        if (position != null && position < 0) {
            throw new IllegalArgumentException("position cannot be negative, was " + position);
        }
    }
}
