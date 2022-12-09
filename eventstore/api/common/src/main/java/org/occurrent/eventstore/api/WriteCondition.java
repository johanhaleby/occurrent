/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.api;

import org.occurrent.condition.Condition;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.eq;

/**
 * A write condition may be applied when writing events to an event store. If the write condition is not fulfilled the events
 * will not be written.
 */
public sealed interface WriteCondition {

    /**
     * Stream version doesn't matter, essentially the same as an unconditional write condition.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    static WriteCondition anyStreamVersion() {
        return StreamVersionWriteCondition.any();
    }

    /**
     * Stream version must be equal to the specified {@code version} in order for the events to be written
     * to the event store.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    static WriteCondition streamVersionEq(long version) {
        return streamVersion(eq(version));
    }

    /**
     * Stream version must match the specified {@link Condition} in order for the events to be written
     * to the event store.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    static WriteCondition streamVersion(Condition<Long> condition) {
        return StreamVersionWriteCondition.streamVersion(condition);
    }

    default boolean isAnyStreamVersion() {
        return this instanceof StreamVersionWriteCondition && ((StreamVersionWriteCondition) this).isAny();
    }

    record StreamVersionWriteCondition(Condition<Long> condition) implements WriteCondition {

        public static StreamVersionWriteCondition streamVersion(Condition<Long> condition) {
            requireNonNull(condition, "Stream version condition cannot be null");
            return new StreamVersionWriteCondition(condition);
        }

        public static StreamVersionWriteCondition any() {
            return new StreamVersionWriteCondition(null);
        }

        @Override
        public String toString() {
            return condition == null ? "any" : condition.description();
        }

        public boolean isAny() {
            return condition == null;
        }
    }
}