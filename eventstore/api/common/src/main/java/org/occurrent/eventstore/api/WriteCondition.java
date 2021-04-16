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

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.eq;

/**
 * A write condition may be applied when writing events to an event store. If the write condition is not fulfilled the events
 * will not be written.
 */
public abstract class WriteCondition {

    private WriteCondition() {
    }

    /**
     * Stream version doesn't matter, essentially the same as an unconditional write condition.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    public static WriteCondition anyStreamVersion() {
        return StreamVersionWriteCondition.any();
    }

    /**
     * Stream version must be equal to the specified {@code version} in order for the events to be written
     * to the event store.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    public static WriteCondition streamVersionEq(long version) {
        return streamVersion(eq(version));
    }

    /**
     * Stream version must match the specified {@link Condition} in order for the events to be written
     * to the event store.
     *
     * @return A {@link WriteCondition} with the behavior specified above.
     */
    public static WriteCondition streamVersion(Condition<Long> condition) {
        return StreamVersionWriteCondition.streamVersion(condition);
    }

    public boolean isAnyStreamVersion() {
        return this instanceof StreamVersionWriteCondition && ((StreamVersionWriteCondition) this).isAny();
    }

    public static class StreamVersionWriteCondition extends WriteCondition {
        public final Condition<Long> condition;

        private StreamVersionWriteCondition(Condition<Long> condition) {
            this.condition = condition;
        }

        public static StreamVersionWriteCondition streamVersion(Condition<Long> condition) {
            requireNonNull(condition, "Stream version condition cannot be null");
            return new StreamVersionWriteCondition(condition);
        }

        public static StreamVersionWriteCondition any() {
            return new StreamVersionWriteCondition(null);
        }

        @Override
        public String toString() {
            return condition.description;
        }

        public boolean isAny() {
            return condition == null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StreamVersionWriteCondition)) return false;
            StreamVersionWriteCondition that = (StreamVersionWriteCondition) o;
            return Objects.equals(condition, that.condition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(condition);
        }
    }
}