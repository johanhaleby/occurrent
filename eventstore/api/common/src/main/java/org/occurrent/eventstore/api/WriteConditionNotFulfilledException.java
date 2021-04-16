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

import java.util.Objects;
import java.util.StringJoiner;

/**
 * The write condition was not fulfilled so events have not been written to the event store.
 * In a typical scenario, if an application read and writes stream A from two different places at the same time,
 * this is effectively the same as an optimistic locking exception and a retry is appropriate. For more advanced
 * write conditions this may not be the case though.
 */
public class WriteConditionNotFulfilledException extends RuntimeException {
    public final String eventStreamId;
    public final long eventStreamVersion;
    public final WriteCondition writeCondition;

    public WriteConditionNotFulfilledException(String eventStreamId, long eventStreamVersion, WriteCondition writeCondition, String message) {
        super(message);
        this.writeCondition = writeCondition;
        this.eventStreamVersion = eventStreamVersion;
        this.eventStreamId = eventStreamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteConditionNotFulfilledException)) return false;
        WriteConditionNotFulfilledException that = (WriteConditionNotFulfilledException) o;
        return eventStreamVersion == that.eventStreamVersion && Objects.equals(eventStreamId, that.eventStreamId) && Objects.equals(writeCondition, that.writeCondition) && Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStreamId, eventStreamVersion, writeCondition);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", WriteConditionNotFulfilledException.class.getSimpleName() + "[", "]")
                .add("eventStreamId='" + eventStreamId + "'")
                .add("eventStreamVersion=" + eventStreamVersion)
                .add("writeCondition=" + writeCondition)
                .add("message=" + super.getMessage())
                .toString();
    }
}
