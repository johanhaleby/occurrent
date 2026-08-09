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

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.StringJoiner;

/**
 * A {@code CheckpointStorage} refused a checkpoint write because its {@link CheckpointWriteCondition} was not met.
 * <p>
 * This means the node that attempted the write is not the one the stored version belongs to, most often because its
 * lease moved to another node while it was still delivering. That is the state of another machine rather than a
 * mistake in the calling code, so unlike {@code WriteConditionNotFulfilledException} on the event store side,
 * passing a different argument does not fix it, and this does not join
 * {@link SubscriptionRefusedException}'s sealed family. It extends {@link IllegalStateException} instead, and it
 * must never be retried on the path that threw it, since retrying only repeats the write that was already refused.
 */
@NullMarked
public class CheckpointWriteConditionNotFulfilledException extends IllegalStateException {

    /**
     * The id of the subscription whose checkpoint write was refused.
     */
    public final String subscriptionId;

    /**
     * The version stored at the time the condition was evaluated, or empty if the store had no version recorded
     * for this subscription id.
     */
    public final OptionalLong storedVersion;

    /**
     * The condition that was not fulfilled.
     */
    public final CheckpointWriteCondition condition;

    /**
     * Creates an exception with the standard message describing the condition that was not fulfilled and the
     * version the store actually had. This is the message every Occurrent checkpoint storage produces, so prefer
     * this constructor over supplying your own message.
     *
     * @param subscriptionId The id of the subscription whose checkpoint write was refused
     * @param storedVersion  The version stored when the condition was evaluated, or empty if none was stored
     * @param condition      The condition that was not fulfilled
     */
    public CheckpointWriteConditionNotFulfilledException(String subscriptionId, OptionalLong storedVersion, CheckpointWriteCondition condition) {
        this(subscriptionId, storedVersion, condition, String.format(
                "%s was not fulfilled for subscription %s. Condition was %s but stored version was %s.",
                CheckpointWriteCondition.class.getSimpleName(), subscriptionId, condition,
                storedVersion.isPresent() ? String.valueOf(storedVersion.getAsLong()) : "not set"));
    }

    /**
     * Creates an exception with a message of your own, for a store that has something to add beyond the id, the
     * stored version and the condition.
     *
     * @param subscriptionId The id of the subscription whose checkpoint write was refused
     * @param storedVersion  The version stored when the condition was evaluated, or empty if none was stored
     * @param condition      The condition that was not fulfilled
     * @param message        The message to report
     */
    public CheckpointWriteConditionNotFulfilledException(String subscriptionId, OptionalLong storedVersion, CheckpointWriteCondition condition, String message) {
        super(message);
        this.subscriptionId = subscriptionId;
        this.storedVersion = storedVersion;
        this.condition = condition;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof CheckpointWriteConditionNotFulfilledException that)) return false;
        return Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(storedVersion, that.storedVersion)
                && Objects.equals(condition, that.condition) && Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, storedVersion, condition);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CheckpointWriteConditionNotFulfilledException.class.getSimpleName() + "[", "]")
                .add("subscriptionId='" + subscriptionId + "'")
                .add("storedVersion=" + storedVersion)
                .add("condition=" + condition)
                .add("message=" + super.getMessage())
                .toString();
    }
}
