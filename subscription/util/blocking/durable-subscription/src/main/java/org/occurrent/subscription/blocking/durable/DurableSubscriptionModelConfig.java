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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.util.predicate.EveryN;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Predicate;

/**
 * Config class for {@link DurableSubscriptionModel}.
 */
@NullMarked
public class DurableSubscriptionModelConfig {

    public final Predicate<CloudEvent> persistCloudEventPositionPredicate;
    public final boolean startWhenNoStartPositionCanBeRecorded;
    /**
     * How often the quiet position of a subscription is saved, or {@code null} when it is never saved.
     */
    public final @Nullable Duration quietPositionSaveInterval;

    private static final Duration DEFAULT_QUIET_POSITION_SAVE_INTERVAL = Duration.ofMinutes(1);

    /**
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to store no position for an event. A
     *                                           subscription from a {@code StartAt} of your own then has no position stored for a quiet read
     *                                           either. The position it restarts from is still stored when the wrapped model reports that its
     *                                           checkpoint is no longer in the model's history, as the blocking MongoDB models do once the
     *                                           oplog has dropped it. One that starts from a stored position still has its quiet position saved
     *                                           until its first event, since an event the predicate declines turns the save off, see
     *                                           {@link #saveQuietPositionEvery(Duration)}.
     */
    public DurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
        this(persistCloudEventPositionPredicate, false, DEFAULT_QUIET_POSITION_SAVE_INTERVAL);
    }

    /**
     * @param persistPositionForEveryNCloudEvent Store the cloud event position for every {@code n} cloud event.
     */
    public DurableSubscriptionModelConfig(int persistPositionForEveryNCloudEvent) {
        this(new EveryN(persistPositionForEveryNCloudEvent));
    }

    private DurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate, boolean startWhenNoStartPositionCanBeRecorded, @Nullable Duration quietPositionSaveInterval) {
        Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
        this.startWhenNoStartPositionCanBeRecorded = startWhenNoStartPositionCanBeRecorded;
        this.quietPositionSaveInterval = quietPositionSaveInterval;
    }

    /**
     * Whether a subscription that asks for the model default, with no checkpoint stored and a wrapped model whose
     * {@code globalCheckpoint()} answers {@code null}, starts anyway instead of being refused. Starting anyway
     * means no start position is recorded before the first delivery, so a crash before the first checkpoint is
     * saved starts over from wherever the feed has reached by then, and an event whose delivery failed before the
     * crash is not redelivered. The default is {@code false}, which refuses such a subscription with
     * {@code IllegalStateException} from {@code subscribe(..)}.
     *
     * @param startWhenNoStartPositionCanBeRecorded {@code true} to start anyway, accepting that loss window
     * @return A new instance of {@code DurableSubscriptionModelConfig}
     */
    public DurableSubscriptionModelConfig startWhenNoStartPositionCanBeRecorded(boolean startWhenNoStartPositionCanBeRecorded) {
        return new DurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, quietPositionSaveInterval);
    }

    /**
     * How often the position of a subscription that receives no events is saved. A wrapped model that implements
     * {@code QuietPositionReportingSubscriptions}, such as the blocking MongoDB models, reports the position a
     * subscription has read to when a read returned no event for it, and the {@link DurableSubscriptionModel} saves
     * that position as the subscription's checkpoint at most once per {@code interval}. A checkpoint saved for an
     * event starts the interval again, so a subscription that stores a checkpoint for an event at least once per
     * {@code interval} gets no extra write.
     * <p>
     * A subscription that starts from a stored position, the one the store held for it or the one recorded for it when
     * it subscribes from the subscription-model default, has its quiet position saved before its first event whatever
     * the {@link #persistCloudEventPositionPredicate} is. Any other subscription, such as one from a {@code StartAt} of
     * your own, has none saved until the predicate has stored the position of an event, so with a predicate that
     * always returns {@code false} it has no position stored for an event or a quiet read. The position it restarts from
     * is still stored when the wrapped model reports that its checkpoint is no longer in the model's history.
     * <p>
     * The position is not saved while the event the running subscription most recently gave the action is one the
     * predicate declined to store, and not while an event is being delivered. After a pause and a resume, an action of
     * the paused run that is still running keeps the save off until it returns, for as long as that takes. So with a
     * predicate that declines some events, such as {@link EveryN} with {@code n} above 1, a subscription that goes
     * quiet right after a declined event gets no position saved until the predicate stores one.
     * <p>
     * The default is one minute. Keep it well below the time the wrapped model keeps its history, which for MongoDB
     * is the oplog window.
     *
     * @param interval The shortest time between two saves of a subscription's quiet position, greater than zero
     * @return A new instance of {@code DurableSubscriptionModelConfig}
     * @see #neverSaveQuietPosition()
     */
    public DurableSubscriptionModelConfig saveQuietPositionEvery(Duration interval) {
        Objects.requireNonNull(interval, "interval cannot be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be greater than zero but was " + interval);
        }
        return new DurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, interval);
    }

    /**
     * Never save the position of a subscription that receives no events, so a checkpoint is only saved for an event.
     * The stored checkpoint of a subscription that matches no event for longer than the wrapped model keeps its
     * history is then a position that model can no longer start from.
     *
     * @return A new instance of {@code DurableSubscriptionModelConfig}
     * @see #saveQuietPositionEvery(Duration)
     */
    public DurableSubscriptionModelConfig neverSaveQuietPosition() {
        return new DurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, null);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof DurableSubscriptionModelConfig that)) return false;
        return startWhenNoStartPositionCanBeRecorded == that.startWhenNoStartPositionCanBeRecorded
               && Objects.equals(persistCloudEventPositionPredicate, that.persistCloudEventPositionPredicate)
               && Objects.equals(quietPositionSaveInterval, that.quietPositionSaveInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, quietPositionSaveInterval);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DurableSubscriptionModelConfig.class.getSimpleName() + "[", "]")
                .add("persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate)
                .add("startWhenNoStartPositionCanBeRecorded=" + startWhenNoStartPositionCanBeRecorded)
                .add("quietPositionSaveInterval=" + quietPositionSaveInterval)
                .toString();
    }
}