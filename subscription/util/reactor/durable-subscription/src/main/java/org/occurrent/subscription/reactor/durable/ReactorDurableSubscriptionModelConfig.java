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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.util.predicate.EveryN;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Predicate;

/**
 * Config class for {@link ReactorDurableSubscriptionModelConfig}.
 */
@NullMarked
public class ReactorDurableSubscriptionModelConfig {

    public final Predicate<CloudEvent> persistCloudEventPositionPredicate;
    public final boolean startWhenNoStartPositionCanBeRecorded;
    /**
     * How often the quiet position of a subscription is saved, or {@code null} when it is never saved.
     */
    public final @Nullable Duration quietPositionSaveInterval;

    private static final Duration DEFAULT_QUIET_POSITION_SAVE_INTERVAL = Duration.ofMinutes(1);

    /**
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to never store the position.
     */
    public ReactorDurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
        this(persistCloudEventPositionPredicate, false, DEFAULT_QUIET_POSITION_SAVE_INTERVAL);
    }

    /**
     * @param persistPositionForEveryNCloudEvent Store the cloud event position for every {@code n} cloud event.
     */
    public ReactorDurableSubscriptionModelConfig(int persistPositionForEveryNCloudEvent) {
        this(new EveryN(persistPositionForEveryNCloudEvent));
    }

    private ReactorDurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate, boolean startWhenNoStartPositionCanBeRecorded, @Nullable Duration quietPositionSaveInterval) {
        Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
        this.startWhenNoStartPositionCanBeRecorded = startWhenNoStartPositionCanBeRecorded;
        this.quietPositionSaveInterval = quietPositionSaveInterval;
    }

    /**
     * Whether a subscription that asks for the model default, with no checkpoint stored and a wrapped model whose
     * {@code globalCheckpoint()} answers empty, starts anyway instead of being refused. Starting anyway means no
     * start position is recorded before the first delivery, so a crash before the first checkpoint is saved starts
     * over from wherever the feed has reached by then, and an event whose delivery failed before the crash is not
     * redelivered. The default is {@code false}, which refuses such a registration the way
     * {@code ReactorDurableSubscriptionModel}'s javadoc describes.
     *
     * @param startWhenNoStartPositionCanBeRecorded {@code true} to start anyway, accepting that loss window
     * @return A new instance of {@code ReactorDurableSubscriptionModelConfig}
     */
    public ReactorDurableSubscriptionModelConfig startWhenNoStartPositionCanBeRecorded(boolean startWhenNoStartPositionCanBeRecorded) {
        return new ReactorDurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, quietPositionSaveInterval);
    }

    /**
     * How often the position of a subscription that receives no events is saved. A wrapped model that implements
     * {@code QuietPositionReportingSubscriptions}, such as {@code ReactorMongoSubscriptionModel}, reports the position a
     * subscription has read to when a read returned no event for it. It does so also with a
     * {@code ReactorCatchupSubscriptionModel} or {@code ReactorStreamCatchupSubscriptionModel} between it and the
     * {@link ReactorDurableSubscriptionModel}. The {@link ReactorDurableSubscriptionModel} saves that position as the
     * subscription's checkpoint at most once per {@code interval}. A checkpoint saved for an event starts the interval
     * again, so a subscription that stores a checkpoint for an event at least once per {@code interval} gets no extra
     * write.
     * <p>
     * The position is saved from the subscribe on, whatever the {@link #persistCloudEventPositionPredicate} is, but not
     * while the event the running subscription most recently gave the action is one the predicate declined to store,
     * and not while an event is being delivered. So with a predicate that declines some events, such as {@link EveryN}
     * with {@code n} above 1, a subscription that goes quiet right after a declined event gets no position saved until
     * the predicate stores one. A save that fails fails the wrapped model's read the way a failed save after an event
     * fails its action, so the wrapped model reads again from the subscription's position and reports it again.
     * <p>
     * The default is one minute. Keep it well below the time the wrapped model keeps its history, which for MongoDB
     * is the oplog window.
     *
     * @param interval The shortest time between two saves of a subscription's quiet position, greater than zero
     * @return A new instance of {@code ReactorDurableSubscriptionModelConfig}
     * @see #neverSaveQuietPosition()
     */
    public ReactorDurableSubscriptionModelConfig saveQuietPositionEvery(Duration interval) {
        Objects.requireNonNull(interval, "interval cannot be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be greater than zero but was " + interval);
        }
        return new ReactorDurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, interval);
    }

    /**
     * Never save the position of a subscription that receives no events, so a checkpoint is only saved for an event.
     * The stored checkpoint of a subscription that matches no event for longer than the wrapped model keeps its
     * history is then a position that model can no longer start from.
     *
     * @return A new instance of {@code ReactorDurableSubscriptionModelConfig}
     * @see #saveQuietPositionEvery(Duration)
     */
    public ReactorDurableSubscriptionModelConfig neverSaveQuietPosition() {
        return new ReactorDurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded, null);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof ReactorDurableSubscriptionModelConfig that)) return false;
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
        return new StringJoiner(", ", ReactorDurableSubscriptionModelConfig.class.getSimpleName() + "[", "]")
                .add("persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate)
                .add("startWhenNoStartPositionCanBeRecorded=" + startWhenNoStartPositionCanBeRecorded)
                .add("quietPositionSaveInterval=" + quietPositionSaveInterval)
                .toString();
    }
}