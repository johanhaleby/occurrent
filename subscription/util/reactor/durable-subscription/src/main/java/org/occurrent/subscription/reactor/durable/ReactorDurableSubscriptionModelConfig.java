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
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to never store the position.
     */
    public ReactorDurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
        this(persistCloudEventPositionPredicate, false);
    }

    /**
     * @param persistPositionForEveryNCloudEvent Store the cloud event position for every {@code n} cloud event.
     */
    public ReactorDurableSubscriptionModelConfig(int persistPositionForEveryNCloudEvent) {
        this(new EveryN(persistPositionForEveryNCloudEvent));
    }

    private ReactorDurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate, boolean startWhenNoStartPositionCanBeRecorded) {
        Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
        this.startWhenNoStartPositionCanBeRecorded = startWhenNoStartPositionCanBeRecorded;
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
        return new ReactorDurableSubscriptionModelConfig(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof ReactorDurableSubscriptionModelConfig that)) return false;
        return startWhenNoStartPositionCanBeRecorded == that.startWhenNoStartPositionCanBeRecorded
               && Objects.equals(persistCloudEventPositionPredicate, that.persistCloudEventPositionPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistCloudEventPositionPredicate, startWhenNoStartPositionCanBeRecorded);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ReactorDurableSubscriptionModelConfig.class.getSimpleName() + "[", "]")
                .add("persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate)
                .add("startWhenNoStartPositionCanBeRecorded=" + startWhenNoStartPositionCanBeRecorded)
                .toString();
    }
}