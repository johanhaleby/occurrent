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

    /**
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to never store the position.
     */
    public ReactorDurableSubscriptionModelConfig(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
        Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
    }

    /**
     * @param persistPositionForEveryNCloudEvent Store the cloud event position for every {@code n} cloud event.
     */
    public ReactorDurableSubscriptionModelConfig(int persistPositionForEveryNCloudEvent) {
        this(new EveryN(persistPositionForEveryNCloudEvent));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReactorDurableSubscriptionModelConfig)) return false;
        ReactorDurableSubscriptionModelConfig that = (ReactorDurableSubscriptionModelConfig) o;
        return Objects.equals(persistCloudEventPositionPredicate, that.persistCloudEventPositionPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistCloudEventPositionPredicate);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ReactorDurableSubscriptionModelConfig.class.getSimpleName() + "[", "]")
                .add("persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate)
                .toString();
    }
}