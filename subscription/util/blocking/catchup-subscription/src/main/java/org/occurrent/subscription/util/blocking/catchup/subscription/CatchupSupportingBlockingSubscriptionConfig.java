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

package org.occurrent.subscription.util.blocking.catchup.subscription;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.util.predicate.EveryN;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configuration for {@link CatchupSupportingBlockingSubscription}
 */
public class CatchupSupportingBlockingSubscriptionConfig {

    public final int cacheSize;
    public final Predicate<CloudEvent> persistCloudEventPositionPredicate;

    /**
     * @param cacheSize                          The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                                           and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to never store the position.
     */
    public CatchupSupportingBlockingSubscriptionConfig(int cacheSize, Predicate<CloudEvent> persistCloudEventPositionPredicate) {
        if (cacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be greater than or equal to 1");
        }
        Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        this.cacheSize = cacheSize;
        this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
    }


    /**
     * @param cacheSize                          The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                                           and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     * @param persistPositionForEveryNCloudEvent Persist the position of every N cloud event so that it's possible to avoid restarting from scratch when subscription is restarted.
     */
    public CatchupSupportingBlockingSubscriptionConfig(int cacheSize, int persistPositionForEveryNCloudEvent) {
        this(cacheSize, new EveryN(persistPositionForEveryNCloudEvent));
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CatchupSupportingBlockingSubscriptionConfig)) return false;
        CatchupSupportingBlockingSubscriptionConfig that = (CatchupSupportingBlockingSubscriptionConfig) o;
        return cacheSize == that.cacheSize &&
                Objects.equals(persistCloudEventPositionPredicate, that.persistCloudEventPositionPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheSize, persistCloudEventPositionPredicate);
    }

    @Override
    public String toString() {
        return "CatchupSupportingBlockingSubscriptionConfig{" +
                "cacheSize=" + cacheSize +
                ", persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate +
                '}';
    }
}