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

import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.Objects;

import static org.occurrent.subscription.util.blocking.catchup.subscription.SubscriptionPositionStorageConfig.dontUseSubscriptionPositionStorage;

/**
 * Configuration for {@link CatchupSubscriptionModel}
 */
public class CatchupSubscriptionModelConfig {

    public final int cacheSize;
    public final SubscriptionPositionStorageConfig subscriptionStorageConfig;

    /**
     * @param cacheSize The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                  and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     */
    public CatchupSubscriptionModelConfig(int cacheSize) {
        this(cacheSize, dontUseSubscriptionPositionStorage());
    }

    /**
     * @param subscriptionStorageConfig Configures if and how subscription position persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(SubscriptionPositionStorageConfig subscriptionStorageConfig) {
        this(100, subscriptionStorageConfig);
    }

    /**
     * @param cacheSize                 The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                                  and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     * @param subscriptionStorageConfig Configures if and how subscription position persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(int cacheSize, SubscriptionPositionStorageConfig subscriptionStorageConfig) {
        if (cacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be greater than or equal to 1");
        }
        Objects.requireNonNull(subscriptionStorageConfig, SubscriptionPositionStorageConfig.class.getSimpleName() + " cannot be null");
        this.cacheSize = cacheSize;
        this.subscriptionStorageConfig = subscriptionStorageConfig;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CatchupSubscriptionModelConfig)) return false;
        CatchupSubscriptionModelConfig that = (CatchupSubscriptionModelConfig) o;
        return cacheSize == that.cacheSize &&
                Objects.equals(subscriptionStorageConfig, that.subscriptionStorageConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheSize, subscriptionStorageConfig);
    }

    @Override
    public String toString() {
        return "CatchupSupportingBlockingSubscriptionConfig{" +
                "cacheSize=" + cacheSize +
                ", catchupPositionPersistenceConfig=" + subscriptionStorageConfig +
                '}';
    }
}