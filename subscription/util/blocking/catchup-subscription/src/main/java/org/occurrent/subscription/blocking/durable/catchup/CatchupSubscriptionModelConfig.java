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

package org.occurrent.subscription.blocking.durable.catchup;

import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.Objects;

import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.filter.Filter.TIME;

/**
 * Configuration for {@link CatchupSubscriptionModel}
 */
public class CatchupSubscriptionModelConfig {

    public final int cacheSize;
    public final SubscriptionPositionStorageConfig subscriptionStorageConfig;
    public final SortBy catchupPhaseSortBy;

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given cache size. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param cacheSize The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                  and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     */
    public CatchupSubscriptionModelConfig(int cacheSize) {
        this(cacheSize, SubscriptionPositionStorageConfig.dontUseSubscriptionPositionStorage());
    }

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given subscription storage config. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param subscriptionStorageConfig Configures if and how subscription position persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(SubscriptionPositionStorageConfig subscriptionStorageConfig) {
        this(100, subscriptionStorageConfig);
    }

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given settings. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param cacheSize                 The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                                  and "subscription" mode ({@link Subscription}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     * @param subscriptionStorageConfig Configures if and how subscription position persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(int cacheSize, SubscriptionPositionStorageConfig subscriptionStorageConfig) {
        // We sort by time but fallback to stream version if time is the same for two events.
        // While this is will _not_ sort the entire in database in insertion order, it at least guarantees
        // order within a stream. Note that we can't do SortBy.time(ASCENDING).thenNatural(ASCENDING)
        // since for certain databases (MongoDB) this will prevent sorting from using a "time index" for queries
        // (see https://docs.mongodb.com/manual/reference/method/cursor.sort/#return-natural-order).
        // For MongoDB, doing SortBy.time(ASCENDING).then("_id", ASCENDING) would be better,
        // but "_id" is unique to MongoDB so we cannot use it here.
        this(cacheSize, subscriptionStorageConfig, SortBy.ascending(TIME, STREAM_VERSION));
    }

    private CatchupSubscriptionModelConfig(int cacheSize, SubscriptionPositionStorageConfig subscriptionStorageConfig, SortBy sortBy) {
        if (cacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be greater than or equal to 1");
        }
        Objects.requireNonNull(subscriptionStorageConfig, SubscriptionPositionStorageConfig.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(sortBy, SortBy.class + " cannot be null");
        this.cacheSize = cacheSize;
        this.subscriptionStorageConfig = subscriptionStorageConfig;
        this.catchupPhaseSortBy = sortBy;
    }

    /**
     * Specify how to sort the events that are read from the event store during catch-up phase. By default, "natural order" is used when
     * no filter is specified, and time then stream version, if time is the same for two events. If you know that you're reading from a datastore
     * that has insertion order support, or if you need a different sort events after they've been filtered by the {@link CatchupSubscriptionModel},
     * you can specify your own {@code sortBy} instance here. Note that you most likely need the {@code sortBy} instance be covered by an index for
     * it to work efficiently.
     * <p>
     * For example, in MongoDB, if you only sort by "time", then if two events have the exact same time, then the order returned from MongoDB is unspecified.
     * Thus the default value of {@code sortBy} is <code>SortBy.ascending(TIME, STREAM_VERSION)</code>. However, say that your filter is
     * <code>Filter.type("<some type>")</code>, then you could create an index, <code>{type : 1, time : 1, _id : 1}</code>, and call {@link #catchupPhaseSortBy(SortBy)}
     * with <code>SortBy.ascending(TIME, "_id")</code>. This means that MongoDB can efficiently both search for the correct type and then perform a sort based on time,
     * but use "insertion order" if time is the same for two or more events. If you don't supply a filter, then you can instead create the index <code>{time : 1, _id : 1}</code>.
     * </p>
     *
     * @param sortBy The {@link SortBy} instance to use during catchup phase. Default is <code>SortBy.ascending(TIME, STREAM_VERSION)</code>.
     * @return A new instance of {@link CatchupSubscriptionModel}.
     */
    public CatchupSubscriptionModelConfig catchupPhaseSortBy(SortBy sortBy) {
        return new CatchupSubscriptionModelConfig(cacheSize, subscriptionStorageConfig, sortBy);
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