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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;

import java.util.Objects;

import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.filter.Filter.TIME;

/**
 * Configuration for {@link CatchupSubscriptionModel}
 */
@NullMarked
public class CatchupSubscriptionModelConfig {

    static final long DEFAULT_DCB_CATCHUP_POSITION_WINDOW_SIZE = 1000;

    /**
     * Default ceiling on the number of event ids kept to dedupe the catch-up-to-live handover, used by convenience
     * constructors without an explicit {@code cacheSize}. The cache grows to cover the replay-to-live overlap
     * (bounded by write volume during replay, not total history) and evicts oldest-first past this ceiling.
     * Exceeding it causes extra duplicate deliveries, never loss (at-least-once). Well above the previous
     * {@code 100} so a rebuild under heavy concurrent writes no longer evicts the overlap before live re-delivers
     * it. Each id is a short string, so lower it to cap memory or raise it to cut duplicates further.
     */
    public static final int DEFAULT_HANDOVER_CACHE_SIZE = 100_000;

    /**
     * The ceiling on the number of CloudEvent ids kept in-memory to deduplicate the switch from catch-up mode to
     * live subscription mode. The cache grows to cover the overlap the live subscription re-delivers up to this
     * ceiling, then evicts oldest-first. Exceeding the ceiling yields extra duplicate deliveries, never loss.
     */
    public final int cacheSize;
    public final CheckpointStorageConfig subscriptionStorageConfig;
    public final SortBy catchupPhaseSortBy;
    /**
     * The DCB sequence-position window size used when {@link CatchupSubscriptionModel} replays in DCB mode. The replay
     * pages through the DCB sequence in windows of this many positions so a large rebuild does not materialize the
     * whole matched set at once. Ignored in stream mode.
     */
    public final long dcbCatchupPositionWindowSize;

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given cache size. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param cacheSize The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                  and "subscription" mode ({@link SubscriptionHandle}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     */
    public CatchupSubscriptionModelConfig(int cacheSize) {
        this(cacheSize, CheckpointStorageConfig.dontUseCheckpointStorage());
    }

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given subscription storage config. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param subscriptionStorageConfig Configures if and how checkpoint persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(CheckpointStorageConfig subscriptionStorageConfig) {
        this(DEFAULT_HANDOVER_CACHE_SIZE, subscriptionStorageConfig);
    }

    /**
     * Create a new {@code CatchupSubscriptionModelConfig} will the given settings. Will default to sort by time and then stream version (if time is the same for two events)
     * during the catchup phase. You can change this by calling {@link #catchupPhaseSortBy(SortBy)}.
     *
     * @param cacheSize                 The number of cloud events id's to store in-memory when switching from "catch-up" mode (i.e. querying the {@link EventStoreQueries} API)
     *                                  and "subscription" mode ({@link SubscriptionHandle}). The cache is needed to reduce the number of duplicate events the occurs when switching.
     * @param subscriptionStorageConfig Configures if and how checkpoint persistence should be handled during the catch-up phase.
     */
    public CatchupSubscriptionModelConfig(int cacheSize, CheckpointStorageConfig subscriptionStorageConfig) {
        // Sorts by time, falling back to stream version when time ties, which guarantees order within a stream but
        // not full insertion order. Not SortBy.time(ASCENDING).thenNatural(ASCENDING): on MongoDB that prevents the
        // sort from using a time index (see https://docs.mongodb.com/manual/reference/method/cursor.sort/#return-natural-order).
        // SortBy.time(ASCENDING).then("_id", ASCENDING) would be better on MongoDB, but "_id" is Mongo-specific.
        this(cacheSize, subscriptionStorageConfig, SortBy.ascending(TIME, STREAM_VERSION));
    }

    private CatchupSubscriptionModelConfig(int cacheSize, CheckpointStorageConfig subscriptionStorageConfig, SortBy sortBy) {
        this(cacheSize, subscriptionStorageConfig, sortBy, DEFAULT_DCB_CATCHUP_POSITION_WINDOW_SIZE);
    }

    private CatchupSubscriptionModelConfig(int cacheSize, CheckpointStorageConfig subscriptionStorageConfig, SortBy sortBy, long dcbCatchupPositionWindowSize) {
        if (cacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be greater than or equal to 1");
        }
        Objects.requireNonNull(subscriptionStorageConfig, CheckpointStorageConfig.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(sortBy, SortBy.class + " cannot be null");
        if (dcbCatchupPositionWindowSize < 1) {
            throw new IllegalArgumentException("DCB catch-up position window size must be greater than or equal to 1");
        }
        this.cacheSize = cacheSize;
        this.subscriptionStorageConfig = subscriptionStorageConfig;
        this.catchupPhaseSortBy = sortBy;
        this.dcbCatchupPositionWindowSize = dcbCatchupPositionWindowSize;
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
        return new CatchupSubscriptionModelConfig(cacheSize, subscriptionStorageConfig, sortBy, dcbCatchupPositionWindowSize);
    }

    /**
     * Specify the DCB sequence-position window size used when {@link CatchupSubscriptionModel} replays in DCB mode.
     * The replay pages through the DCB sequence in windows of this many positions, bounding how many matched events
     * are held in memory at once. Ignored in stream mode. Default is {@value #DEFAULT_DCB_CATCHUP_POSITION_WINDOW_SIZE}.
     *
     * @param dcbCatchupPositionWindowSize The number of DCB sequence positions to read per window. Must be at least 1.
     * @return A new instance of {@link CatchupSubscriptionModelConfig}.
     */
    public CatchupSubscriptionModelConfig dcbCatchupPositionWindowSize(long dcbCatchupPositionWindowSize) {
        return new CatchupSubscriptionModelConfig(cacheSize, subscriptionStorageConfig, catchupPhaseSortBy, dcbCatchupPositionWindowSize);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof CatchupSubscriptionModelConfig that)) return false;
        return cacheSize == that.cacheSize &&
                dcbCatchupPositionWindowSize == that.dcbCatchupPositionWindowSize &&
                Objects.equals(subscriptionStorageConfig, that.subscriptionStorageConfig) &&
                Objects.equals(catchupPhaseSortBy, that.catchupPhaseSortBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheSize, subscriptionStorageConfig, catchupPhaseSortBy, dcbCatchupPositionWindowSize);
    }

    @Override
    public String toString() {
        return "CatchupSubscriptionModelConfig{" +
                "cacheSize=" + cacheSize +
                ", subscriptionStorageConfig=" + subscriptionStorageConfig +
                ", catchupPhaseSortBy=" + catchupPhaseSortBy +
                ", dcbCatchupPositionWindowSize=" + dcbCatchupPositionWindowSize +
                '}';
    }
}