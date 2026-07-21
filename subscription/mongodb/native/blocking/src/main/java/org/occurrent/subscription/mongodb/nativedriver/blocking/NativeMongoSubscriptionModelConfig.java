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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the {@code NativeMongoSubscriptionModel}.
 */
@NullMarked
public class NativeMongoSubscriptionModelConfig {

    final RetryStrategy retryStrategy;
    final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    final @Nullable Integer batchSize;
    final @Nullable Duration maxAwaitTime;

    /**
     * Create a new instance of {@link NativeMongoSubscriptionModelConfig} with default settings.
     * It will by default use a {@link RetryStrategy} with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry.
     */
    public NativeMongoSubscriptionModelConfig() {
        this(RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false, null, null);
    }

    private NativeMongoSubscriptionModelConfig(RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost, @Nullable Integer batchSize, @Nullable Duration maxAwaitTime) {
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        if (batchSize != null && batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0 but was " + batchSize);
        }
        if (maxAwaitTime != null && maxAwaitTime.toMillis() <= 0) {
            throw new IllegalArgumentException("maxAwaitTime must be at least 1 ms but was " + maxAwaitTime);
        }
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
        this.batchSize = batchSize;
        this.maxAwaitTime = maxAwaitTime;
    }

    /**
     * Create a new {@code NativeMongoSubscriptionModelConfig} by using this static method instead of calling the {@link #NativeMongoSubscriptionModelConfig()} constructor.
     * Behaves the same as calling the constructor so this is just syntactic sugar.
     *
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public static NativeMongoSubscriptionModelConfig withConfig() {
        return new NativeMongoSubscriptionModelConfig();
    }

    /**
     * If there’s not enough history available in the MongoDB oplog to resume a subscription created from a {@code NativeMongoSubscriptionModel}, you can configure it to restart the subscription from the current time automatically.
     * This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It’s disabled by default since it might not be 100% safe
     * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store, and you have lots of writes happening to the event store.
     * It’s safe if you run the subscription in the same process as the writes to the event store if you make sure that the subscription is started before you accept writes to the event store on startup. To enable automatic restart, you can do like this:
     *
     * <pre>
     * var subscriptionModel = new NativeMongoSubscriptionModel(database, "events", TimeRepresentation.RFC_3339_STRING, executor, NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true));
     * </pre>
     *
     * @param restartSubscriptionsOnChangeStreamHistoryLost Whether or not to automatically restart a subscription, whose change stream history is lost.
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public NativeMongoSubscriptionModelConfig restartSubscriptionsOnChangeStreamHistoryLost(boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, batchSize, maxAwaitTime);
    }

    /**
     * Specify the retry strategy to use. This is used both when retrying the action supplied to a subscription if it throws an exception,
     * and to back off between attempts when the subscription model automatically restarts a subscription after a change stream error.
     *
     * @param retryStrategy A custom retry strategy to use
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public NativeMongoSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, batchSize, maxAwaitTime);
    }

    /**
     * Configure the number of change-stream documents the server returns per batch (maps to the underlying
     * {@link com.mongodb.client.ChangeStreamIterable#batchSize(int)}). A larger batch size reduces the number
     * of round-trips to the server and can improve throughput for high-volume subscriptions such as an outbox,
     * at the cost of a larger per-batch memory footprint.
     * <p>
     * If not configured, the MongoDB driver/server default is used (this is the behavior prior to this option
     * existing). As a rule of thumb, values in the low hundreds (e.g. {@code 500}) work well for high-throughput
     * scenarios, but the optimal value depends on your event size and load.
     *
     * @param batchSize The number of documents per batch. Must be greater than {@code 0}.
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public NativeMongoSubscriptionModelConfig batchSize(int batchSize) {
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, batchSize, maxAwaitTime);
    }

    /**
     * Configure the maximum amount of time the server waits for new change-stream documents before returning an
     * (possibly empty) batch (maps to the underlying {@link com.mongodb.client.ChangeStreamIterable#maxAwaitTime(long, java.util.concurrent.TimeUnit)}).
     * A smaller value lowers delivery latency at the cost of more frequent {@code getMore} round-trips when the
     * stream is idle; a larger value keeps an idle cursor waiting longer and reduces chatter.
     * <p>
     * If not configured, the MongoDB driver/server default is used (this is the behavior prior to this option
     * existing). Values in the range 200 ms&ndash;1000 ms strike a reasonable balance between latency and resource
     * usage for most workloads.
     *
     * @param maxAwaitTime The maximum wait time. Must be greater than {@code 0}.
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public NativeMongoSubscriptionModelConfig maxAwaitTime(Duration maxAwaitTime) {
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, batchSize, requireNonNull(maxAwaitTime, "maxAwaitTime cannot be null"));
    }
}
