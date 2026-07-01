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

    /**
     * Create a new instance of {@link NativeMongoSubscriptionModelConfig} with default settings.
     * It will by default use a {@link RetryStrategy} with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry.
     */
    public NativeMongoSubscriptionModelConfig() {
        this(RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false);
    }

    private NativeMongoSubscriptionModelConfig(RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
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
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost);
    }

    /**
     * Specify the retry strategy to use. This is used both when retrying the action supplied to a subscription if it throws an exception,
     * and to back off between attempts when the subscription model automatically restarts a subscription after a change stream error.
     *
     * @param retryStrategy A custom retry strategy to use
     * @return A new instance of {@code NativeMongoSubscriptionModelConfig}
     */
    public NativeMongoSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new NativeMongoSubscriptionModelConfig(retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost);
    }
}
