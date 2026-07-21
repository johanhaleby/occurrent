package org.occurrent.subscription.mongodb.spring.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the {@code SpringSubscriptionModel}.
 */
@NullMarked
public class SpringMongoSubscriptionModelConfig {

    final String eventCollection;
    final TimeRepresentation timeRepresentation;
    final RetryStrategy retryStrategy;
    final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    final Executor executor;
    final @Nullable Duration maxAwaitTime;

    /**
     * Create a new instance of {@link SpringMongoSubscriptionModelConfig} with the given settings.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the checkpoint.
     *
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        this(eventCollection, timeRepresentation, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false, defaultExecutor(), null);
    }

    private SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost,
                                               Executor executor, @Nullable Duration maxAwaitTime) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        requireNonNull(executor, Executor.class.getSimpleName() + " cannot be null");
        if (maxAwaitTime != null && (maxAwaitTime.isNegative() || maxAwaitTime.isZero())) {
            throw new IllegalArgumentException("maxAwaitTime must be greater than 0 but was " + maxAwaitTime);
        }
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
        this.executor = executor;
        this.maxAwaitTime = maxAwaitTime;
    }

    /**
     * Create a new SpringSubscriptionModelConfig by using this static method instead of calling the {@link #SpringMongoSubscriptionModelConfig(String, TimeRepresentation)} constructor.
     * Behaves the same as calling the constructor so this is just syntactic sugar.
     *
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public static SpringMongoSubscriptionModelConfig withConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation);
    }

    /**
     * If there’s not enough history available in the MongoDB oplog to resume a subscription created from a SpringMongoSubscriptionModel, you can configure it to restart the subscription from the current time automatically.
     * This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It’s disabled by default since it might not be 100% safe
     * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store, and you have lots of writes happening to the event store.
     * It’s safe if you run the subscription in the same process as the writes to the event store if you make sure that the subscription is started before you accept writes to the event store on startup. To enable automatic restart, you can do like this:
     *
     * <pre>
     * var subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, SpringSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));
     * </pre>
     *
     * @param restartSubscriptionsOnChangeStreamHistoryLost Whether or not to automatically restart a subscription, whose change stream history is lost.
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig restartSubscriptionsOnChangeStreamHistoryLost(boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, maxAwaitTime);
    }

    /**
     * Specify the retry strategy to use.
     *
     * @param retryStrategy A custom retry strategy to use if the {@code action} supplied to the subscription throws an exception
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, maxAwaitTime);
    }

    /**
     * Specify the executor to use for this subscription model. Under the hood the {@link SpringMongoSubscriptionModel} will use this executor when initializing the {@link DefaultMessageListenerContainer}
     * to listen to events written MongoDB. By default a {@link ThreadPoolTaskExecutor} will be used with queue size {@code 0}, which effectively will make behave as unbounded
     * {@link java.util.concurrent.Executors#newCachedThreadPool()}.
     * <br/><br/>
     * Note that if you're using a non-spring implementation, for example an {@link java.util.concurrent.ExecutorService}, you need to shut it down your self after
     * {@link SpringMongoSubscriptionModel} is shutdown.
     *
     * @param executor The executor to use
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     * @see ThreadPoolTaskExecutor
     */
    public SpringMongoSubscriptionModelConfig executor(Executor executor) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, maxAwaitTime);
    }

    /**
     * Configure the maximum amount of time the server waits for new change-stream documents before returning a
     * (possibly empty) batch. This maps to the {@code maxAwaitTime} of the underlying MongoDB change stream (set
     * on Spring Data's {@code ChangeStreamRequestOptions}). A smaller value lowers delivery latency at the cost of
     * more frequent {@code getMore} round-trips when the stream is idle; a larger value keeps an idle cursor
     * waiting longer and reduces chatter.
     * <p>
     * If not configured, the MongoDB driver/server default is used (this is the behavior prior to this option
     * existing). Values in the range 200 ms&ndash;1000 ms strike a reasonable balance between latency and resource
     * usage for most workloads.
     * <p>
     * Note that, unlike the {@code NativeMongoSubscriptionModel}, this model does <em>not</em> expose a
     * {@code batchSize} option. It reads the change stream through Spring Data's {@link DefaultMessageListenerContainer},
     * whose {@code ChangeStreamRequest}/{@code ChangeStreamRequestOptions} API does not carry a batch size (Spring's
     * {@code ChangeStreamTask} never applies one), so there is no supported way to set it on this path. Use the
     * {@code NativeMongoSubscriptionModel} if you need to tune the batch size.
     *
     * @param maxAwaitTime The maximum wait time. Must be greater than {@code 0}.
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig maxAwaitTime(Duration maxAwaitTime) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, requireNonNull(maxAwaitTime, "maxAwaitTime cannot be null"));
    }

    /**
     * Use virtual threads for blocking MongoDB change stream listener tasks while keeping Spring's
     * {@link ThreadPoolTaskExecutor} lifecycle semantics.
     *
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig useVirtualThreads() {
        return executor(virtualThreadExecutor());
    }

    private static Executor defaultExecutor() {
        return newTaskExecutor(false);
    }

    private static Executor virtualThreadExecutor() {
        return newTaskExecutor(true);
    }

    private static Executor newTaskExecutor(boolean virtualThreads) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setQueueCapacity(0);
        executor.setVirtualThreads(virtualThreads);
        executor.initialize();
        return executor;
    }
}
