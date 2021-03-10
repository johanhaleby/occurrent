package org.occurrent.subscription.mongodb.spring.blocking;

import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the {@code SpringSubscriptionModel}.
 */
public class SpringMongoSubscriptionModelConfig {

    final String eventCollection;
    final TimeRepresentation timeRepresentation;
    final RetryStrategy retryStrategy;
    final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    final Executor executor;

    /**
     * Create a new instance of {@link SpringMongoSubscriptionModelConfig} with the given settings.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the subscription position.
     *
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        this(eventCollection, timeRepresentation, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false, defaultExecutor());
    }

    private SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost,
                                               Executor executor) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        requireNonNull(executor, Executor.class.getSimpleName() + " cannot be null");
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
        this.executor = executor;
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
     * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store and you have lot’s of writes happening to the event store.
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
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor);
    }

    /**
     * Specify the retry strategy to use.
     *
     * @param retryStrategy A custom retry strategy to use if the {@code action} supplied to the subscription throws an exception
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor);
    }

    /**
     * Specify the executor to use for this subscription model. Under the hood the {@link SpringMongoSubscriptionModel} will use this executor when initializing the {@link DefaultMessageListenerContainer}
     * to listen to events written MongoDB. By default a {@link ThreadPoolTaskExecutor} will be used with queue size {@code 0}, which effectively will make behave as unbounded {@link Executors#newCachedThreadPool()}.
     *
     * Note that if you're using a non-spring implementation, for example an {@link ExecutorService}, you need to shut it down your self after {@link SpringMongoSubscriptionModel} is shutdown.
     *
     * @param executor The executor to use
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     * @see ThreadPoolTaskExecutor
     */
    public SpringMongoSubscriptionModelConfig executor(Executor executor) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor);
    }

    private static Executor defaultExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setQueueCapacity(0);
        executor.initialize();
        return executor;
    }
}