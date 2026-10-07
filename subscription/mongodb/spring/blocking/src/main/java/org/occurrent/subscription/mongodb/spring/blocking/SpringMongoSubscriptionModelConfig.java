package org.occurrent.subscription.mongodb.spring.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
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
    // Null when the model makes its own executor
    final @Nullable Executor executor;
    final boolean virtualThreads;
    final @Nullable Duration maxAwaitTime;
    final boolean autoStartup;

    /**
     * Create a new instance of {@link SpringMongoSubscriptionModelConfig} with the given settings.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the checkpoint.
     *
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        this(eventCollection, timeRepresentation, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false, null, false, null, true);
    }

    private SpringMongoSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost,
                                               @Nullable Executor executor, boolean virtualThreads, @Nullable Duration maxAwaitTime, boolean autoStartup) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        if (maxAwaitTime != null && maxAwaitTime.toMillis() <= 0) {
            throw new IllegalArgumentException("maxAwaitTime must be at least 1 ms but was " + maxAwaitTime);
        }
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
        this.executor = executor;
        this.virtualThreads = virtualThreads;
        this.maxAwaitTime = maxAwaitTime;
        this.autoStartup = autoStartup;
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
     * This matters whenever a subscription opens its change stream from a position the oplog no longer holds. That happens when an application is restarted with subscriptions configured to start from such a position,
     * and when a subscription is resumed, or its change stream is restarted, after it has been paused or disconnected for longer than the oplog keeps history.
     * It’s disabled by default since it might not be 100% safe
     * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store, and you have lots of writes happening to the event store.
     * It’s safe if you run the subscription in the same process as the writes to the event store if you make sure that the subscription is started before you accept writes to the event store on startup. To enable automatic restart, you can do like this:
     *
     * <pre>
     * var subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, SpringSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));
     * </pre>
     * <p>
     * The model asks MongoDB for the current time with {@code ping}, and tells it to every listener added through
     * {@code HistoryLossReportingSubscriptions} before the restart. A {@code DurableSubscriptionModel} that wraps this
     * model adds one, and stores that time as the subscription's checkpoint unless another node has written it with a
     * newer lease, so a process restart in between doesn't open the change stream at the lost position again. While
     * the reply to {@code ping} has no operation time, the model doesn't restart a subscription whose restart position
     * an added listener stores, as a {@code DurableSubscriptionModel} does for each subscription it stores checkpoints
     * for, and tries the restart again as its {@code RetryStrategy} says. It restarts every other
     * subscription from the current time all the same.
     *
     * @param restartSubscriptionsOnChangeStreamHistoryLost Whether or not to automatically restart a subscription, whose change stream history is lost.
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig restartSubscriptionsOnChangeStreamHistoryLost(boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, virtualThreads, maxAwaitTime, autoStartup);
    }

    /**
     * Specify the retry strategy to use.
     *
     * @param retryStrategy A custom retry strategy to use if the {@code action} supplied to the subscription throws an exception
     * @return A new instance of {@code SpringSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, virtualThreads, maxAwaitTime, autoStartup);
    }

    /**
     * Specify the executor to use for this subscription model. The {@link SpringMongoSubscriptionModel} reads the change stream of each subscription on a thread from this executor,
     * so it needs a thread per subscription. A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused subscription can stay
     * busy for up to {@link #maxAwaitTime(Duration)} after it returns, and for as long as an action still runs once the pause has stopped waiting for it. So the model needs a thread
     * for each running subscription, and one more for each closed run that is still reading or still running its action. Every pause and resume of a subscription, and every
     * stop and start of the model, can add such a run, so no fixed number of threads is always enough.
     * When an executor with a fixed number of threads has no thread free for a resume or a start, the subscription counts as running and is handed to the executor again, 100 ms
     * and then up to 2 seconds apart, until a thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the resume rather
     * than leaving the subscription paused. A subscribe the executor has no thread for throws. By default each model creates a {@link ThreadPoolTaskExecutor} with queue size
     * {@code 0}, which makes it behave as an unbounded
     * {@link java.util.concurrent.Executors#newCachedThreadPool()}, and shuts it down when the model is shut down.
     * <br/><br/>
     * An executor you pass here is yours, so you need to shut it down yourself after the {@link SpringMongoSubscriptionModel} is shut down.
     *
     * @param executor The executor to use
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     * @see ThreadPoolTaskExecutor
     */
    public SpringMongoSubscriptionModelConfig executor(Executor executor) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, requireNonNull(executor, Executor.class.getSimpleName() + " cannot be null"), false, maxAwaitTime, autoStartup);
    }

    /**
     * Configure the maximum amount of time the server waits for new change-stream documents before returning a
     * (possibly empty) batch. This maps to the {@code maxAwaitTime} of the underlying MongoDB change stream. A smaller value lowers delivery latency at the cost of
     * more frequent {@code getMore} round-trips when the stream is idle. A larger value keeps an idle cursor
     * waiting longer and reduces chatter.
     * <p>
     * If not configured, the MongoDB driver/server default is used (this is the behavior prior to this option
     * existing). Values between 200 ms and 1000 ms strike a reasonable balance between latency and resource
     * usage for most workloads.
     * <p>
     * Note that, unlike the {@code NativeMongoSubscriptionModel}, this model has no {@code batchSize} option. Use the
     * {@code NativeMongoSubscriptionModel} if you need to tune the batch size.
     *
     * @param maxAwaitTime The maximum wait time. Must be greater than {@code 0}.
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig maxAwaitTime(Duration maxAwaitTime) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, virtualThreads, requireNonNull(maxAwaitTime, "maxAwaitTime cannot be null"), autoStartup);
    }

    /**
     * Read the change streams on virtual threads. Each model creates a {@link ThreadPoolTaskExecutor} that uses
     * virtual threads, and shuts it down when the model is shut down. This replaces an executor set with
     * {@link #executor(Executor)}.
     *
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig useVirtualThreads() {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, null, true, maxAwaitTime, autoStartup);
    }

    /**
     * Whether the subscription model starts itself. When {@code false} it is created stopped, so a subscription
     * registered on it is paused from the outset and only runs once you call {@code start()} or
     * {@code resumeSubscription(id)}. Use this to bring subscriptions up under your own control, behind a leader
     * election or a health check, or in a test that wants to choose which subscriptions run.
     * <p>
     * This also decides what {@code isAutoStartup()} reports to Spring, so a model registered as a bean is not
     * started for you either. Defaults to {@code true}, which is the behaviour before this option existed.
     *
     * @param autoStartup Whether to start on creation.
     * @return A new instance of {@code SpringMongoSubscriptionModelConfig}
     */
    public SpringMongoSubscriptionModelConfig autoStartup(boolean autoStartup) {
        return new SpringMongoSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, executor, virtualThreads, maxAwaitTime, autoStartup);
    }
}
