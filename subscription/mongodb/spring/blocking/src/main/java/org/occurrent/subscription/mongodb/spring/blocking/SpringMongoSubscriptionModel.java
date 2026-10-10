/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.blocking.changestream.internal.ChangeStreamSubscriptions;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalCheckpointErrorMessage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * This is a subscription that uses Spring's {@link MongoTemplate} to listen to changes from an event store. It reads
 * each subscription's change stream on a thread of its own from the configured executor.
 * This Subscription doesn't maintain the checkpoint, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 */
@NullMarked
public class SpringMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions, RepositionableSubscriptions, HistoryRetainingSubscriptions, HistoryLossReportingSubscriptions, QuietPositionReportingSubscriptions, SmartLifecycle {

    /**
     * Acknowledging costs nothing here. This model reads the event store's own change stream, so returning normally
     * advances a checkpoint and removes no event, and the store keeps its events on its own terms. An event erased
     * through {@code EventStoreOperations} is gone by that erasure rather than by the acknowledgement, so answering
     * {@code false} for it would only strand an instance on an event nobody can supply.
     */
    @Override
    public boolean retains(CloudEvent event) {
        return true;
    }

    @Override
    public boolean retainsEveryEvent() {
        return true;
    }

    private static final Logger log = LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);

    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final MongoTemplate mongoTemplate;
    final Executor executor;
    // Whether this model made the executor itself, and so shuts it down
    private final boolean ownsExecutor;
    private final boolean autoStartup;
    private final ChangeStreamSubscriptions subscriptions;

    /**
     * Create a blocking subscription using Spring. It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively
     * go up to max 2 seconds wait time between each retry when reading/saving/deleting the checkpoint.
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringMongoSubscriptionModel(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation) {
        this(mongoTemplate, withConfig(eventCollection, timeRepresentation));
    }

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param retryStrategy      A custom retry strategy to use if the {@code action} supplied to the subscription throws an exception
     */
    public SpringMongoSubscriptionModel(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy) {
        this(mongoTemplate, withConfig(eventCollection, timeRepresentation).retryStrategy(retryStrategy));
    }

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongoTemplate The mongo template to use
     * @param config        The configuration to use
     */
    public SpringMongoSubscriptionModel(MongoTemplate mongoTemplate, SpringMongoSubscriptionModelConfig config) {
        requireNonNull(mongoTemplate, MongoTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(config, SpringMongoSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
        this.mongoTemplate = mongoTemplate;
        this.timeRepresentation = config.timeRepresentation;
        this.eventCollection = config.eventCollection;
        this.autoStartup = config.autoStartup;
        this.ownsExecutor = config.executor == null;
        this.executor = config.executor == null ? newTaskExecutor(config.virtualThreads) : config.executor;
        // Left stopped when autoStartup is false, so subscribe(..) holds every subscription paused and no change
        // stream is opened until the caller starts one
        this.subscriptions = new ChangeStreamSubscriptions(this, new ChangeStreams(), SpringMongoSubscriptionModel.class, log, config.timeRepresentation, config.retryStrategy,
                config.restartSubscriptionsOnChangeStreamHistoryLost, null, config.maxAwaitTime, config.autoStartup);
    }

    // False for an executor it can't ask, and for a Spring executor not yet initialized, which isn't shut down
    static boolean isShutDown(Executor executor) {
        try {
            return switch (executor) {
                case ExecutorService executorService -> executorService.isShutdown();
                case ThreadPoolTaskExecutor taskExecutor -> taskExecutor.getThreadPoolExecutor().isShutdown();
                case ThreadPoolTaskScheduler taskScheduler -> taskScheduler.getScheduledExecutor().isShutdown();
                case SimpleAsyncTaskExecutor taskExecutor -> !taskExecutor.isActive();
                case ConcurrentTaskExecutor taskExecutor -> isShutDown(taskExecutor.getConcurrentExecutor());
                default -> false;
            };
        } catch (IllegalStateException notInitialized) {
            return false;
        }
    }

    private static ThreadPoolTaskExecutor newTaskExecutor(boolean virtualThreads) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setQueueCapacity(0);
        executor.setVirtualThreads(virtualThreads);
        executor.initialize();
        return executor;
    }

    // What the subscriptions ask of this model. The three calls at the end go to this model's own methods, so a
    // subclass that overrides one also sees the calls start(..), stop() and shutdown() make.
    private final class ChangeStreams implements ChangeStreamSubscriptions.Model {
        @Override
        public ChangeStreamIterable<Document> watch(List<Bson> pipeline) {
            MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(eventCollection);
            return pipeline.isEmpty() ? collection.watch(Document.class) : collection.watch(pipeline, Document.class);
        }

        @Override
        public Document runCommand(Document command) {
            return mongoTemplate.executeCommand(command);
        }

        @Override
        public void execute(Runnable task) {
            executor.execute(() -> {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    // A refused checkpoint write or a retry strategy that gave up, both logged where delivery stopped.
                    // This model has never thrown either on the executor
                    log.debug("Delivery stopped for a subscription", e);
                }
            });
        }

        @Override
        public boolean executorIsShutDown() {
            return isShutDown(executor);
        }

        @Override
        public void shutdownExecutor() {
            // Five seconds for a running action to return, as in the native model, since the executor's own shutdown
            // interrupts it
            if (ownsExecutor) {
                ExecutorShutdown.shutdownSafely(((ThreadPoolTaskExecutor) executor).getThreadPoolExecutor(), 5, TimeUnit.SECONDS);
            }
        }

        @Override
        public Subscription subscription(String subscriptionId, CountDownLatch started) {
            return new SpringMongoSubscription(subscriptionId, started, () -> subscriptions.isShutdown());
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return SpringMongoSubscriptionModel.this.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            SpringMongoSubscriptionModel.this.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            SpringMongoSubscriptionModel.this.cancelSubscription(subscriptionId);
        }
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscriptions.subscribe(subscriptionId, () -> pipelineFor(filter), startAt, action, false);
    }

    /**
     * Holds the subscription paused as a subscription made while this model is stopped, so its change stream opens on
     * {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)}. A subscription started at the present
     * is delivered the events written from this call on.
     */
    @Override
    public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscriptions.subscribe(subscriptionId, () -> pipelineFor(filter), startAt, action, true);
    }

    private List<Bson> pipelineFor(@Nullable SubscriptionFilter filter) {
        return List.copyOf(ApplyFilterToChangeStreamOptionsBuilder.changeStreamPipeline(timeRepresentation, filter, Aggregation.DEFAULT_CONTEXT));
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        subscriptions.cancelSubscription(subscriptionId);
    }

    /**
     * Cancels every subscription and shuts down the executor this model made for itself. An executor given to
     * {@link SpringMongoSubscriptionModelConfig#executor(Executor)} is left running.
     */
    // Takes the monitor itself while it cancels, and waits for the executor without it
    @PreDestroy
    @Override
    public void shutdown() {
        subscriptions.shutdown();
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        // Increment by 1 to avoid clashing with an existing event, preventing duplicates in rare replay cases.
        BsonTimestamp currentOperationTime;
        try {
            currentOperationTime = MongoCommons.getServerOperationTime(mongoTemplate.executeCommand(new Document("hostInfo", 1)), 1);
        } catch (UncategorizedMongoDbException e) {
            if (e.getCause() instanceof MongoCommandException) {
                log.warn(cannotFindGlobalCheckpointErrorMessage(e.getCause()));
                // Happens when the server prohibits "hostInfo" (e.g. shared Atlas clusters). Null is the
                // contract's answer for a problem this model cannot resolve.
                return null;
            } else {
                throw e;
            }
        }
        return new MongoOperationTimeCheckpoint(currentOperationTime);
    }

    /**
     * Opens a change stream at {@code checkpoint} and closes it again. Answers {@code false} when MongoDB refuses to
     * open it because the oplog no longer reaches back to {@code checkpoint}.
     */
    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return MongoCommons.canResumeFrom(eventCollection, checkpoint, mongoTemplate::executeCommand);
    }

    // Life-cycle implementation

    /**
     * Pause an individual subscription. The change stream behind it is closed, but the position it has read to is
     * kept, so {@link #resumeSubscription(String)} continues from there and events written while it was paused are
     * delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        Runnable waitForTheRunningAction;
        synchronized (this) {
            requireNotShutdown(subscriptionId);
            waitForTheRunningAction = subscriptions.pauseSubscription(subscriptionId);
        }
        // Without the monitor, so a call for another subscription doesn't wait for the paused one's action
        waitForTheRunningAction.run();
    }

    /**
     * Resume a paused subscription from the change-stream position it had read to, so that nothing written while it
     * was paused is lost.
     * <p>
     * Delivery is <i>at least once</i> across a pause: an event whose handler had not finished when the subscription
     * was paused, and every event another consumer of the same subscription id handled in the meantime, is handed to
     * this handler again on resume. That is deliberate, since wasted work is the cheaper mistake, and it means
     * handlers must be idempotent. For a subscription started at the present, this model asks MongoDB for its
     * operation time on the executor once {@code subscribe(..)} has registered it, and records the answer as that
     * subscription's position before its change stream opens. A pause doesn't stop the question, so one paused before
     * it handled any event, even before MongoDB answered, resumes from that time, and the events written since are
     * delivered too, as long as the oplog still holds that time. When it no longer does, the resume gets the handling
     * that {@code restartSubscriptionsOnChangeStreamHistoryLost} configures. When MongoDB's reply has no operation
     * time, nothing is recorded and the resume opens at the present.
     * <p>
     * A subscription whose {@code RetryStrategy} gave up opening its change stream, or delivering an event, still counts
     * as running, and pausing it and then resuming it starts it again. The give-up is logged as an error. When the
     * strategy gives up asking MongoDB for its operation time, the give-up is logged, and it can keep a change stream
     * from opening the same way. Pausing and resuming the subscription after that starts it again. A subscription whose change stream
     * history was lost with {@code restartSubscriptionsOnChangeStreamHistoryLost} turned off is removed from this
     * model, so its id can be subscribed again.
     * <p>
     * That is what this call does on its own. A {@code DurableSubscriptionModel} wrapping this model calls
     * {@link #resumeSubscription(String, StartAt)} with a stored checkpoint instead whenever one exists, so a
     * subscription reached that way can resume somewhere else entirely, for example the position a competing
     * consumer's other node advanced to while this one held no lease.
     *
     * @see #pauseSubscription(String)
     * @see #resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        requireNotShutdown(subscriptionId);
        return subscriptions.resumeSubscription(subscriptionId, null);
    }

    /**
     * Resume a paused subscription at {@code startAt}, instead of the change-stream position it had read to. A
     * checkpoint the change stream can't open at, such as a catch-up's position ({@code GlobalCheckpoint}) or a time,
     * resumes from the position it had read to, as {@link #resumeSubscription(String)} does, since opening the change
     * stream at the present would skip what was written while the subscription was paused. So does a dynamic
     * {@code startAt} each time it resolves to such a checkpoint.
     *
     * @see RepositionableSubscriptions#resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        requireNonNull(startAt, "StartAt cannot be null");
        subscriptions.checkStartPosition(startAt);
        requireNotShutdown(subscriptionId);
        return subscriptions.resumeSubscription(subscriptionId, startAt);
    }

    // A model that is shut down has no subscriptions, so it answers as for an id it never had
    private void requireNotShutdown(String subscriptionId) {
        if (subscriptions.isShutdown()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    /**
     * Synchronized because a subscription moves between the two maps in two steps, so an unsynchronized reader can
     * land between them and miss an id that exists. It also keeps a caller from seeing the ids of a model that
     * {@link #shutdown()} has already flagged as shut down but not yet cleared.
     */
    @Override
    public synchronized Set<String> subscriptionIds() {
        return subscriptions.subscriptionIds();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return subscriptions.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return subscriptions.isPaused(subscriptionId);
    }

    // SmartLifecycle

    /**
     * Start the model and, when {@code resumeSubscriptionsAutomatically} is {@code true}, resume every paused
     * subscription through {@link #resumeSubscription(String)} and wait until the {@link Subscription} each call
     * returns has started. Pausing, cancelling and listing subscriptions keep working while it waits.
     * <p>
     * The wait for a subscription ends early when it's paused or cancelled, when the model shuts down, or when its
     * change stream won't open at all, because its {@code RetryStrategy} gave up or its change stream history was lost
     * with {@code restartSubscriptionsOnChangeStreamHistoryLost} turned off.
     * <p>
     * When a resume or a wait throws, every other subscription is still resumed and waited for, and then this method
     * throws the first exception with the others added as suppressed.
     * <p>
     * A subscription made while the model is stopped is paused until this call or a resume starts it. For one given
     * {@link StartAt#now()}, no position, or a {@code StartAt.dynamic(..)} position that resolves to one of those,
     * {@code subscribe(..)} asks MongoDB for its operation time on the executor and returns without waiting for the
     * answer. The subscription starts at that time, so its position is fixed when MongoDB answers, shortly after
     * {@code subscribe(..)} returns, and an event written before then isn't delivered to it. To be sure an event is
     * delivered, call this method and wait for the subscription's {@link Subscription#waitUntilStarted()} before
     * writing it. While MongoDB can't be reached, the question is retried with the model's {@code RetryStrategy}.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        subscriptions.start(resumeSubscriptionsAutomatically);
    }

    // Takes the monitor itself while it pauses, and waits for the running actions without it
    @Override
    public void stop() {
        subscriptions.stop();
    }

    @Override
    public void start() {
        start(true);
    }

    @Override
    public boolean isRunning() {
        return subscriptions.isRunning();
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    @Override
    public void addHistoryLossListener(HistoryLossListener listener) {
        subscriptions.addHistoryLossListener(listener);
    }

    @Override
    public void removeHistoryLossListener(HistoryLossListener listener) {
        subscriptions.removeHistoryLossListener(listener);
    }

    @Override
    public void addQuietPositionListener(QuietPositionListener listener) {
        subscriptions.addQuietPositionListener(listener);
    }

    @Override
    public void removeQuietPositionListener(QuietPositionListener listener) {
        subscriptions.removeQuietPositionListener(listener);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SpringMongoSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("eventCollection='" + eventCollection + "'")
                .add("timeRepresentation=" + timeRepresentation)
                .add("executor=" + executor)
                .add("autoStartup=" + autoStartup)
                .add(subscriptions.toString())
                .toString();
    }
}
