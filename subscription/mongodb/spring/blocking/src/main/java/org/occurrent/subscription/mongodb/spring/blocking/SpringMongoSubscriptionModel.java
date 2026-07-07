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
import com.mongodb.MongoQueryException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.internal.ExecutorShutdown.shutdownSafely;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalCheckpointErrorMessage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This Subscription doesn't maintain the checkpoint, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 */
@NullMarked
public class SpringMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    // Shared executor for restart loops so a persistently failing subscription backs off (via retryStrategy)
    // instead of spawning a new thread for every single restart attempt. One virtual thread is occupied per
    // subscription that is currently restarting, not per attempt, and it's released as soon as the subscription
    // recovers, is paused/cancelled, or the model is shut down. Virtual threads are used (rather than a cached
    // platform-thread pool) because "restartOnce" blocks on "failureSignal.join()" for the entire time the
    // subscription is restarting, which can be a long time for a persistently failing subscription. A blocked
    // virtual thread unmounts from its carrier while parked, so this doesn't pin a platform thread for that
    // duration the way a cached thread pool would.
    private final ExecutorService restartExecutor;
    // Tracks the in-flight "wait for the next failure (or a stop signal)" future for a subscription that is
    // currently being restarted, so pauseSubscription/cancelSubscription/shutdown can wake up a blocked restart
    // loop instead of leaving its thread parked forever.
    private final ConcurrentMap<String, CompletableFuture<@Nullable RestartSignal>> activeRestartSignal;

    private volatile boolean shutdown = false;

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
        requireNonNull(mongoTemplate, MongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(config, SpringMongoSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
        this.mongoOperations = mongoTemplate;
        this.timeRepresentation = config.timeRepresentation;
        this.eventCollection = config.eventCollection;
        this.runningSubscriptions = new ConcurrentHashMap<>();
        this.pausedSubscriptions = new ConcurrentHashMap<>();
        this.retryStrategy = config.retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = config.restartSubscriptionsOnChangeStreamHistoryLost;
        this.restartExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("spring-mongo-subscription-restart-", 0).factory());
        this.activeRestartSignal = new ConcurrentHashMap<>();
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate, config.executor);
        this.messageListenerContainer.start();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, "StartAt cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }

        logDebug("Subscribing ({})", subscriptionId);

        // We wrap the creation of ChangeStreamRequestOptions in a supplier since otherwise the "startAtSupplier"
        // would be supplied only once, here, during initialization. When using a supplier here, the "startAtSupplier"
        // is called again when pausing and resuming a subscription. Take the case when a subscription is started with "StartAt.now()".
        // If we hadn't used a supplier and a subscription is paused and later resumed, it'll be resumed from the _initial_ "StartAt.now()" position,
        // and not the position the "StartAt.now()" position of when the subscription was resumed. This will lead to historic events being
        // replayed which is (most likely) not what the user expects.
        Function<@Nullable StartAt, ChangeStreamRequestOptions> requestOptionsFunction = overridingStartAt -> {
            var subscriptionModelContext = new StartAt.SubscriptionModelContext(SpringMongoSubscriptionModel.class);
            // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, overridingStartAt == null ? startAt : overridingStartAt, subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            return new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        };

        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            if (raw == null) {
                log.error("[{}] Internal Error: ChangeStreamDocument in collection {} was null", subscriptionId, eventCollection);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Received event with for operation {} in namespace {}. Document: {}, Document Key: {}, Update Description: {}", subscriptionId, raw.getOperationTypeString(), raw.getNamespace(), raw.getFullDocument(), raw.getDocumentKey(), raw.getUpdateDescription());
            }

            BsonDocument resumeToken = raw.getResumeToken();
            MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                    .map(cloudEvent -> new CheckpointAwareCloudEvent(cloudEvent, new MongoResumeTokenCheckpoint(resumeToken)))
                    .ifPresentOrElse(executeWithRetry(action, __ -> !shutdown, retryStrategy), () -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Won't deserialize document to cloud event for operation type {} in namespace {}: {}", raw.getOperationTypeString(), raw.getNamespace(), raw.getFullDocument());
                        }
                    });
        };

        Function<@Nullable StartAt, ChangeStreamRequest<Document>> requestBuilder = sa -> new ChangeStreamRequest<>(listener, requestOptionsFunction.apply(sa));
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = registerNewSpringSubscription(subscriptionId, requestBuilder.apply(null), null);
        SpringMongoSubscription springMongoSubscription = new SpringMongoSubscription(subscriptionId, subscription);
        logDebug("MessageListenerContainer running (subscriptionId={}): {}", subscriptionId, messageListenerContainer.isRunning());
        if (messageListenerContainer.isRunning()) {
            runningSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, requestBuilder));
        } else {
            pausedSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, requestBuilder));
        }
        return springMongoSubscription;
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        logDebug("Cancelling subscription for {}", subscriptionId);
        InternalSubscription subscription = runningSubscriptions.remove(subscriptionId);
        if (subscription == null) {
            logDebug("Subscription {} not found when cancelling", subscriptionId);
        } else {
            stopRestartLoop(subscriptionId);
            messageListenerContainer.remove(subscription.getSpringSubscription());
        }
    }

    @PreDestroy
    @Override
    public synchronized void shutdown() {
        logDebug("Shutting down subscription model");
        shutdown = true;
        runningSubscriptions.forEach((subscriptionId, internalSubscription) -> {
            stopRestartLoop(subscriptionId);
            internalSubscription.shutdown();
        });
        runningSubscriptions.clear();
        pausedSubscriptions.forEach((__, internalSubscription) -> internalSubscription.shutdown());
        pausedSubscriptions.clear();
        stopMessageListenerContainer();
        shutdownSafely(restartExecutor, 5, TimeUnit.SECONDS);
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        // Note that we increase the "increment" by 1 in order to not clash with an existing event in the event store.
        // This is so that we can avoid duplicates in certain rare cases when replaying events.
        BsonTimestamp currentOperationTime;
        try {
            currentOperationTime = MongoCommons.getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)), 1);
        } catch (UncategorizedMongoDbException e) {
            if (e.getCause() instanceof MongoCommandException) {
                log.warn(cannotFindGlobalCheckpointErrorMessage(e.getCause()));
                // This can if the server doesn't allow to get the operation time since "db.adminCommand( { "hostInfo" : 1 } )" is prohibited.
                // This is the case on for example shared Atlas clusters. If this happens we return the current time of the client instead.
                return null;
            } else {
                throw e;
            }
        }
        return new MongoOperationTimeCheckpoint(currentOperationTime);
    }

    // Life-cycle implementation

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        logDebug("Pausing subscription for {}", subscriptionId);
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't running.");
        }
        stopRestartLoop(subscriptionId);
        messageListenerContainer.remove(internalSubscription.getSpringSubscription());
        pausedSubscriptions.put(subscriptionId, internalSubscription);
        logDebug("Subscription {} paused", subscriptionId);
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        logDebug("Resuming subscription for {}", subscriptionId);
        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }

        if (!messageListenerContainer.isRunning()) {
            logDebug("Subscription was not running, will start (subscriptionId={})", subscriptionId);
            messageListenerContainer.start();
        }

        org.springframework.data.mongodb.core.messaging.Subscription newSubscription = registerNewSpringSubscription(subscriptionId, internalSubscription.newChangeStreamRequest(), null);
        InternalSubscription newInternalSubscription = internalSubscription.copy(newSubscription);
        runningSubscriptions.put(subscriptionId, newInternalSubscription);
        logDebug("Subscription {} resumed", subscriptionId);
        return new SpringMongoSubscription(subscriptionId, newSubscription);
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    // SmartLifecycle

    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        logDebug("Starting subscription model (resumeSubscriptionsAutomatically={}, shutdown={})", resumeSubscriptionsAutomatically, shutdown);
        if (!shutdown) {
            messageListenerContainer.start();
            if (resumeSubscriptionsAutomatically) {
                pausedSubscriptions.forEach((subscriptionId, __) -> resumeSubscription(subscriptionId).waitUntilStarted());
            }
        }
    }

    @Override
    public synchronized void stop() {
        logDebug("Stopping subscription model (shutdown={})", shutdown);
        if (!shutdown) {
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
            stopMessageListenerContainer();
        }
    }

    @Override
    public void start() {
        start(true);
    }

    @Override
    public boolean isRunning() {
        return !shutdown && messageListenerContainer.isRunning();
    }

    @Override
    public boolean isAutoStartup() {
        return messageListenerContainer.isAutoStartup();
    }

    private void stopMessageListenerContainer() {
        logDebug("Stopping MessageListenerContainer");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        messageListenerContainer.stop(countDownLatch::countDown);
        try {
            boolean success = countDownLatch.await(10, TimeUnit.SECONDS);
            if (!success) {
                log.warn("Failed to stop {} after 10 seconds", SpringMongoSubscriptionModel.class.getSimpleName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private org.springframework.data.mongodb.core.messaging.Subscription registerNewSpringSubscription(String subscriptionId, ChangeStreamRequest<Document> documentChangeStreamRequest, @Nullable CompletableFuture<@Nullable RestartSignal> failureSignal) {
        logDebug("registerNewSpringSubscription for subscription {}", subscriptionId);
        return messageListenerContainer.register(documentChangeStreamRequest, Document.class, throwable -> {
            if (throwable instanceof DataAccessException) {
                Throwable cause = throwable.getCause();
                if (cause instanceof MongoQueryException) {
                    log.warn("Caught {} ({}) for subscription {}, will restart!", MongoQueryException.class.getSimpleName(), cause.getMessage(), subscriptionId, throwable);
                    reportFailure(subscriptionId, failureSignal, new RestartSignal(StartAt.subscriptionModelDefault(), throwable));
                } else if (cause instanceof MongoCommandException && ((MongoCommandException) cause).getErrorCode() == CHANGE_STREAM_HISTORY_LOST_ERROR_CODE) {
                    String restartMessage = restartSubscriptionsOnChangeStreamHistoryLost ? "will restart subscription from current time." :
                            "will not restart subscription! Consider removing the subscription from the durable storage or use a catch-up subscription to get up to speed if needed.";
                    if (restartSubscriptionsOnChangeStreamHistoryLost) {
                        log.warn("There was not enough oplog to resume subscription {}, {}", subscriptionId, restartMessage, throwable);
                        reportFailure(subscriptionId, failureSignal, new RestartSignal(StartAt.now(), throwable));
                    } else {
                        log.error("There was not enough oplog to resume subscription {}, {}", subscriptionId, restartMessage, throwable);
                        reportFailure(subscriptionId, failureSignal, null);
                    }
                } else if (shutdown) {
                    if (log.isDebugEnabled()) {
                        log.debug("Subscription {} is shutting down, ignoring {}.", subscriptionId, throwable.getClass().getName(), throwable);
                    }
                    reportFailure(subscriptionId, failureSignal, null);
                } else {
                    log.error("Error caught for subscription {}: {} {}. Will restart!", subscriptionId, cause.getClass().getName(), cause.getMessage(), throwable);
                    reportFailure(subscriptionId, failureSignal, new RestartSignal(StartAt.subscriptionModelDefault(), throwable));
                }
            } else if (isCursorNoLongerOpen(throwable)) {
                if (log.isDebugEnabled()) {
                    log.debug("Cursor is no longer open for subscription {}, this may happen if you pause a subscription very soon after subscribing.", subscriptionId, throwable);
                }
                reportFailure(subscriptionId, failureSignal, null);
            } else {
                log.error("An error occurred for subscription {}, will restart", subscriptionId, throwable);
                reportFailure(subscriptionId, failureSignal, new RestartSignal(StartAt.subscriptionModelDefault(), throwable));
            }
        });
    }

    // Carries what a restart attempt should do next: reconnect at "startAt" because "cause" was the triggering
    // error. A completed future holding "null" instead of a RestartSignal means "stop restarting" (the
    // subscription was paused/cancelled/shut down, or history was lost and restarting is disabled).
    private record RestartSignal(StartAt startAt, Throwable cause) {
    }

    // Delivers the outcome of a change-stream error to whichever restart loop is currently responsible for this
    // subscription: if a restart loop is already waiting for the next failure, wake it up with the signal;
    // otherwise this is the first failure since subscribe/resume, so start a new restart loop on the shared
    // restart executor instead of spawning a dedicated thread for this one attempt.
    private void reportFailure(String subscriptionId, @Nullable CompletableFuture<@Nullable RestartSignal> failureSignal, @Nullable RestartSignal signal) {
        if (failureSignal != null) {
            failureSignal.complete(signal);
        } else if (signal != null) {
            restartExecutor.execute(() -> runRestartLoop(subscriptionId, signal));
        }
    }

    // Runs on the shared restart executor: retries restarting the subscription with the backoff configured by
    // "retryStrategy", the same strategy already used elsewhere in this class, instead of restarting immediately
    // and unconditionally like the previous thread-per-attempt implementation did. The loop ends (without
    // throwing) as soon as a restart attempt reports "no further restart needed".
    private void runRestartLoop(String subscriptionId, RestartSignal firstSignal) {
        AtomicReference<RestartSignal> next = new AtomicReference<>(firstSignal);
        try {
            executeWithRetry((Runnable) () -> {
                RestartSignal signal = requireNonNull(next.get());
                RestartSignal outcome = restartOnce(subscriptionId, signal);
                if (outcome == null) {
                    next.set(null);
                    return;
                }
                next.set(outcome);
                throw outcome.cause() instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(outcome.cause());
            }, __ -> !shutdown, retryStrategy).run();
        } catch (RuntimeException e) {
            // A backoff sleep that's interrupted (restart executor shut down) restores the thread's interrupt
            // status before rethrowing, see RetryExecution. Report that distinctly from genuine retry exhaustion,
            // and don't clear the interrupt status since the executor thread is being torn down.
            if (Thread.currentThread().isInterrupted()) {
                log.warn("Restart loop for subscription {} was interrupted, likely because the restart executor is shutting down", subscriptionId, e);
            } else if (shutdown) {
                logDebug("Stopped restarting subscription {} because the subscription model is shutting down", subscriptionId);
            } else {
                log.error("Giving up restarting subscription {}, retries exhausted", subscriptionId, e);
            }
        }
    }

    // Performs a single restart attempt, then blocks (without holding the model's lock) until either the
    // freshly (re)registered subscription fails again or the loop is told to stop. Returns the signal describing
    // the next failure so the caller can retry, or null if no further restart is needed.
    private @Nullable RestartSignal restartOnce(String subscriptionId, RestartSignal signal) {
        CompletableFuture<@Nullable RestartSignal> failureSignal = new CompletableFuture<>();
        synchronized (this) {
            InternalSubscription internalSubscription = runningSubscriptions.get(subscriptionId);
            if (internalSubscription == null || shutdown) {
                logDebug("Couldn't find a running subscription {} to restart, or model is shut down", subscriptionId);
                return null;
            }
            org.springframework.data.mongodb.core.messaging.Subscription oldSpringSubscription = internalSubscription.getSpringSubscription();
            ChangeStreamRequest<Document> newChangeStreamRequest = internalSubscription.newChangeStreamRequest(signal.startAt());
            activeRestartSignal.put(subscriptionId, failureSignal);
            org.springframework.data.mongodb.core.messaging.Subscription newSpringSubscription = registerNewSpringSubscription(subscriptionId, newChangeStreamRequest, failureSignal);
            internalSubscription.occurrentSubscription.changeSubscription(newSpringSubscription);
            messageListenerContainer.remove(oldSpringSubscription);
        }
        log.info("Subscription {} successfully restarted", subscriptionId);
        try {
            return failureSignal.join();
        } finally {
            activeRestartSignal.remove(subscriptionId, failureSignal);
        }
    }

    // Wakes up a restart loop that's currently blocked waiting for the next failure of "subscriptionId", if any,
    // so pausing/cancelling/shutting down doesn't leave a restart-executor thread parked forever.
    private void stopRestartLoop(String subscriptionId) {
        CompletableFuture<@Nullable RestartSignal> pending = activeRestartSignal.remove(subscriptionId);
        if (pending != null) {
            pending.complete(null);
        }
    }

    private static boolean isCursorNoLongerOpen(Throwable throwable) {
        return throwable instanceof IllegalStateException && throwable.getMessage().startsWith("Cursor") && throwable.getMessage().endsWith("is not longer open.");
    }

    // Model that hold both the spring subscription and the change stream request so that we can pause the subscription
    // (by removing it and starting it again)
    private record InternalSubscription(SpringMongoSubscription occurrentSubscription, Function<@Nullable StartAt, ChangeStreamRequest<Document>> changeStreamRequestBuilder) {

        InternalSubscription copy(org.springframework.data.mongodb.core.messaging.Subscription springSubscription) {
            return new InternalSubscription(new SpringMongoSubscription(occurrentSubscription.id(), springSubscription), changeStreamRequestBuilder);
        }

        ChangeStreamRequest<Document> newChangeStreamRequest() {
            return changeStreamRequestBuilder.apply(null);
        }

        ChangeStreamRequest<Document> newChangeStreamRequest(StartAt startAt) {
            return changeStreamRequestBuilder.apply(startAt);
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalSubscription that)) return false;
            return Objects.equals(occurrentSubscription, that.occurrentSubscription) && Objects.equals(changeStreamRequestBuilder, that.changeStreamRequestBuilder);
        }

        org.springframework.data.mongodb.core.messaging.Subscription getSpringSubscription() {
            return occurrentSubscription.getSubscriptionReference().get();
        }

        void shutdown() {
            occurrentSubscription.shutdown();
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", InternalSubscription.class.getSimpleName() + "[", "]")
                    .add("occurrentSubscription=" + occurrentSubscription)
                    .add("changeStreamRequestBuilder=" + changeStreamRequestBuilder)
                    .toString();
        }
    }

    private static void logDebug(String message, Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }


    @Override
    public boolean equals(@Nullable Object o) {
        if (!(o instanceof SpringMongoSubscriptionModel that)) return false;
        return restartSubscriptionsOnChangeStreamHistoryLost == that.restartSubscriptionsOnChangeStreamHistoryLost && shutdown == that.shutdown && Objects.equals(eventCollection, that.eventCollection) && Objects.equals(messageListenerContainer, that.messageListenerContainer) && Objects.equals(runningSubscriptions, that.runningSubscriptions) && Objects.equals(pausedSubscriptions, that.pausedSubscriptions) && timeRepresentation == that.timeRepresentation && Objects.equals(mongoOperations, that.mongoOperations) && Objects.equals(retryStrategy, that.retryStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollection, messageListenerContainer, runningSubscriptions, pausedSubscriptions, timeRepresentation, mongoOperations, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, shutdown);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SpringMongoSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("eventCollection='" + eventCollection + "'")
                .add("messageListenerContainer=" + messageListenerContainer)
                .add("runningSubscriptions=" + runningSubscriptions)
                .add("pausedSubscriptions=" + pausedSubscriptions)
                .add("timeRepresentation=" + timeRepresentation)
                .add("mongoOperations=" + mongoOperations)
                .add("retryStrategy=" + retryStrategy)
                .add("restartSubscriptionsOnChangeStreamHistoryLost=" + restartSubscriptionsOnChangeStreamHistoryLost)
                .add("shutdown=" + shutdown)
                .toString();
    }
}