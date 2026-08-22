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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.internal.ExecutorShutdown.shutdownSafely;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalCheckpointErrorMessage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This subscription model doesn't maintain the checkpoint, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 */
@NullMarked
public class SpringMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions, RepositionableSubscriptions, SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    private final boolean autoStartup;
    private final @Nullable Duration maxAwaitTime;
    // Shared executor for restart loops so a failing subscription backs off (via retryStrategy) instead of
    // spawning a thread per restart attempt. One virtual thread per currently-restarting subscription,
    // released on recovery, pause/cancel, or shutdown. Virtual threads matter because restartOnce() blocks
    // on failureSignal.join() for the whole restart duration, and a blocked virtual thread unmounts from
    // its carrier instead of pinning a platform thread.
    private final ExecutorService restartExecutor;
    // Tracks the in-flight "wait for next failure or stop signal" future for a restarting subscription, so
    // pause/cancel/shutdown can wake a blocked restart loop instead of leaving it parked forever.
    private final ConcurrentMap<String, CompletableFuture<@Nullable RestartSignal>> activeRestartSignal;

    private volatile boolean shutdown = false;

    // A refused checkpoint write must never be retried, on the per-event delivery below. The call site already
    // passes its own predicate, which RetryExecution combines with the strategy's own. The restart loop is
    // excluded separately, in registerNewSpringSubscription's error handler, since it never runs a retried
    // delivery action itself.
    private final Predicate<Throwable> RETRYABLE = e -> !shutdown && !(e instanceof CheckpointWriteConditionNotFulfilledException);

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
        this.maxAwaitTime = config.maxAwaitTime;
        this.restartExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("spring-mongo-subscription-restart-", 0).factory());
        this.activeRestartSignal = new ConcurrentHashMap<>();
        this.autoStartup = config.autoStartup;
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate, config.executor);
        // Left stopped when autoStartup is false, so subscribe(..) registers into pausedSubscriptions and no change
        // stream is opened until the caller starts one. Starting here and stopping again would open and close them.
        if (autoStartup) {
            this.messageListenerContainer.start();
        }
    }

    @Override
    public synchronized SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, "StartAt cannot be null");

        if (isKnown(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }

        logDebug("Subscribing ({})", subscriptionId);

        // Tracks the change-stream position this subscription has read to, seeded with the StartAt it was
        // created with. Every request rebuild (pause/resume, restart after an error) starts from here rather
        // than from the original StartAt, which for the default resolves to the present all over again and
        // drops everything written while the subscription was away (#522). Mirrors the currentStartAt of
        // NativeMongoSubscriptionModel and ReactorMongoSubscriptionModel.
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);

        // Wraps ChangeStreamRequestOptions creation in a supplier so it's recomputed on pause/resume, not just
        // once at subscribe time. Before anything has been read the tracked position is still the original
        // StartAt, so a subscription created with StartAt.now() and resumed before its first event starts at
        // the resume-time present rather than replaying from the initial now().
        Supplier<ChangeStreamRequestOptions> requestOptionsSupplier = () -> {
            var subscriptionModelContext = new StartAt.SubscriptionModelContext(SpringMongoSubscriptionModel.class);
            // builder::resumeAt maps to the driver's startAtOperationTime here rather than to a resume token,
            // and that includes an operation stamped at exactly the given time.
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, currentStartAt.get(), subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            return maxAwaitTime == null
                    ? new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions)
                    : new ChangeStreamRequestOptions(null, eventCollection, maxAwaitTime, changeStreamOptions);
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
                    .ifPresentOrElse(executeWithRetry(action, RETRYABLE, retryStrategy), () -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Won't deserialize document to cloud event for operation type {} in namespace {}: {}", raw.getOperationTypeString(), raw.getNamespace(), raw.getFullDocument());
                        }
                    });
            // Advanced after the action rather than before it, so a pause or a change-stream error between two
            // documents can at worst hand the one in flight over again, never skip it. Advanced for a document
            // that didn't deserialize into a CloudEvent too, since there is nothing left to deliver for it.
            currentStartAt.set(StartAt.checkpoint(new MongoResumeTokenCheckpoint(resumeToken)));
        };

        Supplier<ChangeStreamRequest<Document>> requestBuilder = () -> new ChangeStreamRequest<>(listener, requestOptionsSupplier.get());
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = registerNewSpringSubscription(subscriptionId, requestBuilder.get(), null);
        SpringMongoSubscription springMongoSubscription = new SpringMongoSubscription(subscriptionId, subscription);
        logDebug("MessageListenerContainer running (subscriptionId={}): {}", subscriptionId, messageListenerContainer.isRunning());
        if (messageListenerContainer.isRunning()) {
            runningSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, currentStartAt, requestBuilder));
        } else {
            pausedSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, currentStartAt, requestBuilder));
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
        // Increment by 1 to avoid clashing with an existing event, preventing duplicates in rare replay cases.
        BsonTimestamp currentOperationTime;
        try {
            currentOperationTime = MongoCommons.getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)), 1);
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

    // Life-cycle implementation

    /**
     * Pause an individual subscription. The change stream behind it is closed, but the position it has read to is
     * kept, so {@link #resumeSubscription(String)} continues from there and events written while it was paused are
     * delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        logDebug("Pausing subscription for {}", subscriptionId);
        requireKnown(subscriptionId);
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }
        stopRestartLoop(subscriptionId);
        messageListenerContainer.remove(internalSubscription.getSpringSubscription());
        pausedSubscriptions.put(subscriptionId, internalSubscription);
        logDebug("Subscription {} paused", subscriptionId);
    }

    /**
     * Resume a paused subscription from the change-stream position it had read to, so that nothing written while it
     * was paused is lost.
     * <p>
     * Delivery is <i>at least once</i> across a pause: an event whose handler had not finished when the subscription
     * was paused, and every event another consumer of the same subscription id handled in the meantime, is handed to
     * this handler again on resume. That is deliberate, since wasted work is the cheaper mistake, and it means
     * handlers must be idempotent. A subscription that had not received anything yet has no position to resume from
     * and starts at the present instead.
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
    public synchronized SubscriptionHandle resumeSubscription(String subscriptionId) {
        return doResumeSubscription(subscriptionId, null);
    }

    /**
     * Resume a paused subscription at {@code startAt}, instead of the change-stream position it had read to.
     *
     * @see RepositionableSubscriptions#resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized SubscriptionHandle resumeSubscription(String subscriptionId, StartAt startAt) {
        requireNonNull(startAt, "StartAt cannot be null");
        MongoCommons.checkStartPosition(startAt, new StartAt.SubscriptionModelContext(SpringMongoSubscriptionModel.class));
        return doResumeSubscription(subscriptionId, startAt);
    }

    // The shared resume path. repositionTo is the caller's explicit position from the two-arg overload, or null
    // from the one-arg overload, in which case currentStartAt is left untouched and the resume continues from
    // whatever it already holds.
    private SubscriptionHandle doResumeSubscription(String subscriptionId, @Nullable StartAt repositionTo) {
        logDebug("Resuming subscription for {}", subscriptionId);
        requireKnown(subscriptionId);
        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        if (repositionTo != null) {
            internalSubscription.currentStartAt().set(repositionTo);
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

    /**
     * Synchronized because a subscription moves between the two maps in two steps, so an unsynchronized reader can
     * land between them and miss an id that exists. It also keeps a caller from seeing the ids of a model that
     * {@link #shutdown()} has already flagged as shut down but not yet cleared.
     */
    @Override
    public synchronized Set<String> subscriptionIds() {
        return Stream.concat(runningSubscriptions.keySet().stream(), pausedSubscriptions.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    private boolean isKnown(String subscriptionId) {
        return runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId);
    }

    // Separates "no such subscription here" from "wrong state for this call", which a caller holding several models
    // needs in order to tell "keep looking" from "this is the owner and the answer is no".
    private void requireKnown(String subscriptionId) {
        if (!isKnown(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    // SmartLifecycle

    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        logDebug("Starting subscription model (resumeSubscriptionsAutomatically={}, shutdown={})", resumeSubscriptionsAutomatically, shutdown);
        if (!shutdown) {
            messageListenerContainer.start();
            if (resumeSubscriptionsAutomatically) {
                // Snapshot the keys before iterating: resumeSubscription moves each id out of pausedSubscriptions as it
                // goes, and forEach over a map that its own callback mutates can visit an entry that has already
                // moved, or miss one that has not. Mirrors the reactor twin.
                new ArrayList<>(pausedSubscriptions.keySet()).forEach(subscriptionId -> resumeSubscription(subscriptionId).waitUntilStarted());
            }
        }
    }

    @Override
    public synchronized void stop() {
        logDebug("Stopping subscription model (shutdown={})", shutdown);
        if (!shutdown) {
            // Snapshot the keys before iterating: pauseSubscription moves each id from runningSubscriptions to
            // pausedSubscriptions as it goes, and forEach over a map that its own callback mutates can visit an entry
            // that has already moved, or miss one that has not. Mirrors the reactor twin.
            new ArrayList<>(runningSubscriptions.keySet()).forEach(this::pauseSubscription);
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
        return autoStartup;
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
            if (throwable instanceof CheckpointWriteConditionNotFulfilledException) {
                // Stays known and pausable, unlike the history-lost branch below, since forgetting it here would let
                // the strategy pause a subscription this model no longer knows about. Logged at error level because
                // nothing else would say why the node went quiet. reportFailure with a null signal ends this
                // subscription's restart loop, or never starts one, instead of running it unbounded.
                log.error("Checkpoint write for subscription {} was refused: {}. This node's lease has moved to another one, so delivery stops here rather than retrying. The subscription stays known and running until the next lease refresh pauses it, and a resume redelivers the event once this node holds the lease again.", subscriptionId, throwable.getMessage(), throwable);
                reportFailure(subscriptionId, failureSignal, null);
            } else if (throwable instanceof DataAccessException) {
                Throwable cause = throwable.getCause();
                if (cause instanceof MongoQueryException) {
                    log.warn("Caught {} ({}) for subscription {}, will restart!", MongoQueryException.class.getSimpleName(), cause.getMessage(), subscriptionId, throwable);
                    reportFailure(subscriptionId, failureSignal, new RestartSignal(null, throwable));
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
                    reportFailure(subscriptionId, failureSignal, new RestartSignal(null, throwable));
                }
            } else if (isCursorNoLongerOpen(throwable)) {
                if (log.isDebugEnabled()) {
                    log.debug("Cursor is no longer open for subscription {}, this may happen if you pause a subscription very soon after subscribing.", subscriptionId, throwable);
                }
                reportFailure(subscriptionId, failureSignal, null);
            } else {
                log.error("An error occurred for subscription {}, will restart", subscriptionId, throwable);
                reportFailure(subscriptionId, failureSignal, new RestartSignal(null, throwable));
            }
        });
    }

    // Carries what a restart attempt should do next: reconnect because "cause" triggered it, from
    // "restartFrom" when that says where, and otherwise from the position the subscription has read to. A
    // completed future holding null instead means "stop restarting" (paused/cancelled/shut down, or history
    // lost with restarting disabled).
    private record RestartSignal(@Nullable StartAt restartFrom, Throwable cause) {
    }

    // Delivers a change-stream error to whichever restart loop is responsible for this subscription: wakes
    // an already-waiting loop, or starts a new one on the shared restart executor if this is the first
    // failure since subscribe/resume.
    private void reportFailure(String subscriptionId, @Nullable CompletableFuture<@Nullable RestartSignal> failureSignal, @Nullable RestartSignal signal) {
        if (failureSignal != null) {
            failureSignal.complete(signal);
        } else if (signal != null) {
            restartExecutor.execute(() -> runRestartLoop(subscriptionId, signal));
        }
    }

    // Runs on the shared restart executor, retrying with the backoff from "retryStrategy" instead of
    // restarting immediately and unconditionally. Ends without throwing once a restart attempt reports no
    // further restart needed.
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
            // An interrupted backoff sleep (executor shutting down) restores the thread's interrupt status
            // before rethrowing, see RetryExecution. Reported distinctly from retry exhaustion, and the
            // interrupt status isn't cleared since the executor thread is being torn down.
            if (Thread.currentThread().isInterrupted()) {
                log.warn("Restart loop for subscription {} was interrupted, likely because the restart executor is shutting down", subscriptionId, e);
            } else if (shutdown) {
                logDebug("Stopped restarting subscription {} because the subscription model is shutting down", subscriptionId);
            } else {
                log.error("Giving up restarting subscription {}, retries exhausted", subscriptionId, e);
            }
        }
    }

    // Performs one restart attempt, then blocks (without the model's lock) until the subscription fails
    // again or is told to stop. Returns the next failure signal for the caller to retry, or null if done.
    private @Nullable RestartSignal restartOnce(String subscriptionId, RestartSignal signal) {
        CompletableFuture<@Nullable RestartSignal> failureSignal = new CompletableFuture<>();
        synchronized (this) {
            InternalSubscription internalSubscription = runningSubscriptions.get(subscriptionId);
            if (internalSubscription == null || shutdown) {
                logDebug("Couldn't find a running subscription {} to restart, or model is shut down", subscriptionId);
                return null;
            }
            org.springframework.data.mongodb.core.messaging.Subscription oldSpringSubscription = internalSubscription.getSpringSubscription();
            StartAt restartFrom = signal.restartFrom();
            if (restartFrom != null) {
                // Only change stream history loss names a position, and it names the present because the
                // position this subscription had read to is no longer in the oplog. Every other error restarts
                // from that position, so a transient failure doesn't skip whatever committed while the
                // subscription was down.
                internalSubscription.currentStartAt().set(restartFrom);
            }
            ChangeStreamRequest<Document> newChangeStreamRequest = internalSubscription.newChangeStreamRequest();
            activeRestartSignal.put(subscriptionId, failureSignal);
            // Removed before the replacement is registered, not after. Both share the listener that advances the
            // tracked position, so a straggling delivery from the old cursor would move it back to an older token
            // and replay from there. Nothing is lost either way, since the replacement starts from the position
            // rather than from the present.
            messageListenerContainer.remove(oldSpringSubscription);
            org.springframework.data.mongodb.core.messaging.Subscription newSpringSubscription = registerNewSpringSubscription(subscriptionId, newChangeStreamRequest, failureSignal);
            internalSubscription.occurrentSubscription.changeSubscription(newSpringSubscription);
        }
        log.info("Subscription {} successfully restarted", subscriptionId);
        try {
            return failureSignal.join();
        } finally {
            activeRestartSignal.remove(subscriptionId, failureSignal);
        }
    }

    // Wakes a restart loop blocked on the next failure of "subscriptionId", if any, so pause/cancel/shutdown
    // doesn't leave a restart-executor thread parked forever.
    private void stopRestartLoop(String subscriptionId) {
        CompletableFuture<@Nullable RestartSignal> pending = activeRestartSignal.remove(subscriptionId);
        if (pending != null) {
            pending.complete(null);
        }
    }

    private static boolean isCursorNoLongerOpen(Throwable throwable) {
        return throwable instanceof IllegalStateException && throwable.getMessage().startsWith("Cursor") && throwable.getMessage().endsWith("is not longer open.");
    }

    // Holds the spring subscription, the position the subscription has read to, and the change stream request
    // builder that reads it, so a subscription can be paused (by removing it) and resumed (by starting a new
    // one from that position).
    private record InternalSubscription(SpringMongoSubscription occurrentSubscription, AtomicReference<StartAt> currentStartAt, Supplier<ChangeStreamRequest<Document>> changeStreamRequestBuilder) {

        // Keeps the same currentStartAt reference, so the resumed subscription continues from where the paused
        // one got to rather than from the StartAt it was created with.
        InternalSubscription copy(org.springframework.data.mongodb.core.messaging.Subscription springSubscription) {
            return new InternalSubscription(new SpringMongoSubscription(occurrentSubscription.id(), springSubscription), currentStartAt, changeStreamRequestBuilder);
        }

        ChangeStreamRequest<Document> newChangeStreamRequest() {
            return changeStreamRequestBuilder.get();
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalSubscription that)) return false;
            return Objects.equals(occurrentSubscription, that.occurrentSubscription) && Objects.equals(currentStartAt, that.currentStartAt) && Objects.equals(changeStreamRequestBuilder, that.changeStreamRequestBuilder);
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
                    .add("currentStartAt=" + currentStartAt.get())
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
        return restartSubscriptionsOnChangeStreamHistoryLost == that.restartSubscriptionsOnChangeStreamHistoryLost && autoStartup == that.autoStartup && shutdown == that.shutdown && Objects.equals(maxAwaitTime, that.maxAwaitTime) && Objects.equals(eventCollection, that.eventCollection) && Objects.equals(messageListenerContainer, that.messageListenerContainer) && Objects.equals(runningSubscriptions, that.runningSubscriptions) && Objects.equals(pausedSubscriptions, that.pausedSubscriptions) && timeRepresentation == that.timeRepresentation && Objects.equals(mongoOperations, that.mongoOperations) && Objects.equals(retryStrategy, that.retryStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollection, messageListenerContainer, runningSubscriptions, pausedSubscriptions, timeRepresentation, mongoOperations, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost, autoStartup, maxAwaitTime, shutdown);
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
                .add("autoStartup=" + autoStartup)
                .add("maxAwaitTime=" + maxAwaitTime)
                .add("shutdown=" + shutdown)
                .toString();
    }
}