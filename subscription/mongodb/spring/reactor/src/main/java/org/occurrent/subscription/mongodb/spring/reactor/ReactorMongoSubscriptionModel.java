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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.MongoCommandException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.*;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalCheckpointErrorMessage;

/**
 * This is a subscription that uses project reactor and Spring to listen to changes from an event store.
 * This Subscription doesn't maintain the checkpoint, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc. It produces a {@link CloudEvent} implementation of type {@link CheckpointAwareCloudEvent}
 * that includes the checkpoint. Use {@link CheckpointAwareCloudEvent#getCheckpointOrThrowIAE(CloudEvent)}
 * to get the checkpoint.
 * <p>
 * Survives the same class of MongoDB operational disruption {@code SpringMongoSubscriptionModel} does (replica-set
 * failovers, transient network errors, and, if configured to, change stream history loss): the underlying change
 * stream automatically resubscribes and resumes from the position of the last change-stream document read, so
 * recovery is gap-free rather than a replay or a skipped window. See {@link ReactorMongoSubscriptionModelConfig}.
 * <p>
 * Also supports named, lifecycle-managed subscriptions, which is what makes it a {@link SubscriptionModel} ({@link Subscribable}
 * plus {@link SubscriptionModelLifeCycle}): pause, resume, and cancel an individual subscription by id, in addition to
 * the plain {@link #subscribe(SubscriptionFilter, StartAt)} {@link Flux} primitive.
 */
@NullMarked
public class ReactorMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, IntrospectableSubscriptions {
    private static final Logger log = LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class);

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final ReactorMongoSubscriptionModelConfig config;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();

    private volatile boolean shutdown = false;
    private volatile boolean running = true;

    /**
     * Create a reactive subscription using Spring
     *
     * @param mongo              The {@link ReactiveMongoOperations} instance to use when reading events from the event store
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public ReactorMongoSubscriptionModel(ReactiveMongoOperations mongo, String eventCollection, TimeRepresentation timeRepresentation) {
        this(mongo, eventCollection, timeRepresentation, ReactorMongoSubscriptionModelConfig.withConfig());
    }

    /**
     * Create a reactive subscription using Spring
     *
     * @param mongo              The {@link ReactiveMongoOperations} instance to use when reading events from the event store
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param config             Configure how the subscription model should behave, for example retry backoff and how to handle change stream history lost errors.
     */
    public ReactorMongoSubscriptionModel(ReactiveMongoOperations mongo, String eventCollection, TimeRepresentation timeRepresentation, ReactorMongoSubscriptionModelConfig config) {
        this.mongo = requireNonNull(mongo, ReactiveMongoOperations.class.getSimpleName() + " cannot be null");
        this.eventCollection = requireNonNull(eventCollection, "Event collection cannot be null");
        this.timeRepresentation = requireNonNull(timeRepresentation, "Time representation cannot be null");
        this.config = requireNonNull(config, ReactorMongoSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        // currentStartAt tracks the last change-stream document read (even if it produced no delivered
        // CloudEvent), so a resubscribe from retryWhen resumes gap-free. Safe here since the caller consumes
        // the Flux directly. The buffered named-subscription path below advances only on action completion.
        // Flux.defer gives each subscriber its own tracked position.
        return Flux.defer(() -> {
            AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
            return resilientChangeStream(filter, currentStartAt, currentStartAt::set, null);
        });
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        if (shutdown) {
            throw new IllegalStateException("Cannot start subscription because the subscription model is shutdown.");
        }
        // Validates the filter now, so an unsupported one is refused to the caller instead of failing later inside the
        // deferred change-stream pipeline, where nobody is listening and the retry above it would re-throw it forever.
        // Same fix NativeMongoSubscriptionModel got (#524); the plain Flux subscribe(filter, startAt) stays lazy on
        // purpose, since a cold publisher delivers its failure to the subscriber. The result is discarded: the real
        // options are built per (re)subscribe with the tracked start position.
        ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, ChangeStreamOptions.builder());
        // And the start position, which was the other half of #524 and stayed lazy when the filter was fixed. A
        // checkpoint this model cannot parse failed inside the Flux.defer below, where shouldRestart sends it round the
        // unbounded retry forever: waitUntilStarted() never answers and isRunning(id) keeps saying yes. A dynamic
        // position is a no-op in there, for a reason checkStartPosition documents.
        MongoCommons.checkStartPosition(startAt, new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
        return startInternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action);
    }

    private Subscription startInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Function<CloudEvent, Mono<Void>> action) {
        if (!running) {
            // Model stopped: don't subscribe, so waitUntilStarted() doesn't complete for a subscription that
            // won't deliver anything until start(true)/resumeSubscription actually starts it.
            InternalSubscription internalSubscription = new InternalSubscription(Disposables.disposed(), currentStartAt, filter, action, Mono.never());
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            return new ReactorMongoSubscription(subscriptionId, internalSubscription.started);
        }
        Sinks.Empty<Void> startedSink = Sinks.empty();
        // Placeholder goes in before subscribing: a synchronously-failing subscribe (e.g. building the change
        // stream options throws) runs the error handler below before subscribe() returns, which would
        // otherwise remove an entry never put in.
        runningSubscriptions.put(subscriptionId, new InternalSubscription(Disposables.disposed(), currentStartAt, filter, action, startedSink.asMono()));
        // Eager per-document tracking (used by plain subscribe(...) above) isn't used here: concatMap(action)
        // can buffer several documents ahead of a slow action, and tracking eagerly would let a pause/cancel
        // or retry resume past a buffered document without ever handing it to action, losing it. Advancing
        // currentStartAt only once action() completes means retry/pause/cancel can at most redeliver the
        // in-flight event, never skip one.
        Disposable disposable = resilientChangeStream(filter, currentStartAt, __ -> {
                }, startedSink)
                // The action's own error is retried here, with the same backoff the change stream restarts with,
                // mirroring the blocking models' RetryStrategy around the handler (no attempt cap by default). Without
                // this the error passes retryWhen, which only guards the change stream above, and terminates the whole
                // subscription: one bad delivery would end it while isRunning(id) said otherwise. Mono.defer, because
                // a retry must re-invoke the action the way the blocking RetryStrategy re-calls the handler:
                // resubscribing whatever Mono the first call returned would replay that attempt's failure forever.
                .concatMap(cloudEvent -> Mono.defer(() -> action.apply(cloudEvent))
                        .retryWhen(unboundedBackoff()
                                .doBeforeRetry(retrySignal -> log.warn("Action for subscription {} failed, will retry (attempt {})", subscriptionId, retrySignal.totalRetries() + 1, retrySignal.failure())))
                        .doOnSuccess(unused -> currentStartAt.set(StartAt.checkpoint(CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(cloudEvent)))))
                .subscribe(unused -> {
                        }, throwable -> {
                            log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable);
                            // No-op if the sink already completed successfully, otherwise (e.g. building the
                            // change stream options threw) this keeps waitUntilStarted() from hanging forever.
                            startedSink.tryEmitError(throwable);
                            // A dead subscription must not count as running, or isRunning(id) would lie and the
                            // id couldn't be reused without an explicit cancelSubscription().
                            runningSubscriptions.remove(subscriptionId);
                        });
        InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, startedSink.asMono());
        if (runningSubscriptions.replace(subscriptionId, internalSubscription) == null) {
            // Placeholder already removed by a synchronous error above, so this subscription is dead,
            // dispose defensively to match what the error handler otherwise does.
            disposable.dispose();
        }
        return new ReactorMongoSubscription(subscriptionId, internalSubscription.started);
    }

    private Flux<CloudEvent> resilientChangeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Consumer<StartAt> onDocumentRead, Sinks.@Nullable Empty<Void> startedSink) {
        return changeStream(filter, currentStartAt, onDocumentRead, startedSink)
                .retryWhen(unboundedBackoff()
                        .filter(throwable -> shouldRestart(throwable, currentStartAt)));
    }

    // One spec for both retry sites, so the action retry cannot drift from the backoff the change stream restarts with.
    private RetryBackoffSpec unboundedBackoff() {
        return Retry.backoff(Long.MAX_VALUE, config.minBackoff).maxBackoff(config.maxBackoff);
    }

    private Flux<CloudEvent> changeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Consumer<StartAt> onDocumentRead, Sinks.@Nullable Empty<Void> startedSink) {
        return Flux.defer(() -> {
            SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(ReactorMongoSubscriptionModel.class);
            // builder::resumeAt maps to the driver's startAtOperationTime here rather than to a resume token,
            // and that includes an operation stamped at exactly the given time.
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, currentStartAt.get(), subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
            // "Started" only means the change stream Flux was subscribed to, not that the server acknowledged
            // the command and the cursor is positioned. Weaker than NativeMongoSubscriptionModel's latch,
            // which only fires after that round trip completes.
            if (startedSink != null) {
                changeStream = changeStream.doOnSubscribe(subscription -> startedSink.tryEmitEmpty());
            }
            return changeStream
                    .flatMap(changeEvent -> {
                        ChangeStreamDocument<Document> raw = changeEvent.getRaw();
                        if (raw == null) {
                            // Mirrors SpringMongoSubscriptionModel's defensive check. Not expected, but skipping
                            // this event beats an NPE that retries the whole subscription.
                            log.error("Internal error: ChangeStreamEvent for collection {} had a null raw document", eventCollection);
                            return Mono.empty();
                        }
                        MongoResumeTokenCheckpoint checkpoint = new MongoResumeTokenCheckpoint(requireNonNull(raw.getResumeToken()));
                        // Advances the tracked position for every document received, even ones that don't
                        // deserialize into a delivered CloudEvent, mirroring NativeMongoSubscriptionModel, so
                        // a resubscribe resumes gap-free.
                        onDocumentRead.accept(StartAt.checkpoint(checkpoint));
                        return MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                                .map(cloudEvent -> new CheckpointAwareCloudEvent(cloudEvent, checkpoint))
                                .map(Mono::just)
                                .orElse(Mono.empty());
                    });
        });
    }

    // ChangeStreamHistoryLost (286) restarts from StartAt.now() only when configured to. Everything else
    // (failover, transient network error, anything the driver itself couldn't resume) restarts from the
    // tracked position. Mirrors NativeMongoSubscriptionModel and SpringMongoSubscriptionModel.
    private boolean shouldRestart(Throwable throwable, AtomicReference<StartAt> currentStartAt) {
        if (isChangeStreamHistoryLost(throwable)) {
            if (config.restartSubscriptionsOnChangeStreamHistoryLost) {
                log.warn("There was not enough oplog to resume the subscription, will restart subscription from current time.", throwable);
                currentStartAt.set(StartAt.now());
                return true;
            } else {
                log.error("There was not enough oplog to resume the subscription, will not restart subscription! Consider removing the subscription from the durable storage or use a catch-up subscription to get up to speed if needed.", throwable);
                return false;
            }
        }
        log.warn("Error caught for change stream subscription: {} {}. Will restart!", throwable.getClass().getName(), throwable.getMessage(), throwable);
        return true;
    }

    private static boolean isChangeStreamHistoryLost(Throwable throwable) {
        Throwable cause = throwable instanceof UncategorizedMongoDbException ? throwable.getCause() : throwable;
        return cause instanceof MongoCommandException mongoCommandException && mongoCommandException.getErrorCode() == MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE;
    }

    /**
     * Completes empty when the server prohibits the {@code hostInfo} command, which is what a shared Atlas
     * cluster does. See {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} for what an empty completion
     * means to a caller.
     */
    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        // Increment by 1 so the resume position lands after the most recently written event, matching
        // SpringMongoSubscriptionModel, avoiding a replay.
        return mongo.executeCommand(new Document("hostInfo", 1))
                .map(document -> MongoCommons.getServerOperationTime(document, 1))
                .onErrorResume(UncategorizedMongoDbException.class, throwable -> {
                    if (throwable.getCause() instanceof MongoCommandException) {
                        // Happens when the server prohibits "hostInfo" (e.g. shared Atlas clusters), falls
                        // back to the client's current time.
                        log.warn(cannotFindGlobalCheckpointErrorMessage(throwable.getCause()));
                        return Mono.empty();
                    } else {
                        return Mono.error(throwable);
                    }
                })
                .map(MongoOperationTimeCheckpoint::new);
    }

    /**
     * Pause an individual subscription. The change stream behind it is disposed, but the position it has read to is
     * kept, so {@link #resumeSubscription(String)} continues from there and events written while it was paused are
     * delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorMongoSubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isPaused(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        } else if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
    }

    /**
     * Resume a paused subscription from the change-stream position it had read to, so that nothing written while it
     * was paused is lost.
     * <p>
     * Delivery is <i>at least once</i> across a pause: an event whose action's {@code Mono} had not completed when
     * the subscription was paused, and every event another consumer of the same subscription id handled in the
     * meantime, is handed to this action again on resume. That is deliberate, since wasted work is the cheaper
     * mistake, and it means actions must be idempotent. A subscription that had not received anything yet has no
     * position to resume from and starts at the present instead.
     *
     * @see #pauseSubscription(String)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorMongoSubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isRunning(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }

        running = true;
        // Reuses the same currentStartAt reference so resume continues from the last delivered event, not
        // the original StartAt.
        return startInternalSubscription(subscriptionId, internalSubscription.filter, internalSubscription.currentStartAt, internalSubscription.action);
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
        }
        pausedSubscriptions.remove(subscriptionId);
    }

    @PreDestroy
    @Override
    public synchronized void shutdown() {
        shutdown = true;
        running = false;
        runningSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        runningSubscriptions.clear();
        pausedSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        pausedSubscriptions.clear();
    }

    @Override
    public synchronized void stop() {
        if (!shutdown) {
            running = false;
            // Snapshot the keys before iterating: pauseSubscription moves each id from runningSubscriptions to
            // pausedSubscriptions as it goes, and forEach over a map that its own callback mutates can visit an
            // entry that has already moved, or miss one that has not. Mirrors ReactorDurableSubscriptionModel.
            new ArrayList<>(runningSubscriptions.keySet()).forEach(this::pauseSubscription);
        }
    }

    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        if (!shutdown) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // Same snapshot reasoning as stop(): resumeSubscription moves each id out of pausedSubscriptions as
                // it goes, so iterating the live map here would be exposed to the same hazard.
                new ArrayList<>(pausedSubscriptions.keySet()).forEach(this::resumeSubscription);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
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

    private static final class InternalSubscription {
        final Disposable disposable;
        final AtomicReference<StartAt> currentStartAt;
        final @Nullable SubscriptionFilter filter;
        final Function<CloudEvent, Mono<Void>> action;
        final Mono<Void> started;

        private InternalSubscription(Disposable disposable, AtomicReference<StartAt> currentStartAt, @Nullable SubscriptionFilter filter, Function<CloudEvent, Mono<Void>> action, Mono<Void> started) {
            this.disposable = disposable;
            this.currentStartAt = currentStartAt;
            this.filter = filter;
            this.action = action;
            this.started = started;
        }
    }
}