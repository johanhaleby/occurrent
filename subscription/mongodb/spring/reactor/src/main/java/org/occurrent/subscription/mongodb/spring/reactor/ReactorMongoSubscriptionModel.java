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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.ClientSession;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.*;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
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
 * The model reads the change stream itself, with the {@code aggregate} and {@code getMore} commands on a MongoDB
 * session of its own, so it sees the resume token MongoDB sends with a batch that has no event in it. A subscription
 * whose filter matches no event moves its position to that token, so a pause, a restart or a lease handover doesn't
 * resume from a position the oplog has dropped while the subscription was up to date. The model reports that
 * position through {@link QuietPositionReportingSubscriptions}.
 * <p>
 * After a replica-set failover, a transient network error and, if configured to, lost change stream history, the
 * model opens the change stream again with a backoff, from the position of the last change stream document the
 * subscription has handled. See {@link ReactorMongoSubscriptionModelConfig}.
 * <p>
 * Also supports named, lifecycle-managed subscriptions, which is what makes it a {@link SubscriptionModel} ({@link Subscribable}
 * plus {@link SubscriptionModelLifeCycle}): pause, resume, and cancel an individual subscription by id, in addition to
 * the plain {@link #subscribe(SubscriptionFilter, StartAt)} {@link Flux} primitive.
 */
@NullMarked
public class ReactorMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, IntrospectableSubscriptions, QuietPositionReportingSubscriptions {
    private static final Logger log = LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class);

    // MongoDB refuses a getMore from another session than the aggregate that opened the cursor, and a command without
    // a session of its own takes whichever session the driver's pool hands out. Nothing read on it depends on an
    // earlier write, so it needs no causal consistency.
    private static final ClientSessionOptions CHANGE_STREAM_SESSION = ClientSessionOptions.builder().causallyConsistent(false).build();

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final ReactorMongoSubscriptionModelConfig config;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();
    // The last run started for each subscription id, kept until it has ended, so the next run for the id starts after it
    private final ConcurrentMap<String, Run> lastRuns = new ConcurrentHashMap<>();
    private final List<QuietPositionListener> quietPositionListeners = new CopyOnWriteArrayList<>();

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
        // Flux.defer gives each subscriber its own tracked position. It follows the last change stream document handed
        // to the subscriber or skipped, and the token of the last read that returned none, so a restart below goes on
        // from there. A batch read ahead of the subscriber doesn't move it, since a restart drops that batch.
        return Flux.defer(() -> {
            AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
            return withChangeStreamCursor(cursor -> openingPosition(currentStartAt, currentStartAt::compareAndSet)
                    .flatMap(position -> cursor.open(position, filter))
                    .concatWith(Mono.defer(cursor::getMore).repeat()))
                    .concatMapIterable(this::readsIn, 1)
                    .<CloudEvent>handle((read, sink) -> {
                        currentStartAt.set(read.position());
                        if (read.cloudEvent() != null) {
                            sink.next(read.cloudEvent());
                        }
                    })
                    .retryWhen(unboundedBackoff().filter(throwable -> shouldRestart(null, throwable, () -> currentStartAt.set(StartAt.now()))));
        });
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        RunStart runStart;
        synchronized (this) {
            if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (shutdown) {
                throw new SubscriptionModelShutdownException();
            }
            // Validates the filter now, so an unsupported one is refused to the caller instead of failing later inside the
            // deferred change stream pipeline, where nobody is listening and the retry above it would re-throw it forever.
            // NativeMongoSubscriptionModel does the same. The plain Flux subscribe(filter, startAt) stays lazy on purpose,
            // since a cold publisher delivers its failure to the subscriber.
            ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, ChangeStreamOptions.builder());
            // And the start position, for the same reason. A checkpoint this model cannot parse would fail
            // inside the run, where shouldRestart sends it round the unbounded retry forever, so waitUntilStarted() never
            // answers and isRunning(id) keeps saying yes. A dynamic position is a no-op in there, for a reason
            // checkStartPosition documents.
            MongoCommons.checkStartPosition(startAt, new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
            InternalSubscription internalSubscription = new InternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action);
            if (!running) {
                // Model stopped: don't start it, so waitUntilStarted() doesn't complete for a subscription that won't
                // deliver anything until start(true) or resumeSubscription actually starts it.
                pausedSubscriptions.put(subscriptionId, internalSubscription);
                return new ReactorMongoSubscription(subscriptionId, Mono.never());
            }
            runStart = startRun(internalSubscription);
        }
        return launch(runStart);
    }

    // Holds the monitor. Registers the subscription as running before the run is subscribed to, so a run that fails
    // straight away removes an entry that is there.
    private RunStart startRun(InternalSubscription internalSubscription) {
        Run run = new Run();
        internalSubscription.run = run;
        runningSubscriptions.put(internalSubscription.subscriptionId, internalSubscription);
        Run previous = lastRuns.put(internalSubscription.subscriptionId, run);
        run.ended().doOnTerminate(() -> lastRuns.remove(internalSubscription.subscriptionId, run)).subscribe();
        return new RunStart(internalSubscription, run, previous == null ? Mono.empty() : previous.ended());
    }

    // Runs without the monitor, since subscribing resolves the start position, which can call the caller's function
    private Subscription launch(RunStart runStart) {
        InternalSubscription internalSubscription = runStart.internalSubscription();
        Run run = runStart.run();
        // The run reads nothing until the previous run for the id has ended, so its steps never overlap one of that run
        Disposable reads = runStart.previousRunEnded()
                .thenMany(Flux.defer(() -> reads(internalSubscription, run)))
                .subscribe(__ -> {
                        }, throwable -> runEnded(internalSubscription, run, throwable),
                        () -> runEnded(internalSubscription, run, new IllegalStateException("The change stream of subscription " + internalSubscription.subscriptionId + " completed")));
        run.readWith(reads);
        return new ReactorMongoSubscription(internalSubscription.subscriptionId, run.started());
    }

    private void runEnded(InternalSubscription internalSubscription, Run run, Throwable throwable) {
        log.error("Subscription {} terminated with an unrecoverable error", internalSubscription.subscriptionId, throwable);
        // No-op if the subscription had already started, otherwise this keeps waitUntilStarted() from hanging forever
        run.failedToStart(throwable);
        run.close();
        synchronized (this) {
            // A dead subscription must not count as running, or isRunning(id) would lie and the id couldn't be reused
            // without an explicit cancelSubscription(). Only while this run is still the subscription's, since a pause
            // and a resume, or a cancel and a new subscription with the same id, start a newer run.
            if (internalSubscription.run == run) {
                internalSubscription.run = null;
                runningSubscriptions.remove(internalSubscription.subscriptionId, internalSubscription);
            }
        }
    }

    private Flux<Void> reads(InternalSubscription internalSubscription, Run run) {
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        BiPredicate<StartAt, StartAt> recordOpeningPosition = (expected, pinned) -> run.compareAndMove(currentStartAt, expected, pinned);
        return withChangeStreamCursor(cursor -> openingPosition(currentStartAt, recordOpeningPosition)
                .flatMap(position -> quietPositionHandlers(internalSubscription.subscriptionId, run)
                        .flatMap(quietPositionHandlers -> cursor.open(position, internalSubscription.filter)
                                .doOnNext(__ -> run.opened())
                                .flatMap(batch -> run.step(() -> handle(internalSubscription, run, batch, quietPositionHandlers)))))
                .thenMany(Mono.defer(() -> quietPositionHandlers(internalSubscription.subscriptionId, run)
                                .flatMap(quietPositionHandlers -> cursor.getMore()
                                        .flatMap(batch -> run.step(() -> handle(internalSubscription, run, batch, quietPositionHandlers)))))
                        .repeat()))
                .retryWhen(unboundedBackoff().filter(throwable -> shouldRestart(internalSubscription.subscriptionId, throwable, () -> run.move(currentStartAt, StartAt.now()))));
    }

    // Asked before every read, inside a step, so a listener answers before it knows what the read returns
    private Mono<List<Function<Checkpoint, Mono<Void>>>> quietPositionHandlers(String subscriptionId, Run run) {
        if (quietPositionListeners.isEmpty()) {
            return Mono.just(List.of());
        }
        return run.step(() -> Flux.fromIterable(quietPositionListeners)
                .concatMap(listener -> listener.beforeReading(subscriptionId))
                .collectList());
    }

    // Runs inside a step. An empty batch moves the position to the token MongoDB sent with it and hands that position to
    // the quiet position handlers. Otherwise the action gets each event, and the position moves past an event once the
    // action's Mono for it has completed, so a pause, a cancel or a restart delivers again at most the event in hand.
    private Mono<Void> handle(InternalSubscription internalSubscription, Run run, Batch batch, List<Function<Checkpoint, Mono<Void>>> quietPositionHandlers) {
        if (batch.documents().isEmpty()) {
            BsonDocument postBatchResumeToken = batch.postBatchResumeToken();
            if (postBatchResumeToken == null) {
                return Mono.empty();
            }
            Checkpoint quietPosition = new MongoResumeTokenCheckpoint(postBatchResumeToken);
            run.move(internalSubscription.currentStartAt, StartAt.checkpoint(quietPosition));
            return Flux.fromIterable(quietPositionHandlers)
                    .concatMap(quietPositionHandler -> quietPositionHandler.apply(quietPosition))
                    .then();
        }
        return Flux.fromIterable(batch.documents())
                .concatMap(document -> deliver(internalSubscription, run, document))
                .then();
    }

    private Mono<Void> deliver(InternalSubscription internalSubscription, Run run, ChangeStreamDocument<Document> document) {
        MongoResumeTokenCheckpoint checkpoint = new MongoResumeTokenCheckpoint(requireNonNull(document.getResumeToken()));
        StartAt afterDocument = StartAt.checkpoint(checkpoint);
        Optional<CloudEvent> cloudEvent = MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(document, timeRepresentation);
        if (cloudEvent.isEmpty()) {
            run.move(internalSubscription.currentStartAt, afterDocument);
            return Mono.empty();
        }
        CloudEvent checkpointAwareCloudEvent = new CheckpointAwareCloudEvent(cloudEvent.get(), checkpoint);
        String subscriptionId = internalSubscription.subscriptionId;
        // The action's own error is retried here, with the same backoff the change stream restarts with, mirroring the
        // blocking models' RetryStrategy around the handler (no attempt cap by default). Mono.defer, because a retry must
        // call the action again the way the blocking RetryStrategy calls the handler again: subscribing again to whatever
        // Mono the first call returned would replay that attempt's failure forever. A pause cancels a pending retry.
        return Mono.defer(() -> internalSubscription.action.apply(checkpointAwareCloudEvent))
                .retryWhen(unboundedBackoff()
                        .doBeforeRetry(retrySignal -> log.warn("Action for subscription {} failed, will retry (attempt {})", subscriptionId, retrySignal.totalRetries() + 1, retrySignal.failure())))
                .then(Mono.fromRunnable(() -> run.move(internalSubscription.currentStartAt, afterDocument)));
    }

    // The plain subscribe(filter, startAt) moves its position for every read, the empty ones included
    private List<Read> readsIn(Batch batch) {
        if (batch.documents().isEmpty()) {
            BsonDocument postBatchResumeToken = batch.postBatchResumeToken();
            return postBatchResumeToken == null ? List.of() : List.of(new Read(StartAt.checkpoint(new MongoResumeTokenCheckpoint(postBatchResumeToken)), null));
        }
        List<Read> reads = new ArrayList<>(batch.documents().size());
        for (ChangeStreamDocument<Document> document : batch.documents()) {
            MongoResumeTokenCheckpoint checkpoint = new MongoResumeTokenCheckpoint(requireNonNull(document.getResumeToken()));
            CloudEvent cloudEvent = MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(document, timeRepresentation)
                    .map(deserialized -> (CloudEvent) new CheckpointAwareCloudEvent(deserialized, checkpoint))
                    .orElse(null);
            reads.add(new Read(StartAt.checkpoint(checkpoint), cloudEvent));
        }
        return reads;
    }

    // Opens a session for one opening of the change stream, and once the reads have ended kills the cursor on the
    // server and closes the session. Every command on the cursor goes through that session.
    private <T> Flux<T> withChangeStreamCursor(Function<ChangeStreamCursor, Publisher<T>> reads) {
        return Flux.defer(() -> {
            AtomicLong cursorId = new AtomicLong();
            return mongo.withSession(CHANGE_STREAM_SESSION)
                    .execute(operations -> reads.apply(new ChangeStreamCursor(operations, cursorId)), session -> killCursorAndClose(session, cursorId.get()));
        });
    }

    // An error is only logged, since MongoDB closes a cursor nobody reads after its idle timeout anyway
    private void killCursorAndClose(ClientSession session, long cursorId) {
        if (cursorId == 0) {
            session.close();
            return;
        }
        mongo.withSession(session).executeCommand(new Document("killCursors", eventCollection).append("cursors", List.of(cursorId)))
                .doFinally(__ -> session.close())
                .subscribe(__ -> {
                }, throwable -> log.debug("Failed to kill change stream cursor {} on collection {}, MongoDB closes it after its idle timeout.", cursorId, eventCollection, throwable));
    }

    // One spec for both retry sites, so the action retry cannot drift from the backoff the change stream restarts with.
    private RetryBackoffSpec unboundedBackoff() {
        return Retry.backoff(Long.MAX_VALUE, config.minBackoff).maxBackoff(config.maxBackoff);
    }

    // Does what MongoCommons.resolveOpeningPosition does without blocking. Its javadoc says why a position that
    // resolves to the present is recorded before the change stream opens. record compares and sets the position.
    private Mono<StartAt> openingPosition(AtomicReference<StartAt> currentStartAt, BiPredicate<StartAt, StartAt> record) {
        return Mono.defer(() -> {
            SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(ReactorMongoSubscriptionModel.class);
            StartAt tracked = currentStartAt.get();
            StartAt resolved = tracked.get(subscriptionModelContext);
            if (!MongoCommons.opensAtThePresent(resolved)) {
                return Mono.just(requireNonNull(resolved));
            }
            // An empty reply would end the reads with no error, so nothing would restart them
            return mongo.executeCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND)
                    .switchIfEmpty(Mono.error(() -> new IllegalStateException("MongoDB returned no reply to " + MongoCommons.CURRENT_OPERATION_TIME_COMMAND.toJson())))
                    .flatMap(reply -> {
                        BsonTimestamp operationTime = MongoCommons.operationTimeAfter(reply);
                        if (operationTime == null) {
                            log.warn(MongoCommons.noOperationTimeToPinToMessage(reply));
                            return Mono.just(StartAt.now());
                        }
                        if (record.test(tracked, MongoCommons.pinnedTo(tracked, operationTime))) {
                            return Mono.just(StartAt.checkpoint(new MongoOperationTimeCheckpoint(operationTime)));
                        }
                        return openingPosition(currentStartAt, record);
                    });
        });
    }

    // ChangeStreamHistoryLost (286) restarts from StartAt.now() only when configured to. Everything else
    // (failover, transient network error, a cursor MongoDB closed) restarts from the tracked position. Mirrors
    // NativeMongoSubscriptionModel and SpringMongoSubscriptionModel.
    private boolean shouldRestart(@Nullable String subscriptionId, Throwable throwable, Runnable restartAtThePresent) {
        String subscription = subscriptionId == null ? "the subscription" : "subscription " + subscriptionId;
        if (isChangeStreamHistoryLost(throwable)) {
            if (config.restartSubscriptionsOnChangeStreamHistoryLost) {
                log.warn("There was not enough oplog to resume {}, will restart subscription from current time.", subscription, throwable);
                restartAtThePresent.run();
                return true;
            } else {
                log.error("There was not enough oplog to resume {}, will not restart subscription! Consider removing the subscription from the durable storage or use a catch-up subscription to get up to speed if needed.", subscription, throwable);
                return false;
            }
        }
        log.warn("Error caught for change stream of {}: {} {}. Will restart!", subscription, throwable.getClass().getName(), throwable.getMessage(), throwable);
        return true;
    }

    // Spring wraps the driver's exception, so the cause chain is searched
    private static boolean isChangeStreamHistoryLost(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof MongoCommandException mongoCommandException && mongoCommandException.getErrorCode() == MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE) {
                return true;
            }
        }
        return false;
    }

    // The same mapping of filter values Spring Data's ReactiveMongoTemplate.changeStream used
    private AggregationOperationContext filterContext() {
        MongoConverter converter = mongo.getConverter();
        return new TypeBasedAggregationOperationContext(Object.class, converter.getMappingContext(), new QueryMapper(converter), FieldLookupPolicy.relaxed());
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

    @Override
    public void addQuietPositionListener(QuietPositionListener listener) {
        quietPositionListeners.add(requireNonNull(listener, QuietPositionListener.class.getSimpleName() + " cannot be null"));
    }

    @Override
    public void removeQuietPositionListener(QuietPositionListener listener) {
        quietPositionListeners.remove(listener);
    }

    /**
     * Pause an individual subscription. The change stream behind it is closed and the {@code Mono} of an action still
     * running is cancelled, but the position it has read to is kept, so {@link #resumeSubscription(String)} continues
     * from there and events written while it was paused are delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        Run run;
        synchronized (this) {
            if (shutdown) {
                throw new IllegalStateException(ReactorMongoSubscriptionModel.class.getSimpleName() + " is shutdown");
            }
            requireKnown(subscriptionId);
            if (isPaused(subscriptionId)) {
                throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
            } else if (!isRunning(subscriptionId)) {
                throw new SubscriptionNotRunningException(subscriptionId);
            }
            run = pause(subscriptionId);
        }
        closeOutsideTheMonitor(run);
    }

    // Holds the monitor, and returns the run to close once it's released
    private @Nullable Run pause(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            return null;
        }
        pausedSubscriptions.put(subscriptionId, internalSubscription);
        Run run = internalSubscription.run;
        internalSubscription.run = null;
        return run;
    }

    // Closing cancels the Mono of a running action, and cancelling runs the caller's code
    private static void closeOutsideTheMonitor(@Nullable Run run) {
        if (run != null) {
            run.close();
        }
    }

    /**
     * Resume a paused subscription from the change stream position it had read to, so that nothing written while it
     * was paused is lost. The resumed subscription reads nothing until the {@code Mono} of an action the pause
     * cancelled has been cancelled.
     * <p>
     * Delivery is <i>at least once</i> across a pause: an event whose action's {@code Mono} had not completed when
     * the subscription was paused, and every event another consumer of the same subscription id handled in the
     * meantime, is handed to this action again on resume. That is deliberate, since wasted work is the cheaper
     * mistake, and it means actions must be idempotent. A read that returned no event for the subscription moves its
     * position to the token MongoDB sent with that read, so a subscription that matched nothing for a while resumes
     * from there rather than from its last event. This model asks MongoDB for its operation time right before the
     * change stream of a subscription started at the present opens, and records it as that subscription's position.
     * One paused before it read anything resumes from that time, so the events written since are delivered too, as long
     * as the oplog still holds that time. When it no longer does, the resume gets the handling that
     * {@code restartSubscriptionsOnChangeStreamHistoryLost} configures. When MongoDB's reply has no operation time, or the
     * subscription was paused before MongoDB answered, nothing is recorded and the resume opens at the present.
     *
     * @see #pauseSubscription(String)
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        RunStart runStart;
        synchronized (this) {
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
            // Reuses the same currentStartAt reference so resume continues from the position the paused run reached,
            // not the original StartAt.
            runStart = startRun(internalSubscription);
        }
        return launch(runStart);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        Run run = null;
        synchronized (this) {
            InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
            if (internalSubscription != null) {
                run = internalSubscription.run;
                internalSubscription.run = null;
            }
            pausedSubscriptions.remove(subscriptionId);
        }
        closeOutsideTheMonitor(run);
    }

    @PreDestroy
    @Override
    public void shutdown() {
        List<Run> runs = new ArrayList<>();
        synchronized (this) {
            shutdown = true;
            running = false;
            runningSubscriptions.values().forEach(internalSubscription -> {
                if (internalSubscription.run != null) {
                    runs.add(internalSubscription.run);
                    internalSubscription.run = null;
                }
            });
            runningSubscriptions.clear();
            pausedSubscriptions.clear();
        }
        runs.forEach(ReactorMongoSubscriptionModel::closeOutsideTheMonitor);
    }

    @Override
    public void stop() {
        List<@Nullable Run> runs = new ArrayList<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            running = false;
            // Snapshot the keys before iterating: pause moves each id from runningSubscriptions to pausedSubscriptions
            // as it goes, and forEach over a map that its own callback mutates can visit an entry that has already
            // moved, or miss one that has not. Mirrors ReactorDurableSubscriptionModel.
            new ArrayList<>(runningSubscriptions.keySet()).forEach(subscriptionId -> runs.add(pause(subscriptionId)));
        }
        runs.forEach(ReactorMongoSubscriptionModel::closeOutsideTheMonitor);
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        List<RunStart> runStarts = new ArrayList<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // Same snapshot reasoning as stop(): starting a run moves each id out of pausedSubscriptions as it
                // goes, so iterating the live map here would be exposed to the same hazard.
                new ArrayList<>(pausedSubscriptions.keySet()).forEach(subscriptionId -> {
                    InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
                    if (internalSubscription != null) {
                        runStarts.add(startRun(internalSubscription));
                    }
                });
            }
        }
        runStarts.forEach(this::launch);
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
        final String subscriptionId;
        final @Nullable SubscriptionFilter filter;
        final AtomicReference<StartAt> currentStartAt;
        final Function<CloudEvent, Mono<Void>> action;
        // The run that reads for the subscription while it's running, guarded by the model's monitor
        @Nullable Run run;

        private InternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Function<CloudEvent, Mono<Void>> action) {
            this.subscriptionId = subscriptionId;
            this.filter = filter;
            this.currentStartAt = currentStartAt;
            this.action = action;
        }
    }

    private record RunStart(InternalSubscription internalSubscription, Run run, Mono<Void> previousRunEnded) {
    }

    // What one aggregate or getMore of the change stream returned
    private record Batch(List<ChangeStreamDocument<Document>> documents, @Nullable BsonDocument postBatchResumeToken) {
    }

    private record Read(StartAt position, @Nullable CloudEvent cloudEvent) {
    }

    /**
     * The reads of one subscription from subscribe, resume or start until a pause, a cancel, a shutdown, or an error
     * the model doesn't restart on. What the run does for the subscription between two reads is a step: asking the
     * quiet position listeners, calling the action, and handing over a quiet position. A step starts only while the
     * run is open, and the position moves only while the run is open or a step is under way. The run has ended once it
     * is closed and no step is under way, and the next run for the subscription reads nothing before that.
     * <p>
     * The monitor of the run guards only its own fields, and no code of the caller runs while it's held.
     */
    private static final class Run {
        private final Sinks.Empty<Void> started = Sinks.empty();
        private final Sinks.Empty<Void> ended = Sinks.empty();
        private final Disposable.Swap reads = Disposables.swap();
        private boolean open = true;
        private int stepsUnderWay;
        private boolean hasEnded;

        Mono<Void> started() {
            return started.asMono();
        }

        Mono<Void> ended() {
            return ended.asMono();
        }

        // The cursor is open
        void opened() {
            started.tryEmitEmpty();
        }

        void failedToStart(Throwable throwable) {
            started.tryEmitError(throwable);
        }

        // Disposed straight away when the run was closed before it was subscribed to
        void readWith(Disposable disposable) {
            reads.update(disposable);
        }

        void close() {
            synchronized (this) {
                open = false;
            }
            // Cancels a step under way, whose end then counts it out
            reads.dispose();
            endIfIdle();
        }

        <T> Mono<T> step(Supplier<Mono<T>> work) {
            return Mono.defer(() -> {
                if (!startStep()) {
                    // Closed, and the reads are being disposed
                    return Mono.never();
                }
                AtomicBoolean stepEnded = new AtomicBoolean();
                Runnable endStep = () -> {
                    if (stepEnded.compareAndSet(false, true)) {
                        endStep();
                    }
                };
                // doOnTerminate ends the step before its error or completion reaches the retry of the reads, so a
                // restart starts no step while this one is under way. doFinally ends it after a cancel has reached the work.
                return Mono.defer(work).doOnTerminate(endStep).doFinally(__ -> endStep.run());
            });
        }

        void move(AtomicReference<StartAt> position, StartAt next) {
            synchronized (this) {
                if (open || stepsUnderWay > 0) {
                    position.set(next);
                }
            }
        }

        // A run that may no longer move the position answers true, since nothing it opens delivers anything
        boolean compareAndMove(AtomicReference<StartAt> position, StartAt expected, StartAt next) {
            synchronized (this) {
                return !(open || stepsUnderWay > 0) || position.compareAndSet(expected, next);
            }
        }

        private synchronized boolean startStep() {
            if (!open) {
                return false;
            }
            stepsUnderWay++;
            return true;
        }

        private void endStep() {
            synchronized (this) {
                stepsUnderWay--;
            }
            endIfIdle();
        }

        private void endIfIdle() {
            synchronized (this) {
                if (open || stepsUnderWay > 0 || hasEnded) {
                    return;
                }
                hasEnded = true;
            }
            ended.tryEmitEmpty();
        }
    }

    // One opened change stream. Every command goes through the session the cursor belongs to.
    private final class ChangeStreamCursor {
        private final ReactiveMongoOperations operations;
        private final AtomicLong cursorId;

        private ChangeStreamCursor(ReactiveMongoOperations operations, AtomicLong cursorId) {
            this.operations = operations;
            this.cursorId = cursorId;
        }

        Mono<Batch> open(StartAt position, @Nullable SubscriptionFilter filter) {
            // A position at an operation time maps to startAtOperationTime, which includes an operation at exactly
            // the given time.
            Document changeStream = MongoCommons.applyStartPosition(new Document(), (stage, resumeToken) -> stage.append("startAfter", resumeToken),
                    (stage, operationTime) -> stage.append("startAtOperationTime", operationTime), position, new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
            List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$changeStream", changeStream));
            pipeline.addAll(ApplyFilterToChangeStreamOptionsBuilder.changeStreamPipeline(timeRepresentation, filter, filterContext()));
            return run(new Document("aggregate", eventCollection).append("pipeline", pipeline).append("cursor", new Document()), "firstBatch");
        }

        // Waits on the server for up to its default of one second when nothing new matches
        Mono<Batch> getMore() {
            long id = cursorId.get();
            if (id == 0) {
                return Mono.error(new IllegalStateException("MongoDB closed the change stream cursor on collection " + eventCollection));
            }
            return run(new Document("getMore", id).append("collection", eventCollection), "nextBatch");
        }

        private Mono<Batch> run(Document command, String batchField) {
            return operations.execute(database -> Mono.from(database.runCommand(command, BsonDocument.class))
                            .map(reply -> batch(reply, batchField, database.getCodecRegistry())))
                    .next()
                    .switchIfEmpty(Mono.error(() -> new IllegalStateException("MongoDB returned no reply to " + command.keySet().iterator().next() + " on collection " + eventCollection)));
        }

        private Batch batch(BsonDocument reply, String batchField, CodecRegistry codecRegistry) {
            BsonDocument cursor = reply.getDocument("cursor");
            cursorId.set(cursor.getNumber("id").longValue());
            Codec<ChangeStreamDocument<Document>> codec = ChangeStreamDocument.createCodec(Document.class, codecRegistry);
            List<ChangeStreamDocument<Document>> documents = new ArrayList<>();
            for (BsonValue document : cursor.getArray(batchField)) {
                documents.add(codec.decode(new BsonDocumentReader(document.asDocument()), DecoderContext.builder().build()));
            }
            return new Batch(documents, cursor.isDocument("postBatchResumeToken") ? cursor.getDocument("postBatchResumeToken") : null);
        }
    }
}
