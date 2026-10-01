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
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
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
 * A subscription with an id reads its change stream through the MongoDB driver's change stream cursor, one batch at a
 * time, and asks for the next batch once the action's {@code Mono} has completed for every event of the batch before.
 * While it waits for a batch, it looks every second at the resume token MongoDB sent with the last one. When the token
 * of another reply replaces it during the same wait, the batch it came with had no event for the subscription, so the
 * subscription's position moves to it. A pause or a restart then doesn't resume from a position the oplog has dropped
 * while the subscription was up to date. The model reports that position through
 * {@link QuietPositionReportingSubscriptions}.
 * <p>
 * The driver's reactive API doesn't hand out that token, so the model reads it from a private field of the driver.
 * When that doesn't work with the driver in use, the model logs a warning and reads the change stream through
 * {@link ReactiveMongoOperations#changeStream(String, ChangeStreamOptions, Class)} instead, reading ahead of the
 * action. Its position then stays at the last event it handled. The plain
 * {@link #subscribe(SubscriptionFilter, StartAt)} {@link Flux} always reads that way.
 * <p>
 * After a replica-set failover or a transient network error, the driver opens the change stream again by itself. After
 * any other error and, if configured to, lost change stream history, the model opens it again with a backoff, from the
 * position of the last change stream document the subscription has handled. See {@link ReactorMongoSubscriptionModelConfig}.
 * <p>
 * Also supports named, lifecycle-managed subscriptions, which is what makes it a {@link SubscriptionModel} ({@link Subscribable}
 * plus {@link SubscriptionModelLifeCycle}): pause, resume, and cancel an individual subscription by id, in addition to
 * the plain {@link #subscribe(SubscriptionFilter, StartAt)} {@link Flux} primitive.
 */
@NullMarked
public class ReactorMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, IntrospectableSubscriptions, QuietPositionReportingSubscriptions {
    private static final Logger log = LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class);

    // How long the model waits for a batch before it looks at the token again. A getMore that finds nothing new waits up
    // to a second on the server by default, so a quiet change stream gets a new token about as often.
    private static final Duration QUIET_POSITION_CHECK_INTERVAL = Duration.ofSeconds(1);

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final ReactorMongoSubscriptionModelConfig config;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();
    // Completes once every run started so far for the id has ended, kept until then, so the next run for the id waits for all of them
    private final ConcurrentMap<String, Mono<Void>> runsEnded = new ConcurrentHashMap<>();
    private final List<QuietPositionListener> quietPositionListeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean readsQuietPositions;

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
        String unavailableBecause = driverCursorUnavailableBecause();
        this.readsQuietPositions = new AtomicBoolean(unavailableBecause == null);
        if (unavailableBecause != null) {
            warnThatQuietPositionsAreNotRead(unavailableBecause);
        }
    }

    // Loading the class fails when the driver lacks a class it uses
    private static @Nullable String driverCursorUnavailableBecause() {
        try {
            return DriverChangeStreamCursor.unavailableBecause();
        } catch (LinkageError e) {
            return e.toString();
        }
    }

    private static void warnThatQuietPositionsAreNotRead(String reason) {
        log.warn("{} can't read the resume token MongoDB sends with a batch from this MongoDB driver ({}), so it reads change streams through Spring's changeStream instead. A subscription that matches no event for a while keeps the position of the last event it handled, which the oplog can drop.",
                ReactorMongoSubscriptionModel.class.getSimpleName(), reason);
    }

    // Whether subscriptions with an id read through the driver's cursor and move a quiet position
    boolean readsQuietPositions() {
        return readsQuietPositions.get();
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        // currentStartAt tracks the last change-stream document read (even if it produced no delivered
        // CloudEvent), so a resubscribe from retryWhen resumes gap-free. Safe here since the caller consumes
        // the Flux directly. The named-subscription paths below advance only on action completion.
        // Flux.defer gives each subscriber its own tracked position.
        return Flux.defer(() -> {
            AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
            return changeStream(filter, currentStartAt, currentStartAt::compareAndSet, currentStartAt::set, () -> {
            }).retryWhen(unboundedBackoff().filter(throwable -> shouldRestart(null, throwable, () -> currentStartAt.set(StartAt.now()))));
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
        String subscriptionId = internalSubscription.subscriptionId;
        Run run = new Run();
        internalSubscription.run = run;
        runningSubscriptions.put(subscriptionId, internalSubscription);
        // Covers every earlier run, and not only the last one, since a cancel can end a run that is still waiting for
        // the one before it
        Mono<Void> earlierRunsEnded = runsEnded.getOrDefault(subscriptionId, Mono.empty());
        Mono<Void> allRunsEnded = Mono.when(earlierRunsEnded, run.ended()).cache();
        runsEnded.put(subscriptionId, allRunsEnded);
        allRunsEnded.doOnTerminate(() -> runsEnded.remove(subscriptionId, allRunsEnded)).subscribe();
        return new RunStart(internalSubscription, run, earlierRunsEnded);
    }

    // Runs without the monitor, since subscribing resolves the start position, which can call the caller's function
    private Subscription launch(RunStart runStart) {
        InternalSubscription internalSubscription = runStart.internalSubscription();
        Run run = runStart.run();
        // The run reads nothing until every earlier run for the id has ended, so its steps never overlap one of theirs
        Disposable reads = runStart.earlierRunsEnded()
                .thenMany(Flux.defer(() -> reads(internalSubscription, run)))
                .subscribe(__ -> {
                        }, throwable -> runEnded(internalSubscription, run, throwable),
                        () -> runEnded(internalSubscription, run, new IllegalStateException("The change stream of subscription " + internalSubscription.subscriptionId + " completed")));
        run.readWith(reads);
        return new ReactorMongoSubscription(internalSubscription.subscriptionId, run.started());
    }

    private void runEnded(InternalSubscription internalSubscription, Run run, Throwable throwable) {
        String subscriptionId = internalSubscription.subscriptionId;
        log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable);
        // No-op if the subscription had already started, otherwise this keeps waitUntilStarted() from hanging forever
        run.failedToStart(throwable);
        run.close();
        synchronized (this) {
            // A dead subscription must not count as running, or isRunning(id) would lie and the id couldn't be reused
            // without an explicit cancelSubscription(). Only while this run is still the subscription's, since a pause
            // and a resume, or a cancel and a new subscription with the same id, start a newer run.
            if (internalSubscription.run == run) {
                internalSubscription.run = null;
                runningSubscriptions.remove(subscriptionId, internalSubscription);
            }
        }
    }

    private Flux<Void> reads(InternalSubscription internalSubscription, Run run) {
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        return Flux.defer(() -> readsQuietPositions.get() ? readsWithQuietPositions(internalSubscription, run) : readsAhead(internalSubscription, run))
                .retryWhen(unboundedBackoff().filter(throwable -> shouldRestart(internalSubscription.subscriptionId, throwable, () -> run.move(currentStartAt, StartAt.now()))));
    }

    // How subscriptions with an id read before the model read the driver's cursor
    private Flux<Void> readsAhead(InternalSubscription internalSubscription, Run run) {
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        // concatMap can buffer several documents ahead of a slow action, so the position moves only once the action's
        // Mono has completed, and a pause, a cancel or a restart delivers again at most the event in hand
        return changeStream(internalSubscription.filter, currentStartAt, (expected, pinned) -> run.compareAndMove(currentStartAt, expected, pinned), __ -> {
        }, run::opened)
                .concatMap(cloudEvent -> run.step(() -> deliver(internalSubscription, run, cloudEvent)));
    }

    private Flux<Void> readsWithQuietPositions(InternalSubscription internalSubscription, Run run) {
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        return openingPosition(currentStartAt, (expected, pinned) -> run.compareAndMove(currentStartAt, expected, pinned))
                .flatMapMany(position -> Flux.usingWhen(
                        changeStreamAt(position, internalSubscription.filter).flatMap(DriverChangeStreamCursor::open).doOnSubscribe(__ -> run.opened()),
                        cursor -> Mono.defer(() -> nextBatch(internalSubscription, run, cursor)).repeat(),
                        cursor -> Mono.fromRunnable(cursor::close)))
                .onErrorResume(DriverChangeStreamCursor.Unavailable.class, unavailable -> {
                    if (readsQuietPositions.compareAndSet(true, false)) {
                        warnThatQuietPositionsAreNotRead(unavailable.getMessage());
                    }
                    return readsAhead(internalSubscription, run);
                });
    }

    // Waits for the next batch, and every QUIET_POSITION_CHECK_INTERVAL meanwhile looks for a quiet position. Nothing is
    // read ahead, since a token read while a batch is still being handled could be past an event its action hasn't had.
    private Mono<Void> nextBatch(InternalSubscription internalSubscription, Run run, DriverChangeStreamCursor cursor) {
        Sinks.One<List<ChangeStreamDocument<Document>>> batch = Sinks.one();
        cursor.next().subscribe(batch::tryEmitValue, batch::tryEmitError, () -> batch.tryEmitValue(List.of()));
        TokenWatch tokenWatch = new TokenWatch();
        return Mono.defer(() -> quietPositionHandlers(internalSubscription.subscriptionId, run)
                        .flatMap(quietPositionHandlers -> Mono.firstWithSignal(batch.asMono().map(Optional::of), Mono.delay(QUIET_POSITION_CHECK_INTERVAL).thenReturn(Optional.<List<ChangeStreamDocument<Document>>>empty()))
                                .flatMap(read -> read.isPresent() ? Mono.just(read)
                                        : run.step(() -> checkQuietPosition(internalSubscription, run, cursor, tokenWatch, quietPositionHandlers)).thenReturn(read))))
                .repeat()
                .filter(Optional::isPresent)
                .next()
                .flatMap(read -> run.step(() -> handle(internalSubscription, run, read.get())));
    }

    // Asked before every wait for a batch, inside a step, so a listener answers before it knows what the wait brings
    private Mono<List<Function<Checkpoint, Mono<Void>>>> quietPositionHandlers(String subscriptionId, Run run) {
        if (quietPositionListeners.isEmpty()) {
            return Mono.just(List.of());
        }
        return run.step(() -> Flux.fromIterable(quietPositionListeners)
                .concatMap(listener -> listener.beforeReading(subscriptionId))
                .collectList());
    }

    // Runs inside a step. Moves the position to a token the watch confirms and hands that position to the quiet position handlers.
    private Mono<Void> checkQuietPosition(InternalSubscription internalSubscription, Run run, DriverChangeStreamCursor cursor, TokenWatch tokenWatch, List<Function<Checkpoint, Mono<Void>>> quietPositionHandlers) {
        BsonDocument token = tokenWatch.confirmed(cursor.postBatchResumeToken());
        if (token == null) {
            return Mono.empty();
        }
        Checkpoint quietPosition = new MongoResumeTokenCheckpoint(token);
        run.move(internalSubscription.currentStartAt, StartAt.checkpoint(quietPosition));
        return Flux.fromIterable(quietPositionHandlers)
                .concatMap(quietPositionHandler -> quietPositionHandler.apply(quietPosition))
                .then();
    }

    // Runs inside a step. The action gets each event, and the position moves past an event once the action's Mono for
    // it has completed, so a pause, a cancel or a restart delivers again at most the event in hand.
    private Mono<Void> handle(InternalSubscription internalSubscription, Run run, List<ChangeStreamDocument<Document>> documents) {
        if (documents.isEmpty()) {
            return Mono.error(new IllegalStateException("MongoDB closed the change stream cursor on collection " + eventCollection));
        }
        return Flux.fromIterable(documents)
                .concatMap(document -> {
                    MongoResumeTokenCheckpoint checkpoint = new MongoResumeTokenCheckpoint(requireNonNull(document.getResumeToken()));
                    Optional<CloudEvent> cloudEvent = MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(document, timeRepresentation);
                    if (cloudEvent.isEmpty()) {
                        run.move(internalSubscription.currentStartAt, StartAt.checkpoint(checkpoint));
                        return Mono.empty();
                    }
                    return deliver(internalSubscription, run, new CheckpointAwareCloudEvent(cloudEvent.get(), checkpoint));
                })
                .then();
    }

    private Mono<Void> deliver(InternalSubscription internalSubscription, Run run, CloudEvent cloudEvent) {
        StartAt afterEvent = StartAt.checkpoint(CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(cloudEvent));
        String subscriptionId = internalSubscription.subscriptionId;
        // The action's own error is retried here, with the same backoff the change stream restarts with, mirroring the
        // blocking models' RetryStrategy around the handler (no attempt cap by default). Mono.defer, because a retry must
        // call the action again the way the blocking RetryStrategy calls the handler again: subscribing again to whatever
        // Mono the first call returned would replay that attempt's failure forever. A pause cancels a pending retry.
        return Mono.defer(() -> internalSubscription.action.apply(cloudEvent))
                .retryWhen(unboundedBackoff()
                        .doBeforeRetry(retrySignal -> log.warn("Action for subscription {} failed, will retry (attempt {})", subscriptionId, retrySignal.totalRetries() + 1, retrySignal.failure())))
                .then(Mono.fromRunnable(() -> run.move(internalSubscription.currentStartAt, afterEvent)));
    }

    // Built the way ReactiveMongoOperations.changeStream builds it
    private Mono<ChangeStreamPublisher<Document>> changeStreamAt(StartAt position, @Nullable SubscriptionFilter filter) {
        return mongo.getCollection(eventCollection).map(collection -> {
            List<Document> pipeline = ApplyFilterToChangeStreamOptionsBuilder.changeStreamPipeline(timeRepresentation, filter, filterContext());
            ChangeStreamPublisher<Document> changeStream = collection.watch(pipeline, Document.class).fullDocument(FullDocument.DEFAULT);
            // A position at an operation time maps to startAtOperationTime, which includes an operation at exactly the
            // given time.
            return MongoCommons.applyStartPosition(changeStream, ChangeStreamPublisher::startAfter, ChangeStreamPublisher::startAtOperationTime, position, new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
        });
    }

    // The same mapping of filter values ReactiveMongoOperations.changeStream uses
    private AggregationOperationContext filterContext() {
        MongoConverter converter = mongo.getConverter();
        return new TypeBasedAggregationOperationContext(Object.class, converter.getMappingContext(), new QueryMapper(converter), FieldLookupPolicy.relaxed());
    }

    // One spec for both retry sites, so the action retry cannot drift from the backoff the change stream restarts with.
    private RetryBackoffSpec unboundedBackoff() {
        return Retry.backoff(Long.MAX_VALUE, config.minBackoff).maxBackoff(config.maxBackoff);
    }

    // Reads through ReactiveMongoOperations.changeStream. recordOpeningPosition compares and sets the position, and
    // onSubscribe runs when the change stream is subscribed to.
    private Flux<CloudEvent> changeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, BiPredicate<StartAt, StartAt> recordOpeningPosition,
                                          Consumer<StartAt> onDocumentRead, Runnable onSubscribe) {
        SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(ReactorMongoSubscriptionModel.class);
        return openingPosition(currentStartAt, recordOpeningPosition).flatMapMany(openingPosition -> {
            // builder::resumeAt maps to the driver's startAtOperationTime here rather than to a resume token,
            // and that includes an operation stamped at exactly the given time.
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, openingPosition, subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
            // "Started" only means the change stream Flux was subscribed to, not that the server acknowledged
            // the command and the cursor is positioned. Weaker than NativeMongoSubscriptionModel's latch,
            // which only fires after that round trip completes.
            return changeStream
                    .doOnSubscribe(subscription -> onSubscribe.run())
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
            // An empty reply would complete the change stream Flux with no error, so nothing would restart it
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
    // (failover, transient network error, anything the driver itself couldn't resume) restarts from the
    // tracked position. Mirrors NativeMongoSubscriptionModel and SpringMongoSubscriptionModel.
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

    // Spring wraps the driver's exception, and the driver's cursor hands it over as it is
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
     * mistake, and it means actions must be idempotent. While the model reads through the driver's change stream
     * cursor, a subscription that matched nothing for a while has moved its position to the token MongoDB sent with a
     * batch that had no event for it, so it resumes from there rather than from its last event. This model asks MongoDB
     * for its operation time right before the change stream of a subscription started at the present opens, and records
     * it as that subscription's position. One paused before it read anything resumes from that time, so the events
     * written since are delivered too, as long as the oplog still holds that time. When it no longer does, the resume
     * gets the handling that {@code restartSubscriptionsOnChangeStreamHistoryLost} configures. When MongoDB's reply has
     * no operation time, or the subscription was paused before MongoDB answered, nothing is recorded and the resume
     * opens at the present.
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
        // The run that reads for the subscription while it's running. Read and written only while holding the model's monitor
        @Nullable Run run;

        private InternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Function<CloudEvent, Mono<Void>> action) {
            this.subscriptionId = subscriptionId;
            this.filter = filter;
            this.currentStartAt = currentStartAt;
            this.action = action;
        }
    }

    private record RunStart(InternalSubscription internalSubscription, Run run, Mono<Void> earlierRunsEnded) {
    }

    /**
     * The tokens seen during one wait for a batch. The driver asks MongoDB for more within the same wait only after a
     * batch came back with no event for the subscription, and the batch that ends the wait comes last. So a token that
     * a later look during the same wait finds replaced came with a batch that had no event, and the action has
     * completed for every event before it. The token of the batch that ends the wait is never confirmed, since that
     * batch's events haven't been handled yet.
     * <p>
     * The driver decodes a token of its own from every reply, so a look tells a new reply from the one before by the
     * instance it reads, even when MongoDB sent the same token again. A token is confirmed once per value.
     * <p>
     * Used by one wait at a time, one look after the other.
     */
    static final class TokenWatch {
        private @Nullable BsonDocument seen;
        private @Nullable BsonDocument confirmed;

        /**
         * @param token The token the driver's cursor holds now, or {@code null} when there is none to read.
         * @return The token this look confirms, or {@code null} when it confirms none.
         */
        @Nullable BsonDocument confirmed(@Nullable BsonDocument token) {
            if (token == null) {
                return null;
            }
            BsonDocument previous = seen;
            seen = token;
            if (previous == null || previous == token || previous.equals(confirmed)) {
                return null;
            }
            confirmed = previous;
            return previous;
        }
    }

    /**
     * The reads of one subscription from subscribe, resume or start until a pause, a cancel, a shutdown, or an error
     * the model doesn't restart on. Each thing the run does for the subscription between two reads is a step, such as
     * asking the quiet position listeners, calling the action, or handing over a quiet position. A step starts only while the
     * run is open, and the position moves only while the run is open or a step is under way. The run has ended once it
     * is closed and no step is under way, and the next run for the subscription reads nothing before that.
     * <p>
     * The monitor of the run guards only its own fields, and no code of the caller runs while it's held.
     */
    static final class Run {
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

        // The change stream was subscribed to
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
}
