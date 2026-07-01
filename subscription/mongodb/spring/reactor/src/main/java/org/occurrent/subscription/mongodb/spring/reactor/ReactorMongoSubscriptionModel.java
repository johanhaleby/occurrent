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
import org.occurrent.subscription.PositionAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalSubscriptionPositionErrorMessage;

/**
 * This is a subscription that uses project reactor and Spring to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc. It produces a {@link CloudEvent} implementation of type {@link PositionAwareCloudEvent}
 * that includes the subscription position. Use {@link PositionAwareCloudEvent#getSubscriptionPositionOrThrowIAE(CloudEvent)}
 * to get the subscription position.
 * <p>
 * Survives the same class of MongoDB operational disruption {@code SpringMongoSubscriptionModel} does (replica-set
 * failovers, transient network errors, and, if configured to, change stream history loss): the underlying change
 * stream automatically resubscribes and resumes from the position of the last change-stream document read, so
 * recovery is gap-free rather than a replay or a skipped window. See {@link ReactorMongoSubscriptionModelConfig}.
 * <p>
 * Also supports named, lifecycle-managed subscriptions ({@link Subscribable}, {@link SubscriptionModelLifeCycle}):
 * pause, resume, and cancel an individual subscription by id, in addition to the plain {@link #subscribe(SubscriptionFilter, StartAt)}
 * {@link Flux} primitive.
 */
@NullMarked
public class ReactorMongoSubscriptionModel implements PositionAwareSubscriptionModel, Subscribable, SubscriptionModelLifeCycle {
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
        // currentStartAt tracks the position of the last change-stream document read (updated in changeStream(...)
        // below, whether or not it produced a delivered CloudEvent), read again by resilientChangeStream(...) on
        // every resubscribe that retryWhen triggers, so recovery from an error continues gap-free instead of
        // replaying or skipping events. Flux.defer gives each subscriber to the returned Flux its own tracked position.
        return Flux.defer(() -> {
            AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
            return resilientChangeStream(filter, currentStartAt, null);
        });
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }
        if (shutdown) {
            throw new IllegalStateException("Cannot start subscription because the subscription model is shutdown.");
        }
        return startInternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action);
    }

    private Subscription startInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Function<CloudEvent, Mono<Void>> action) {
        Sinks.Empty<Void> startedSink = Sinks.empty();
        Disposable disposable = resilientChangeStream(filter, currentStartAt, startedSink)
                .concatMap(action)
                .subscribe(unused -> {
                        }, throwable -> log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable));
        InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, startedSink.asMono());
        if (running) {
            runningSubscriptions.put(subscriptionId, internalSubscription);
        } else {
            // The model is stopped: dispose immediately so this subscription doesn't deliver events while paused,
            // matching pauseSubscription's own contract. start(true)/resumeSubscription is what actually starts it.
            disposable.dispose();
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
        return new ReactorMongoSubscription(subscriptionId, internalSubscription.started);
    }

    private Flux<CloudEvent> resilientChangeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Sinks.@Nullable Empty<Void> startedSink) {
        return changeStream(filter, currentStartAt, startedSink)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, config.minBackoff)
                        .maxBackoff(config.maxBackoff)
                        .filter(throwable -> shouldRestart(throwable, currentStartAt)));
    }

    private Flux<CloudEvent> changeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Sinks.@Nullable Empty<Void> startedSink) {
        return Flux.defer(() -> {
            SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(ReactorMongoSubscriptionModel.class);
            // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, currentStartAt.get(), subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
            // "Started" only means the change stream Flux has been subscribed to, not that the server has
            // acknowledged the command and the cursor is positioned. This is weaker than NativeMongoSubscriptionModel's
            // latch, which only fires after that blocking round trip has already completed.
            if (startedSink != null) {
                changeStream = changeStream.doOnSubscribe(subscription -> startedSink.tryEmitEmpty());
            }
            return changeStream
                    .flatMap(changeEvent -> {
                        ChangeStreamDocument<Document> raw = changeEvent.getRaw();
                        if (raw == null) {
                            // Mirrors SpringMongoSubscriptionModel's same defensive check. Not expected to ever
                            // happen, but skipping this one event beats an NPE that retries the whole subscription.
                            log.error("Internal error: ChangeStreamEvent for collection {} had a null raw document", eventCollection);
                            return Mono.empty();
                        }
                        MongoResumeTokenSubscriptionPosition subscriptionPosition = new MongoResumeTokenSubscriptionPosition(requireNonNull(raw.getResumeToken()));
                        // Advance the tracked position for every change-stream document received, even if it
                        // doesn't deserialize into a delivered CloudEvent, mirroring NativeMongoSubscriptionModel,
                        // so a resubscribe after an error resumes gap-free.
                        currentStartAt.set(StartAt.subscriptionPosition(subscriptionPosition));
                        return MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                                .map(cloudEvent -> new PositionAwareCloudEvent(cloudEvent, subscriptionPosition))
                                .map(Mono::just)
                                .orElse(Mono.empty());
                    });
        });
    }

    // Classifies the error a change-stream Flux terminated with. ChangeStreamHistoryLost (286) restarts from
    // StartAt.now() only when configured to; everything else (a failover, a transient network error, or anything
    // else the driver itself could not resume) restarts from the tracked position. Mirrors the classification in
    // NativeMongoSubscriptionModel and SpringMongoSubscriptionModel.
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

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        return mongo.executeCommand(new Document("hostInfo", 1))
                .map(MongoCommons::getServerOperationTime)
                .onErrorResume(UncategorizedMongoDbException.class, throwable -> {
                    if (throwable.getCause() instanceof MongoCommandException) {
                        // This can if the server doesn't allow to get the operation time since "db.adminCommand( { "hostInfo" : 1 } )" is prohibited.
                        // This is the case on for example shared Atlas clusters. If this happens we return the current time of the client instead.
                        log.warn(cannotFindGlobalSubscriptionPositionErrorMessage(throwable.getCause()));
                        return Mono.empty();
                    } else {
                        return Mono.error(throwable);
                    }
                })
                .map(MongoOperationTimeSubscriptionPosition::new);
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorMongoSubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
        } else if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        }

        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorMongoSubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already running");
        }

        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }

        running = true;
        // Reuses the same currentStartAt reference so resume continues from the position of the last event
        // delivered before the subscription was paused, rather than replaying (or skipping) from the original StartAt.
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
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
        }
    }

    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        if (!shutdown) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                pausedSubscriptions.forEach((subscriptionId, __) -> resumeSubscription(subscriptionId));
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