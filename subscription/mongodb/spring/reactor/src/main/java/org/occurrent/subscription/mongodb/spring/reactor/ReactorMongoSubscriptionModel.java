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
import io.cloudevents.CloudEvent;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.util.concurrent.atomic.AtomicReference;

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
 */
@NullMarked
public class ReactorMongoSubscriptionModel implements PositionAwareSubscriptionModel {
    private static final Logger log = LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class);

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final ReactorMongoSubscriptionModelConfig config;

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
        // below, whether or not it produced a delivered CloudEvent), read again by changeStream(...) on every
        // resubscribe that retryWhen triggers, so recovery from an error continues gap-free instead of replaying or
        // skipping events. Flux.defer gives each subscriber to the returned Flux its own tracked position.
        return Flux.defer(() -> {
            AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
            return changeStream(filter, currentStartAt)
                    .retryWhen(Retry.backoff(Long.MAX_VALUE, config.minBackoff)
                            .maxBackoff(config.maxBackoff)
                            .filter(throwable -> shouldRestart(throwable, currentStartAt)));
        });
    }

    private Flux<CloudEvent> changeStream(@Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt) {
        return Flux.defer(() -> {
            SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(ReactorMongoSubscriptionModel.class);
            // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, currentStartAt.get(), subscriptionModelContext);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
            return changeStream
                    .flatMap(changeEvent -> {
                        MongoResumeTokenSubscriptionPosition subscriptionPosition = new MongoResumeTokenSubscriptionPosition(requireNonNull(changeEvent.getResumeToken()).asDocument());
                        // Advance the tracked position for every change-stream document received, even if it
                        // doesn't deserialize into a delivered CloudEvent, mirroring NativeMongoSubscriptionModel,
                        // so a resubscribe after an error resumes gap-free.
                        currentStartAt.set(StartAt.subscriptionPosition(subscriptionPosition));
                        return MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(requireNonNull(changeEvent.getRaw()), timeRepresentation)
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
}