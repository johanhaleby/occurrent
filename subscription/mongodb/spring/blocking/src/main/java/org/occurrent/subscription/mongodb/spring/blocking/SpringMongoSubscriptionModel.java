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
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.PositionAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
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

import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalSubscriptionPositionErrorMessage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this subscription doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringMongoSubscriptionModel implements PositionAwareSubscriptionModel, SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;

    private volatile boolean shutdown = false;

    /**
     * Create a blocking subscription using Spring. It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively
     * go up to max 2 seconds wait time between each retry when reading/saving/deleting the subscription position.
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
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate, config.executor);
        this.messageListenerContainer.start();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, "StartAt cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }

        // We wrap the creation of ChangeStreamRequestOptions in a supplier since since otherwise the "startAtSupplier"
        // would be supplied only once, here, during initialization. When using a supplier here, the "startAtSupplier"
        // is called again when pausing and resuming a subscription. Take the case when a subscription is started with "StartAt.now()".
        // If we hadn't used a supplier and a subscription is paused and later resumed, it'll be resumed from the _initial_ "StartAt.now()" position,
        // and not the position the "StartAt.now()" position of when the subscription was resumed. This will lead to historic events being
        // replayed which is (most likely) not what the user expects.
        Function<StartAt, ChangeStreamRequestOptions> requestOptionsFunction = overridingStartAt -> {
            // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, overridingStartAt == null ? startAt : overridingStartAt);
            final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
            return new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        };

        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                    .map(cloudEvent -> new PositionAwareCloudEvent(cloudEvent, new MongoResumeTokenSubscriptionPosition(resumeToken)))
                    .ifPresent(executeWithRetry(action, __ -> !shutdown, retryStrategy));
        };

        Function<StartAt, ChangeStreamRequest<Document>> requestBuilder = sa -> new ChangeStreamRequest<>(listener, requestOptionsFunction.apply(sa));
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = registerNewSpringSubscription(subscriptionId, requestBuilder.apply(null));
        SpringMongoSubscription springMongoSubscription = new SpringMongoSubscription(subscriptionId, subscription);
        if (messageListenerContainer.isRunning()) {
            runningSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, requestBuilder));
        } else {
            pausedSubscriptions.put(subscriptionId, new InternalSubscription(springMongoSubscription, requestBuilder));
        }
        return springMongoSubscription;
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        InternalSubscription subscription = runningSubscriptions.remove(subscriptionId);
        if (subscription != null) {
            messageListenerContainer.remove(subscription.getSpringSubscription());
        }
    }

    @PreDestroy
    @Override
    public synchronized void shutdown() {
        shutdown = true;
        runningSubscriptions.forEach((__, internalSubscription) -> internalSubscription.shutdown());
        runningSubscriptions.clear();
        pausedSubscriptions.forEach((__, internalSubscription) -> internalSubscription.shutdown());
        pausedSubscriptions.clear();
        stopMessageListenerContainer();
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        // Note that we increase the "increment" by 1 in order to not clash with an existing event in the event store.
        // This is so that we can avoid duplicates in certain rare cases when replaying events.
        BsonTimestamp currentOperationTime;
        try {
            currentOperationTime = MongoCommons.getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)), 1);
        } catch (UncategorizedMongoDbException e) {
            if (e.getCause() instanceof MongoCommandException) {
                log.warn(cannotFindGlobalSubscriptionPositionErrorMessage(e.getCause()));
                // This can if the server doesn't allow to get the operation time since "db.adminCommand( { "hostInfo" : 1 } )" is prohibited.
                // This is the case on for example shared Atlas clusters. If this happens we return the current time of the client instead.
                return null;
            } else {
                throw e;
            }
        }
        return new MongoOperationTimeSubscriptionPosition(currentOperationTime);
    }

    // Life-cycle implementation

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't running.");
        }
        messageListenerContainer.remove(internalSubscription.getSpringSubscription());
        pausedSubscriptions.put(subscriptionId, internalSubscription);
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }

        if (!messageListenerContainer.isRunning()) {
            messageListenerContainer.start();
        }

        org.springframework.data.mongodb.core.messaging.Subscription newSubscription = registerNewSpringSubscription(subscriptionId, internalSubscription.newChangeStreamRequest());
        InternalSubscription newInternalSubscription = internalSubscription.copy(newSubscription);
        runningSubscriptions.put(subscriptionId, newInternalSubscription);
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
    public synchronized void start() {
        if (!shutdown) {
            messageListenerContainer.start();
            pausedSubscriptions.forEach((subscriptionId, __) -> resumeSubscription(subscriptionId).waitUntilStarted());
        }
    }

    @Override
    public synchronized void stop() {
        if (!shutdown) {
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
            stopMessageListenerContainer();
        }
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
        CountDownLatch countDownLatch = new CountDownLatch(1);
        messageListenerContainer.stop(countDownLatch::countDown);
        try {
            boolean success = countDownLatch.await(10, TimeUnit.SECONDS);
            if (!success) {
                log.warn("Failed to stop " + SpringMongoSubscriptionModel.class.getSimpleName() + " after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private org.springframework.data.mongodb.core.messaging.Subscription registerNewSpringSubscription(String subscriptionId, ChangeStreamRequest<Document> documentChangeStreamRequest) {
        return messageListenerContainer.register(documentChangeStreamRequest, Document.class, throwable -> {
            if (throwable instanceof UncategorizedMongoDbException) {
                MongoCommandException e = (MongoCommandException) throwable.getCause();
                if (e.getErrorCode() == CHANGE_STREAM_HISTORY_LOST_ERROR_CODE) {
                    String restartMessage = restartSubscriptionsOnChangeStreamHistoryLost ? "will restart subscription from current time." :
                            "will not restart subscription! Consider remove the subscription from the durable storage or use a catch-up subscription to get up to speed if needed.";
                    if (restartSubscriptionsOnChangeStreamHistoryLost) {
                        log.warn("There was not enough oplog to resume subscription {}, {}", subscriptionId, restartMessage, throwable);
                        InternalSubscription internalSubscription = runningSubscriptions.get(subscriptionId);
                        if (internalSubscription != null) {
                            new Thread(() -> {
                                // We restart from now!!
                                org.springframework.data.mongodb.core.messaging.Subscription oldSpringSubscription = internalSubscription.getSpringSubscription();
                                ChangeStreamRequest<Document> newChangeStreamRequestFromNow = internalSubscription.newChangeStreamRequest(StartAt.now());
                                org.springframework.data.mongodb.core.messaging.Subscription newSpringSubscription = registerNewSpringSubscription(subscriptionId, newChangeStreamRequestFromNow);
                                internalSubscription.occurrentSubscription.changeSubscription(newSpringSubscription);
                                messageListenerContainer.remove(oldSpringSubscription);
                                log.info("Subscription {} successfully restarted", subscriptionId);
                            }).start();
                        }
                    } else {
                        log.error("There was not enough oplog to resume subscription {}, {}", subscriptionId, restartMessage, throwable);
                    }
                }
            } else if (isCursorNoLongerOpen(throwable)) {
                if (log.isDebugEnabled()) {
                    log.debug("Cursor is not longer open for subscription {}, this may happen if you pause a subscription very soon after subscribing.", subscriptionId, throwable);
                }
            } else {
                log.error("An error occurred for subscription {}", subscriptionId, throwable);
            }
        });
    }

    private static boolean isCursorNoLongerOpen(Throwable throwable) {
        return throwable instanceof IllegalStateException && throwable.getMessage().startsWith("Cursor") && throwable.getMessage().endsWith("is not longer open.");
    }

    // Model that hold both the spring subscription and the change stream request so that we can pause the subscription
    // (by removing it and starting it again)
    private static class InternalSubscription {
        private final SpringMongoSubscription occurrentSubscription;
        private final Function<StartAt, ChangeStreamRequest<Document>> changeStreamRequestBuilder;

        private InternalSubscription(SpringMongoSubscription subscription, Function<StartAt, ChangeStreamRequest<Document>> changeStreamRequestBuilder) {
            this.occurrentSubscription = subscription;
            this.changeStreamRequestBuilder = changeStreamRequestBuilder;
        }

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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalSubscription)) return false;
            InternalSubscription that = (InternalSubscription) o;
            return Objects.equals(occurrentSubscription, that.occurrentSubscription) && Objects.equals(changeStreamRequestBuilder, that.changeStreamRequestBuilder);
        }

        org.springframework.data.mongodb.core.messaging.Subscription getSpringSubscription() {
            return occurrentSubscription.getSubscriptionReference().get();
        }

        void shutdown() {
            occurrentSubscription.shutdown();
        }

        @Override
        public int hashCode() {
            return Objects.hash(occurrentSubscription, changeStreamRequestBuilder);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", InternalSubscription.class.getSimpleName() + "[", "]")
                    .add("occurrentSubscription=" + occurrentSubscription)
                    .add("changeStreamRequestBuilder=" + changeStreamRequestBuilder)
                    .toString();
        }
    }
}