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
import org.springframework.context.SmartLifecycle;
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
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.convertToDelayStream;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this subscription doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringMongoSubscriptionModel implements PositionAwareSubscriptionModel, SmartLifecycle {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;
    private final RetryStrategy retryStrategy;

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
        this(mongoTemplate, eventCollection, timeRepresentation, RetryStrategy.backoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param retryStrategy      A custom retry strategy to use if the {@code action} supplied to the subscription throws an exception.
     */
    public SpringMongoSubscriptionModel(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy) {
        requireNonNull(mongoTemplate, MongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");

        this.mongoOperations = mongoTemplate;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.runningSubscriptions = new ConcurrentHashMap<>();
        this.pausedSubscriptions = new ConcurrentHashMap<>();
        this.retryStrategy = retryStrategy;
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate);
        this.messageListenerContainer.start();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAtSupplier, "StartAt cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }

        // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
        ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, startAtSupplier.get());
        final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);

        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                    .map(cloudEvent -> new PositionAwareCloudEvent(cloudEvent, new MongoResumeTokenSubscriptionPosition(resumeToken)))
                    .ifPresent(executeWithRetry(action, __ -> !shutdown, convertToDelayStream(retryStrategy)));
        };

        ChangeStreamRequestOptions options = new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(listener, options);
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = messageListenerContainer.register(request, Document.class);
        runningSubscriptions.put(subscriptionId, new InternalSubscription(subscription, request));
        return new SpringMongoSubscription(subscriptionId, subscription);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        InternalSubscription subscription = runningSubscriptions.remove(subscriptionId);
        if (subscription != null) {
            messageListenerContainer.remove(subscription.springSubscription);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        shutdown = true;
        runningSubscriptions.clear();
        pausedSubscriptions.clear();
        messageListenerContainer.stop();
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        // Note that we increase the "increment" by 1 in order to not clash with an existing event in the event store.
        // This is so that we can avoid duplicates in certain rare cases when replaying events.
        BsonTimestamp currentOperationTime = MongoCommons.getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)), 1);
        return new MongoOperationTimeSubscriptionPosition(currentOperationTime);
    }

    // Pause and Resume

    /**
     * Pause an individual subscription. It'll be paused <i>temporarily</i>, which means that it can be
     * resumed later ({@link #resumeSubscription(String)}). This is useful for testing purposes when you want
     * to write events to an event store without triggering this particular subscription.
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws IllegalArgumentException If subscription is not running
     */
    public synchronized void pauseSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't running.");
        }
        messageListenerContainer.remove(internalSubscription.springSubscription);
        pausedSubscriptions.put(subscriptionId, internalSubscription);
    }

    /**
     * Resume a paused ({@link #pauseSubscription(String)}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others).
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws IllegalArgumentException If subscription is not paused
     */
    public synchronized void resumeSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }
        org.springframework.data.mongodb.core.messaging.Subscription newSubscription = messageListenerContainer.register(internalSubscription.request, Document.class);
        runningSubscriptions.put(subscriptionId, internalSubscription.changeSpringSubscriptionTo(newSubscription));

        if (!messageListenerContainer.isRunning()) {
            messageListenerContainer.start();
        }
    }

    /**
     * Check if a particular subscription is running.
     *
     * @param subscriptionId The id of the  subscription to check whether it's running or not
     * @return {@code true} if the subscription is running, {@code false} otherwise.
     */
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    /**
     * Check if a particular subscription is paused.
     *
     * @param subscriptionId The id of the  subscription to check whether it's paused or not
     * @return {@code true} if the subscription is paused, {@code false} otherwise.
     */
    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    // SmartLifecycle

    @Override
    public synchronized void start() {
        if (!shutdown) {
            messageListenerContainer.start();
            pausedSubscriptions.forEach((subscriptionId, __) -> resumeSubscription(subscriptionId));
        }
    }

    @Override
    public synchronized void stop() {
        if (!shutdown) {
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
            messageListenerContainer.stop();
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

    // Model that hold both the spring subscription and the change stream request so that we can pause the subscription
    // (by removing it and starting it again)
    private static class InternalSubscription {
        private final org.springframework.data.mongodb.core.messaging.Subscription springSubscription;
        private final ChangeStreamRequest<Document> request;

        private InternalSubscription(org.springframework.data.mongodb.core.messaging.Subscription subscription, ChangeStreamRequest<Document> request) {
            this.springSubscription = subscription;
            this.request = request;
        }

        InternalSubscription changeSpringSubscriptionTo(org.springframework.data.mongodb.core.messaging.Subscription springSubscription) {
            return new InternalSubscription(springSubscription, request);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalSubscription)) return false;
            InternalSubscription that = (InternalSubscription) o;
            return Objects.equals(springSubscription, that.springSubscription) && Objects.equals(request, that.request);
        }

        @Override
        public int hashCode() {
            return Objects.hash(springSubscription, request);
        }
    }
}