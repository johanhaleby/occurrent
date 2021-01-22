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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this subscription doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringMongoSubscriptionModel implements PositionAwareSubscriptionModel {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, org.springframework.data.mongodb.core.messaging.Subscription> subscriptions;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringMongoSubscriptionModel(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation) {
        requireNonNull(mongoTemplate, MongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");

        this.mongoOperations = mongoTemplate;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.subscriptions = new ConcurrentHashMap<>();
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate);
        this.messageListenerContainer.start();
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAtSupplier, "StartAt cannot be null");

        // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
        ChangeStreamOptionsBuilder builder = MongoCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, startAtSupplier.get());
        final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);

        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(raw, timeRepresentation)
                    .map(cloudEvent -> new PositionAwareCloudEvent(cloudEvent, new MongoResumeTokenSubscriptionPosition(resumeToken)))
                    .ifPresent(action);
        };

        ChangeStreamRequestOptions options = new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = messageListenerContainer.register(new ChangeStreamRequest<>(listener, options), Document.class);
        subscriptions.put(subscriptionId, subscription);
        return new SpringMongoSubscription(subscriptionId, subscription);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        org.springframework.data.mongodb.core.messaging.Subscription subscription = subscriptions.remove(subscriptionId);
        if (subscription != null) {
            messageListenerContainer.remove(subscription);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        subscriptions.clear();
        messageListenerContainer.stop();
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        // Note that we increase the "increment" by 1 in order to not clash with an existing event in the event store.
        // This is so that we can avoid duplicates in certain rare cases when replaying events.
        BsonTimestamp currentOperationTime = MongoCommons.getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)), 1);
        return new MongoOperationTimeSubscriptionPosition(currentOperationTime);
    }
}