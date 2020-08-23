/*
 * Copyright 2020 Johan Haleby
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

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.occurrent.subscription.api.reactor.PositionAwareReactorSubscription;
import org.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoDBCommons;
import org.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CloudEventWithSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;

import static java.util.Objects.requireNonNull;

/**
 * This is a subscription that uses project reactor and Spring to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc.
 */
public class SpringReactorSubscriptionForMongoDB implements PositionAwareReactorSubscription {

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final EventFormat cloudEventSerializer;

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongo              The {@link ReactiveMongoOperations} instance to use when reading events from the event store
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringReactorSubscriptionForMongoDB(ReactiveMongoOperations mongo, String eventCollection, TimeRepresentation timeRepresentation) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Override
    public Flux<CloudEventWithSubscriptionPosition> subscribe(SubscriptionFilter filter, StartAt startAt) {
        // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
        ChangeStreamOptionsBuilder builder = MongoDBCommons.applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, startAt);
        final ChangeStreamOptions changeStreamOptions = ApplyFilterToChangeStreamOptionsBuilder.applyFilter(timeRepresentation, filter, builder);
        Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
        return changeStream
                .flatMap(changeEvent ->
                        MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw(), timeRepresentation)
                                .map(cloudEvent -> new CloudEventWithSubscriptionPosition(cloudEvent, new MongoDBResumeTokenBasedSubscriptionPosition(requireNonNull(changeEvent.getResumeToken()).asDocument())))
                                .map(Mono::just)
                                .orElse(Mono.empty()));
    }

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        return mongo.executeCommand(new Document("hostInfo", 1))
                .map(MongoDBCommons::getServerOperationTime)
                .map(MongoDBOperationTimeBasedSubscriptionPosition::new);
    }
}