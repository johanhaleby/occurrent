/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties(prefix = "occurrent")
public class OccurrentProperties {
    private static final String DEFAULT_MONGO_EVENTS_COLLECTION = "events";

    /**
     * Event Store Configuration (see <a href="https://occurrent.org/documentation#eventstore">docs</a>)
     */
    private EventStoreProperties eventStore = new EventStoreProperties();
    /**
     * Subscription Configuration (see <a href="https://occurrent.org/documentation#subscriptions">docs</a>)
     */
    private SubscriptionProperties subscription = new SubscriptionProperties();

    /**
     * CloudEventConverter Configuration (see <a href="https://occurrent.org/documentation#cloudevent-conversion">docs</a>)
     */
    private CloudEventConverterProperties cloudEventConverter = new CloudEventConverterProperties();

    /**
     * Application Service Configuration (see <a href="https://occurrent.org/documentation#application-service">docs</a>)
     */
    private ApplicationServiceProperties applicationService = new ApplicationServiceProperties();


    public static class ApplicationServiceProperties {

        /**
         * Configure whether to enable the default retry strategy for the application service.
         * If enabled, the {@link GenericApplicationService} will use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
         * each retry, if {@link WriteConditionNotFulfilledException} is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception.
         */
        private boolean enableDefaultRetryStrategy = true;

        private boolean enabled;

        public boolean isEnableDefaultRetryStrategy() {
            return enableDefaultRetryStrategy;
        }

        public void setEnableDefaultRetryStrategy(boolean enableDefaultRetryStrategy) {
            this.enableDefaultRetryStrategy = enableDefaultRetryStrategy;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

    }

    public static class CloudEventConverterProperties {

        /**
         * Specify the source that'll be used in cloud event converter
         * <p>
         * You can regard the “source” attribute as the “stream type” or a “category” for certain streams. For example, if you’re creating a game, you may have two kinds of aggregates in your bounded context, a “game” and a “player”.
         * You can regard these as two different sources (categories). These are represented as URN’s, for example the “game” may have the source “urn:mycompany:mygame:game” and “player” may have “urn:mycompany:mygame:player”.
         * This allows, for example, subscriptions to subscribe to all events related to any player (by using a subscription filter for the source attribute).
         */
        private URI cloudEventSource;

        public URI getCloudEventSource() {
            return cloudEventSource;
        }

        public void setCloudEventSource(URI cloudEventSource) {
            this.cloudEventSource = cloudEventSource;
        }
    }

    public static class EventStoreProperties {

        /**
         * The collection where events are stored
         */
        private String collection = DEFAULT_MONGO_EVENTS_COLLECTION;

        /**
         * Choose how to represent time in the cloud events
         */
        private TimeRepresentation timeRepresentation = TimeRepresentation.DATE;

        /**
         * If the event store should be enabled (i.e. created as Spring Bean)
         * <p>
         * Typically you only want to disable this if you don't need an event store for this application,
         * typically if another application are writing events to the store, and you only want to have subscriptions
         * in this application.
         * </p>
         * <p>
         * Note that settings this to {@code false} also disables the creation of an {@link org.occurrent.application.service.blocking.ApplicationService}
         * and {@link org.occurrent.dsl.query.blocking.DomainEventQueries}.
         * </p>
         */
        private boolean enabled = true;

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }

        public TimeRepresentation getTimeRepresentation() {
            return timeRepresentation;
        }

        public void setTimeRepresentation(TimeRepresentation timeRepresentation) {
            this.timeRepresentation = timeRepresentation;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class SubscriptionProperties {
        /**
         * The collection into which subscription positions will be stored
         */
        private String collection = "subscriptions";

        /**
         * If there’s not enough history available in the MongoDB oplog to resume a subscription created from a SpringMongoSubscriptionModel, you can configure it to restart the subscription from the current time automatically.
         * This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It’s disabled by default since it might not be 100% safe
         * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store, and you have lots of writes happening to the event store.
         * It’s safe if you run the subscription in the same process as the writes to the event store if you make sure that the subscription is started before you accept writes to the event store on startup.
         */
        private boolean restartOnChangeStreamHistoryLost = true;

        /**
         * Toggles whether subscriptions should be enabled (i.e. created and instantiated as Spring Bean).
         * <p>
         * Typically you only want to disable this if you don't need subscriptions or
         * if you're subscriptions are running on another node.
         * </p>
         */
        private boolean enabled = true;

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }

        public boolean isRestartOnChangeStreamHistoryLost() {
            return restartOnChangeStreamHistoryLost;
        }

        public void setRestartOnChangeStreamHistoryLost(boolean restartOnChangeStreamHistoryLost) {
            this.restartOnChangeStreamHistoryLost = restartOnChangeStreamHistoryLost;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public EventStoreProperties getEventStore() {
        return eventStore;
    }

    public void setEventStore(EventStoreProperties eventStore) {
        this.eventStore = eventStore;
    }

    public SubscriptionProperties getSubscription() {
        return subscription;
    }

    public void setSubscription(SubscriptionProperties subscription) {
        this.subscription = subscription;
    }

    public CloudEventConverterProperties getCloudEventConverter() {
        return cloudEventConverter;
    }

    public void setCloudEventConverter(CloudEventConverterProperties cloudEventConverter) {
        this.cloudEventConverter = cloudEventConverter;
    }

    public ApplicationServiceProperties getApplicationService() {
        return applicationService;
    }

    public void setApplicationService(ApplicationServiceProperties applicationService) {
        this.applicationService = applicationService;
    }
}
