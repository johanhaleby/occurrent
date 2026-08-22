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

package org.occurrent.springboot.common;

import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.DeprecatedConfigurationProperty;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

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

    /**
     * Saga Configuration (the {@code @Saga} process manager, blocking stack only)
     */
    private SagaProperties saga = new SagaProperties();

    /**
     * Projection Configuration (the {@code @Projection} read model, both stacks)
     */
    private ProjectionProperties projection = new ProjectionProperties();


    public static class ApplicationServiceProperties {

        /**
         * Configure whether to enable the default retry strategy for the application service.
         * If enabled, the GenericApplicationService will use a retry strategy for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
         * each retry, if a write-condition-not-fulfilled exception is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception.
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

        /**
         * Truncate the cloud event time to this precision, for example {@code millis}. Use this when the event store uses
         * {@code TimeRepresentation.DATE}, which cannot store sub-millisecond precision (a common issue since
         * {@code Instant.now()} carries nanoseconds on modern JVMs). When unset and the event store
         * {@code time-representation} is {@code DATE}, the converter defaults to {@code MILLIS} so that the common case
         * works without configuration. Has no effect when left unset with {@code RFC_3339_STRING}.
         */
        private ChronoUnit timePrecision;

        public URI getCloudEventSource() {
            return cloudEventSource;
        }

        public void setCloudEventSource(URI cloudEventSource) {
            this.cloudEventSource = cloudEventSource;
        }

        public ChronoUnit getTimePrecision() {
            return timePrecision;
        }

        public void setTimePrecision(ChronoUnit timePrecision) {
            this.timePrecision = timePrecision;
        }
    }

    public static class EventStoreProperties {

        /**
         * The collection where events are stored.
         *
         * @deprecated Use {@code occurrent.event-store.mongodb.collection} instead. MongoDB is the only store this
         * ever described, and a second store needs its own key under its own store-qualified path. Setting both is
         * allowed only while they agree. This property is removed in the release after next.
         */
        @Deprecated(forRemoval = true)
        private @Nullable String collection;

        /**
         * Choose how to represent time in the cloud events.
         *
         * @deprecated Use {@code occurrent.event-store.mongodb.time-representation} instead. Whether time is a
         * MongoDB {@code Date} or an RFC 3339 string is meaningless without a MongoDB event store. Setting both is
         * allowed only while they agree. This property is removed in the release after next.
         */
        @Deprecated(forRemoval = true)
        private @Nullable TimeRepresentation timeRepresentation;

        /**
         * MongoDB-specific event-store configuration.
         */
        private MongoProperties mongodb = new MongoProperties();

        /**
         * The event-store capabilities to enable.
         * <p>
         * Defaults to stream-based event sourcing. Add {@link EventStoreCapability#DCB}
         * to enable Dynamic Consistency Boundary infrastructure and APIs.
         */
        private Set<EventStoreCapability> capabilities = Set.of(STREAM);

        /**
         * Stream event-store configuration.
         */
        private StreamProperties stream = new StreamProperties();

        /**
         * If the event store should be enabled (i.e. created as Spring Bean)
         * <p>
         * Typically you only want to disable this if you don't need an event store for this application,
         * typically if another application are writing events to the store, and you only want to have subscriptions
         * in this application.
         * </p>
         * <p>
         * Note that settings this to {@code false} also disables the creation of an ApplicationService
         * and a DomainEventQueries instance.
         * </p>
         */
        private boolean enabled = true;

        // On the getter rather than the field, which is where Spring Boot's configuration-property processor reads
        // it, so the generated metadata carries the replacement and an IDE can offer it. Delegates to
        // resolveCollection() rather than returning the raw field, so a caller compiled against the released
        // non-null-by-default getter keeps seeing a resolved value instead of null once only the new key is set.
        @DeprecatedConfigurationProperty(replacement = "occurrent.event-store.mongodb.collection", reason = "MongoDB is the only store this ever described, and the key now says so.")
        @Deprecated(forRemoval = true)
        public String getCollection() {
            return resolveCollection();
        }

        public void setCollection(@Nullable String collection) {
            this.collection = collection;
        }

        // Delegates to resolveTimeRepresentation() for the same reason getCollection() does.
        @DeprecatedConfigurationProperty(replacement = "occurrent.event-store.mongodb.time-representation", reason = "MongoDB is the only store this ever described, and the key now says so.")
        @Deprecated(forRemoval = true)
        public TimeRepresentation getTimeRepresentation() {
            return resolveTimeRepresentation();
        }

        public void setTimeRepresentation(@Nullable TimeRepresentation timeRepresentation) {
            this.timeRepresentation = timeRepresentation;
        }

        public MongoProperties getMongodb() {
            return mongodb;
        }

        public void setMongodb(MongoProperties mongodb) {
            this.mongodb = mongodb;
        }

        /**
         * The collection to use, resolving the deprecated {@code occurrent.event-store.collection} when
         * {@code occurrent.event-store.mongodb.collection} is not set.
         *
         * @return The resolved collection name, {@code "events"} when neither property is set.
         * @throws IllegalStateException if both properties are set and contradict each other
         */
        public String resolveCollection() {
            String mongodbCollection = mongodb.getCollection();
            if (collection == null) {
                return mongodbCollection == null ? DEFAULT_MONGO_EVENTS_COLLECTION : mongodbCollection;
            } else if (mongodbCollection != null && !mongodbCollection.equals(collection)) {
                throw new IllegalStateException(
                        "occurrent.event-store.mongodb.collection is \"" + mongodbCollection + "\" but the deprecated occurrent.event-store.collection "
                                + "is \"" + collection + "\". Remove occurrent.event-store.collection, and check for it in environment variables and "
                                + "external configuration as well as your configuration files.");
            }
            return collection;
        }

        /**
         * The time representation to use, resolving the deprecated {@code occurrent.event-store.time-representation}
         * when {@code occurrent.event-store.mongodb.time-representation} is not set.
         *
         * @return The resolved time representation, {@link TimeRepresentation#DATE} when neither property is set.
         * @throws IllegalStateException if both properties are set and contradict each other
         */
        public TimeRepresentation resolveTimeRepresentation() {
            TimeRepresentation mongodbTimeRepresentation = mongodb.getTimeRepresentation();
            if (timeRepresentation == null) {
                return mongodbTimeRepresentation == null ? TimeRepresentation.DATE : mongodbTimeRepresentation;
            } else if (mongodbTimeRepresentation != null && mongodbTimeRepresentation != timeRepresentation) {
                throw new IllegalStateException(
                        "occurrent.event-store.mongodb.time-representation is " + mongodbTimeRepresentation + " but the deprecated "
                                + "occurrent.event-store.time-representation is " + timeRepresentation + ". Remove "
                                + "occurrent.event-store.time-representation, and check for it in environment variables and external configuration "
                                + "as well as your configuration files.");
            }
            return timeRepresentation;
        }

        public Set<EventStoreCapability> getCapabilities() {
            return capabilities;
        }

        public void setCapabilities(Set<EventStoreCapability> capabilities) {
            if (capabilities == null || capabilities.isEmpty()) {
                throw new IllegalArgumentException("occurrent.event-store.capabilities must contain at least one capability");
            }
            this.capabilities = Set.copyOf(capabilities);
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public StreamProperties getStream() {
            return stream;
        }

        public void setStream(StreamProperties stream) {
            this.stream = stream;
        }

        /**
         * MongoDB-specific event-store configuration, replacing the store-neutral-named
         * {@code occurrent.event-store.collection} and {@code occurrent.event-store.time-representation}.
         */
        public static class MongoProperties {

            /**
             * The collection where events are stored. Defaults to {@code "events"}.
             */
            private @Nullable String collection;

            /**
             * Choose how to represent time in the cloud events. Defaults to {@link TimeRepresentation#DATE}.
             */
            private @Nullable TimeRepresentation timeRepresentation;

            public @Nullable String getCollection() {
                return collection;
            }

            public void setCollection(@Nullable String collection) {
                this.collection = collection;
            }

            public @Nullable TimeRepresentation getTimeRepresentation() {
                return timeRepresentation;
            }

            public void setTimeRepresentation(@Nullable TimeRepresentation timeRepresentation) {
                this.timeRepresentation = timeRepresentation;
            }
        }
    }

    public static class StreamProperties {

        /**
         * Whether stream-written events carry a global, monotonic position. On by default when unset. Set to
         * {@code false} to opt a STREAM-only store out of writing position. Set to {@code true} to enable it
         * explicitly, which keeps it on even for an existing store whose events have not been backfilled. When unset,
         * the store may turn position off at startup if it finds such a collection. {@link EventStoreCapability#DCB}
         * always writes position regardless of this setting, and a combined STREAM+DCB store always writes position.
         */
        private Boolean position;

        public Boolean getPosition() {
            return position;
        }

        public void setPosition(Boolean position) {
            this.position = position;
        }
    }

    public static class SubscriptionProperties {
        /**
         * The collection into which checkpoints will be stored.
         *
         * @deprecated Use {@code occurrent.subscription.mongodb.collection} instead. MongoDB is the only store this
         * ever described, and a second store needs its own key under its own store-qualified path. Setting both is
         * allowed only while they agree. This property is removed in the release after next.
         */
        @Deprecated(forRemoval = true)
        private @Nullable String collection;

        /**
         * If there’s not enough history available in the MongoDB oplog to resume a subscription created from a SpringMongoSubscriptionModel, you can configure it to restart the subscription from the current time automatically.
         * This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It’s enabled by default even though it might not be 100% safe
         * (meaning that you can miss some events when the subscription is restarted). It’s not 100% safe if you run subscriptions in a different process than the event store, and you have lots of writes happening to the event store.
         * It’s safe if you run the subscription in the same process as the writes to the event store if you make sure that the subscription is started before you accept writes to the event store on startup.
         *
         * @deprecated Use {@code occurrent.subscription.mongodb.restart-on-change-stream-history-lost} instead. A
         * change stream is a MongoDB concept, meaningless without a MongoDB subscription model. Setting both is
         * allowed only while they agree. This property is removed in the release after next.
         */
        @Deprecated(forRemoval = true)
        private @Nullable Boolean restartOnChangeStreamHistoryLost;

        /**
         * MongoDB-specific subscription configuration.
         */
        private MongoProperties mongodb = new MongoProperties();

        /**
         * How much of the subscription machinery to create and start, see {@link SubscriptionMode}. Defaults to
         * {@link SubscriptionMode#AUTO}, which creates subscriptions and starts them.
         */
        private @Nullable SubscriptionMode mode;

        /**
         * Whether subscriptions are created as Spring beans at all.
         *
         * @deprecated Use {@code occurrent.subscription.mode} instead, where {@code false} became
         * {@link SubscriptionMode#DISABLED} and {@code true} became {@link SubscriptionMode#AUTO}. Setting both is
         * allowed only while they agree, so a rewritten configuration file and a leftover environment variable do not
         * fail the application. This property is removed in the release after next.
         */
        @Deprecated(forRemoval = true)
        private @Nullable Boolean enabled;

        /**
         * Whether a durable subscription that asks for the model default, with no checkpoint stored and a wrapped
         * subscription model whose {@code globalCheckpoint()} cannot answer, starts anyway instead of being refused.
         * The MongoDB subscription models cannot answer when the server refuses the {@code hostInfo} command, which
         * shared MongoDB Atlas clusters do. Starting anyway means no start position is recorded before the first
         * delivery, so a crash before the first checkpoint is saved starts over from wherever the feed has reached
         * by then, and an event whose delivery failed before the crash is not redelivered. The default is
         * {@code false}, which refuses such a subscription when it is created.
         */
        private boolean startWhenNoStartPositionCanBeRecorded = false;

        /**
         * Tuning for the catch-up-then-live handover used by a push-fed projection's bootstrap.
         */
        private CatchupThenLiveProperties catchupThenLive = new CatchupThenLiveProperties();

        /**
         * Competing-consumer (leader-election) configuration for subscriptions.
         */
        private SubscriptionCompetingConsumerProperties competingConsumer = new SubscriptionCompetingConsumerProperties();

        // On the getter rather than the field, which is where Spring Boot's configuration-property processor reads
        // it, so the generated metadata carries the replacement and an IDE can offer it. Delegates to
        // resolveCollection() rather than returning the raw field, so a caller compiled against the released
        // non-null-by-default getter keeps seeing a resolved value instead of null once only the new key is set.
        @DeprecatedConfigurationProperty(replacement = "occurrent.subscription.mongodb.collection", reason = "MongoDB is the only store this ever described, and the key now says so.")
        @Deprecated(forRemoval = true)
        public String getCollection() {
            return resolveCollection();
        }

        public void setCollection(@Nullable String collection) {
            this.collection = collection;
        }

        public boolean isStartWhenNoStartPositionCanBeRecorded() {
            return startWhenNoStartPositionCanBeRecorded;
        }

        public void setStartWhenNoStartPositionCanBeRecorded(boolean startWhenNoStartPositionCanBeRecorded) {
            this.startWhenNoStartPositionCanBeRecorded = startWhenNoStartPositionCanBeRecorded;
        }

        // The released getter/setter shape (isX(): boolean / setX(boolean)) is kept, unlike the two getters above,
        // because this field's original type was already the primitive boolean rather than a String or an enum, and
        // isX() delegating to the resolver preserves both the method name and the non-null primitive return a
        // pre-existing caller compiled against. setX(boolean) is safe to keep primitive too: Spring's relaxed binder
        // only ever calls a setter for a key that is actually present, so "unset" is represented by never calling
        // it, leaving the field at its unset null default, rather than by passing it a null argument.
        @DeprecatedConfigurationProperty(replacement = "occurrent.subscription.mongodb.restart-on-change-stream-history-lost", reason = "A change stream is a MongoDB concept, and the key now says so.")
        @Deprecated(forRemoval = true)
        public boolean isRestartOnChangeStreamHistoryLost() {
            return resolveRestartOnChangeStreamHistoryLost();
        }

        public void setRestartOnChangeStreamHistoryLost(boolean restartOnChangeStreamHistoryLost) {
            this.restartOnChangeStreamHistoryLost = restartOnChangeStreamHistoryLost;
        }

        public MongoProperties getMongodb() {
            return mongodb;
        }

        public void setMongodb(MongoProperties mongodb) {
            this.mongodb = mongodb;
        }

        /**
         * The collection to use, resolving the deprecated {@code occurrent.subscription.collection} when
         * {@code occurrent.subscription.mongodb.collection} is not set.
         *
         * @return The resolved collection name, {@code "subscriptions"} when neither property is set.
         * @throws IllegalStateException if both properties are set and contradict each other
         */
        public String resolveCollection() {
            String mongodbCollection = mongodb.getCollection();
            if (collection == null) {
                return mongodbCollection == null ? "subscriptions" : mongodbCollection;
            } else if (mongodbCollection != null && !mongodbCollection.equals(collection)) {
                throw new IllegalStateException(
                        "occurrent.subscription.mongodb.collection is \"" + mongodbCollection + "\" but the deprecated occurrent.subscription.collection"
                                + " is \"" + collection + "\". Remove occurrent.subscription.collection, and check for it in environment variables and "
                                + "external configuration as well as your configuration files.");
            }
            return collection;
        }

        /**
         * Whether to restart the subscription from the current time when there isn't enough oplog history to resume
         * it, resolving the deprecated {@code occurrent.subscription.restart-on-change-stream-history-lost} when
         * {@code occurrent.subscription.mongodb.restart-on-change-stream-history-lost} is not set.
         *
         * @return The resolved value, {@code true} when neither property is set.
         * @throws IllegalStateException if both properties are set and contradict each other
         */
        public boolean resolveRestartOnChangeStreamHistoryLost() {
            Boolean mongodbValue = mongodb.getRestartOnChangeStreamHistoryLost();
            if (restartOnChangeStreamHistoryLost == null) {
                return mongodbValue == null || mongodbValue;
            } else if (mongodbValue != null && !mongodbValue.equals(restartOnChangeStreamHistoryLost)) {
                throw new IllegalStateException(
                        "occurrent.subscription.mongodb.restart-on-change-stream-history-lost is " + mongodbValue + " but the deprecated "
                                + "occurrent.subscription.restart-on-change-stream-history-lost is " + restartOnChangeStreamHistoryLost + ". Remove "
                                + "occurrent.subscription.restart-on-change-stream-history-lost, and check for it in environment variables and external "
                                + "configuration as well as your configuration files.");
            }
            return restartOnChangeStreamHistoryLost;
        }

        // On the getter rather than the field, which is where Spring Boot's configuration-property processor reads it,
        // so the generated metadata carries the replacement and an IDE can offer it.
        @DeprecatedConfigurationProperty(replacement = "occurrent.subscription.mode", reason = "false became disabled and true became auto, and mode can also express manual.")
        @Deprecated(forRemoval = true)
        public @Nullable Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(@Nullable Boolean enabled) {
            this.enabled = enabled;
        }

        public @Nullable SubscriptionMode getMode() {
            return mode;
        }

        public void setMode(@Nullable SubscriptionMode mode) {
            this.mode = mode;
        }

        /**
         * The mode to actually use, resolving the deprecated {@code enabled} property when {@code mode} is not set.
         *
         * @return The resolved mode, {@link SubscriptionMode#AUTO} when neither property is set.
         * @throws IllegalStateException if both properties are set and contradict each other
         */
        public SubscriptionMode resolveMode() {
            return SubscriptionMode.resolve(mode, enabled);
        }

        public CatchupThenLiveProperties getCatchupThenLive() {
            return catchupThenLive;
        }

        public void setCatchupThenLive(CatchupThenLiveProperties catchupThenLive) {
            this.catchupThenLive = catchupThenLive;
        }

        public SubscriptionCompetingConsumerProperties getCompetingConsumer() {
            return competingConsumer;
        }

        public void setCompetingConsumer(SubscriptionCompetingConsumerProperties competingConsumer) {
            this.competingConsumer = competingConsumer;
        }

        /**
         * MongoDB-specific subscription configuration, replacing the store-neutral-named
         * {@code occurrent.subscription.collection} and {@code occurrent.subscription.restart-on-change-stream-history-lost}.
         */
        public static class MongoProperties {

            /**
             * The collection into which checkpoints will be stored. Defaults to {@code "subscriptions"}.
             */
            private @Nullable String collection;

            /**
             * If there’s not enough history available in the MongoDB oplog to resume a subscription created from a
             * SpringMongoSubscriptionModel, you can configure it to restart the subscription from the current time
             * automatically. Defaults to {@code true}.
             */
            private @Nullable Boolean restartOnChangeStreamHistoryLost;

            public @Nullable String getCollection() {
                return collection;
            }

            public void setCollection(@Nullable String collection) {
                this.collection = collection;
            }

            public @Nullable Boolean getRestartOnChangeStreamHistoryLost() {
                return restartOnChangeStreamHistoryLost;
            }

            public void setRestartOnChangeStreamHistoryLost(@Nullable Boolean restartOnChangeStreamHistoryLost) {
                this.restartOnChangeStreamHistoryLost = restartOnChangeStreamHistoryLost;
            }
        }

        /**
         * Competing-consumer (leader-election) configuration for subscriptions.
         */
        public static class SubscriptionCompetingConsumerProperties {

            /**
             * Whether a checkpoint write carries the competing-consumer lease version, so a write from a node that has
             * already lost its lease is refused instead of moving the checkpoint backwards. Enabled by default, and it
             * applies only where a competing-consumer strategy exists at all.
             * <p>
             * This requires a {@code CheckpointStorage} that evaluates write conditions, and the application refuses to
             * start when the one it wires does not. Set this to {@code false} to write every checkpoint
             * unconditionally, which is what a storage that only supports {@code any()} can do. A node that has lost
             * its lease can then still move a checkpoint backwards, and the events between the two positions are
             * delivered again.
             */
            private boolean fenceCheckpoints = true;

            public boolean isFenceCheckpoints() {
                return fenceCheckpoints;
            }

            public void setFenceCheckpoints(boolean fenceCheckpoints) {
                this.fenceCheckpoints = fenceCheckpoints;
            }
        }

        /**
         * Tunes the catch-up-then-live subscription model that a {@code @Projection(source = PUSH)} is bootstrapped
         * with: it replays history from the event store while buffering what the push feed delivers, then drains the
         * buffer and goes live.
         * <p>
         * This applies to a projection fed by a {@code PushSubscriptionModel}. It does <strong>not</strong> reach a
         * projection fed by a {@code DomainEventFeed}, because your application declares that bean itself, so you tune
         * its catch-up by passing the options to its constructor instead.
         * <p>
         * Both values are unset by default, meaning the built-in defaults apply. Setting one leaves the other at its
         * default.
         */
        public static class CatchupThenLiveProperties {

            /**
             * How many recently delivered event ids to retain so the replay-to-live overlap is de-duplicated exactly.
             * Defaults to 10000. Beyond this window the at-least-once contract applies, so an idempotent fold absorbs a
             * duplicate. Raise it only if a replay overlaps more live events than that and duplicate delivery is
             * expensive for the read model.
             */
            private Integer dedupCacheSize;

            /**
             * A fail-loud ceiling on events buffered from the push feed while the replay runs, not a throttle. Defaults
             * to 100000. Reaching it means the replay is not keeping up with the feed at all, so the overflow is
             * reported rather than events being dropped or the buffer growing without bound. The blocking stack throws
             * from the failing feed call, the reactor stack signals the error on that event's returned Mono. Raise it
             * for a large history behind a busy feed.
             */
            private Integer maxBufferedEvents;

            public Integer getDedupCacheSize() {
                return dedupCacheSize;
            }

            public void setDedupCacheSize(Integer dedupCacheSize) {
                if (dedupCacheSize != null && dedupCacheSize <= 0) {
                    throw new IllegalArgumentException("occurrent.subscription.catchup-then-live.dedup-cache-size must be greater than zero");
                }
                this.dedupCacheSize = dedupCacheSize;
            }

            public Integer getMaxBufferedEvents() {
                return maxBufferedEvents;
            }

            public void setMaxBufferedEvents(Integer maxBufferedEvents) {
                if (maxBufferedEvents != null && maxBufferedEvents <= 0) {
                    throw new IllegalArgumentException("occurrent.subscription.catchup-then-live.max-buffered-events must be greater than zero");
                }
                this.maxBufferedEvents = maxBufferedEvents;
            }
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

    public SagaProperties getSaga() {
        return saga;
    }

    public void setSaga(SagaProperties saga) {
        this.saga = saga;
    }

    public ProjectionProperties getProjection() {
        return projection;
    }

    public void setProjection(ProjectionProperties projection) {
        this.projection = projection;
    }

    public static class SagaProperties {

        /**
         * How often a saga's timer poller queries its state store for due timeouts. Defaults to 15 seconds, matching
         * {@code SagaRunnerConfig.defaults()} and JobRunr's default. The interval only bounds how late a due timer
         * fires, and saga timeouts run at a minutes-to-days timescale, so a shorter interval mostly adds empty queries.
         * Lower it only when you rely on short timeouts firing promptly.
         */
        private Duration timerPollInterval = Duration.ofSeconds(15);

        /**
         * Competing-consumer (leader-election) configuration for the saga timer poller.
         */
        private CompetingConsumerProperties competingConsumer = new CompetingConsumerProperties();

        public Duration getTimerPollInterval() {
            return timerPollInterval;
        }

        public void setTimerPollInterval(Duration timerPollInterval) {
            this.timerPollInterval = timerPollInterval;
        }

        public CompetingConsumerProperties getCompetingConsumer() {
            return competingConsumer;
        }

        public void setCompetingConsumer(CompetingConsumerProperties competingConsumer) {
            this.competingConsumer = competingConsumer;
        }

        public static class CompetingConsumerProperties {

            /**
             * Whether to gate the saga timer poller with the shared competing-consumer lease so only one instance polls
             * for due timers in a multi-instance deployment, mirroring the competing-consumer subscription model.
             * Enabled by default. When disabled (or when no competing-consumer strategy is available, for example with
             * subscriptions disabled) every instance runs its own poller, which stays correct but multiplies the query
             * load against the state store.
             */
            private boolean enabled = true;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }
        }
    }

    public static class ProjectionProperties {

        /**
         * Configuration for the {@code AppliedAppendStore} bean the starter auto-configures when the application
         * declares none, see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>.
         * A projection records into it either directly, through {@code AppliedAppendStore.recordApplied(..)}, or
         * automatically through the {@code @Projection(recordAppliedAppends = true)} opt-in.
         */
        private AppliedAppendProperties appliedAppend = new AppliedAppendProperties();

        public AppliedAppendProperties getAppliedAppend() {
            return appliedAppend;
        }

        public void setAppliedAppend(AppliedAppendProperties appliedAppend) {
            this.appliedAppend = appliedAppend;
        }

        public static class AppliedAppendProperties {

            /**
             * The collection the Mongo starter's zero-config {@code AppliedAppendStore} records applied appends
             * into. One document per (projection id, append id) pair.
             */
            private String collection = "appliedAppends";

            /**
             * How long a recorded append is kept before a MongoDB TTL index evicts it. Storage housekeeping only.
             * A wait for an evicted append times out rather than answering wrong, which is the safe direction.
             * Defaults to 7 days, well past the seconds-to-minutes scale a wait actually runs on, leaving a wide margin for
             * debugging a stuck wait before its record disappears.
             */
            private Duration retention = Duration.ofDays(7);

            /**
             * How many times the Mongo-backed {@code AppliedAppendStore} calls MongoDB for one read or one write
             * before it gives up and fails the caller. This counts attempts and does not limit how long they take.
             * A call that fails at once is retried on the store's 100 ms to 2 s backoff, so the default of 10 takes
             * about 11 seconds, while a call to a server that is not answering spends the driver's own server
             * selection timeout, 30 seconds by default, on each of the 10. Set a timeout on the MongoDB client
             * when the wall clock is what matters, since nothing here can limit it.
             * <p>
             * A projection that records applied appends calls the store on the thread that delivers its events, so
             * this is also how long an unreachable store holds that delivery up. Reaching the limit fails the call,
             * and that failure comes out of the projection's own event handling, where the subscription treats it
             * like any other failing handler. The read model is updated before the recording runs, so a redelivery
             * of that event applies it again. Raising the limit rides out a longer outage and holds deliveries up
             * for longer, and 1 means one attempt and no retry at all.
             * <p>
             * {@code AppliedAppendStore.waitUntilApplied(..)} does not fail when this limit is reached. A read that
             * has run out of attempts counts as not applied yet, so the wait goes on polling until its own timeout.
             * <p>
             * Cannot exceed {@link #MAX_ATTEMPTS_CEILING}, which is where the Mongo stores stop a policy that never
             * stops on its own. A larger number here would be accepted and then not happen, so it is rejected
             * instead of being quietly reduced to the ceiling.
             */
            private int maxAttempts = 10;

            /**
             * The largest {@code maxAttempts} an application can ask for, matching the ceiling both Mongo stores
             * enforce. Two orders of magnitude above the default, so it bounds a mistake rather than a choice.
             */
            public static final int MAX_ATTEMPTS_CEILING = 1000;

            /**
             * How {@code AppliedAppendStore.waitUntilApplied(..)} paces its polls.
             */
            private WaitBackoffProperties waitBackoff = new WaitBackoffProperties();

            /**
             * How the {@code @Projection(recordAppliedAppends = true)} registrars pace the scheduled poll that
             * notices a replay whose deliveries are all filtered out server-side, where no delivery ever reaches the
             * recording wrapper to notice the replay itself
             * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
             * decision 7). A replay entirely between two ticks, delivering nothing this projection handles, is
             * missed by this poll too, an accepted residual decision 7 documents rather than closes. {@code max} is
             * how sparse this poll's own sampling ever becomes. Unrelated to {@link #waitBackoff}, which paces a
             * caller's wait for an append to show up, not this poll.
             */
            private ReplayPollProperties replayPoll = new ReplayPollProperties();

            public String getCollection() {
                return collection;
            }

            public void setCollection(String collection) {
                this.collection = collection;
            }

            public Duration getRetention() {
                return retention;
            }

            public void setRetention(Duration retention) {
                this.retention = retention;
            }

            public int getMaxAttempts() {
                return maxAttempts;
            }

            public void setMaxAttempts(int maxAttempts) {
                if (maxAttempts < 1) {
                    throw new IllegalArgumentException("occurrent.projection.applied-append.max-attempts must be at least 1, a store that is never called cannot record or read anything");
                }
                if (maxAttempts > MAX_ATTEMPTS_CEILING) {
                    throw new IllegalArgumentException("occurrent.projection.applied-append.max-attempts cannot exceed " + MAX_ATTEMPTS_CEILING + ", which is where the store stops a retry that never stops on its own, so " + maxAttempts + " would be accepted and then not happen");
                }
                this.maxAttempts = maxAttempts;
            }

            public WaitBackoffProperties getWaitBackoff() {
                return waitBackoff;
            }

            public void setWaitBackoff(WaitBackoffProperties waitBackoff) {
                this.waitBackoff = waitBackoff;
            }

            public ReplayPollProperties getReplayPoll() {
                return replayPoll;
            }

            public void setReplayPoll(ReplayPollProperties replayPoll) {
                this.replayPoll = replayPoll;
            }

            public static class WaitBackoffProperties {

                /**
                 * The interval before the first re-check of whether the append has been applied. Kept short so a
                 * projection that has already applied it answers immediately.
                 */
                private Duration initial = Duration.ofMillis(25);

                /**
                 * The longest the interval grows to. A wait that has been running for a while is polled at this
                 * pace rather than at {@code initial}, which is what keeps a slow-to-apply projection from being
                 * hammered by every waiting caller.
                 */
                private Duration max = Duration.ofMillis(250);

                /**
                 * What the interval is multiplied by after each poll that found the append not yet applied.
                 */
                private double multiplier = 2.0;

                public Duration getInitial() {
                    return initial;
                }

                public void setInitial(Duration initial) {
                    this.initial = initial;
                }

                public Duration getMax() {
                    return max;
                }

                public void setMax(Duration max) {
                    this.max = max;
                }

                public double getMultiplier() {
                    return multiplier;
                }

                public void setMultiplier(double multiplier) {
                    this.multiplier = multiplier;
                }
            }

            public static class ReplayPollProperties {

                /**
                 * The poll interval for a projection that has just registered, or was just seen replaying. Kept
                 * short so a replay whose deliveries are all filtered out is still noticed quickly.
                 */
                private Duration initial = Duration.ofMillis(200);

                /**
                 * The longest the interval grows to, for a projection that has been live for a while.
                 */
                private Duration max = Duration.ofSeconds(5);

                /**
                 * What the interval is multiplied by after each poll that found the projection live.
                 */
                private double multiplier = 2.0;

                public Duration getInitial() {
                    return initial;
                }

                public void setInitial(Duration initial) {
                    this.initial = initial;
                }

                public Duration getMax() {
                    return max;
                }

                public void setMax(Duration max) {
                    this.max = max;
                }

                public double getMultiplier() {
                    return multiplier;
                }

                public void setMultiplier(double multiplier) {
                    this.multiplier = multiplier;
                }
            }
        }
    }
}
