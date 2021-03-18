package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport;

import javax.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import static org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION;
import static org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_LEASE_TIME;

/**
 * A (native Java driver) MongoDB {@link CompetingConsumerStrategy} that uses a lease stored in MongoDB to make sure that only one subscriber can
 * receive events for a particular subscription. A background thread is created to update the lease periodically. Use the {@link Builder} or the {@link #withDefaults(MongoDatabase)} method
 * to get started. Note that this strategy is typically used together with a {@code CompetingConsumerSubscriptionModel}.
 */
public class NativeMongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {

    private final MongoCollection<BsonDocument> collection;
    private final MongoLeaseCompetingConsumerStrategySupport support;

    /**
     * Create a new instance using the default configurations:
     * <table>
     *     <tr><td colspan="2">Lease time</td><td>20 seconds</td></tr>
     *     <tr><td colspan="2">Collection name</td><td>competing-consumer-locks</td></tr>
     *     <tr><td colspan="2">Retry Strategy</td><td>Exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when read/updating the lease</td></tr>
     *     <tr><td colspan="2">Clock</td><td>UTC</td></tr>
     * </table>
     *
     * @param db The MongoDB database to store the lease information for the subscribers.
     * @return A new instance of {@code NativeMongoLeaseCompetingConsumerStrategy}
     * @see Builder to change the default settings
     */
    public static NativeMongoLeaseCompetingConsumerStrategy withDefaults(MongoDatabase db) {
        return new NativeMongoLeaseCompetingConsumerStrategy.Builder(db).build();
    }

    private NativeMongoLeaseCompetingConsumerStrategy(MongoCollection<BsonDocument> collection, MongoLeaseCompetingConsumerStrategySupport support) {
        this.support = support;
        Objects.requireNonNull(collection, MongoCollection.class.getSimpleName() + " cannot be null");
        this.collection = collection;
    }

    /**
     * Register a new competing consumer that will be able to receive events (given that the conditions maintained by the {@code CompetingConsumerStrategy} allow for it).
     *
     * @param subscriptionId The subscription if to consume from
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the registered competing consumer has access (lock) to consume events, <code>false</code> otherwise.
     */
    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return support.registerCompetingConsumer(collection, subscriptionId, subscriberId);
    }

    /**
     * Unregister a competing consumer, it'll no longer receive events. If this competing consumer currently has lock to receive events,
     * the lock will be handed to another subscriber for the same subscription.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     */
    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        support.unregisterCompetingConsumer(collection, subscriptionId, subscriberId);
    }

    /**
     * Check whether a particular subscriber has the lock (access) to read events for the given subscription.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the subscriber has the lock, <code>false</code> otherwise.
     */
    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        return support.hasLock(subscriptionId, subscriberId);
    }

    /**
     * Add a {@link CompetingConsumerListener} to this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to add.
     */
    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
        support.addListener(listenerConsumer);
    }

    /**
     * Remove a {@link CompetingConsumerListener} from this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to remove.
     */
    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
        support.removeListener(listenerConsumer);
    }

    /**
     * Shutdown the competing consumer strategy, it'll close the background thread that updates the lease.
     */
    @PreDestroy
    @Override
    public void shutdown() {
        support.shutdown();
    }

    /**
     * A builder for creating a {@code NativeMongoLeaseCompetingConsumerStrategy} with custom settings.
     */
    public static final class Builder {
        private final MongoCollection<BsonDocument> collection;
        private Clock clock;
        private Duration leaseTime;
        private RetryStrategy retryStrategy;


        /**
         * @param db The mongodb database to use. Will assume that the collection name is {@value MongoLeaseCompetingConsumerStrategySupport#DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION}.
         */
        public Builder(MongoDatabase db) {
            this(db, DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION);
        }

        /**
         * @param db The mongodb database to use and the name of the collection that will store the lease information for subscribers.
         */
        public Builder(MongoDatabase db, String collectionName) {
            this(Objects.requireNonNull(db, MongoDatabase.class.getSimpleName() + " cannot be null").getCollection(Objects.requireNonNull(collectionName, "Collection name cannot be null"), BsonDocument.class));
        }

        /**
         * @param collection The mongodb collection to use.
         */
        public Builder(MongoCollection<BsonDocument> collection) {
            Objects.requireNonNull(collection, MongoCollection.class.getSimpleName() + " cannot be null");
            this.collection = collection;
        }

        /**
         * The clock to use when scheduling lease updates. Default is {@code UTC}.
         *
         * @param clock The clock
         * @return The same builder instance.
         */
        public Builder clock(Clock clock) {
            Objects.requireNonNull(clock, Clock.class.getSimpleName() + " cannot be null");
            this.clock = clock;
            return this;
        }

        /**
         * The maximum during a lease for a subscriber is valid. Default is 20 seconds. A lease will be prolonged automatically after {@code leaseTime / 2}.
         *
         * @param leaseTime The lease time.
         * @return The same builder instance.
         */
        public Builder leaseTime(Duration leaseTime) {
            Objects.requireNonNull(leaseTime, "Lease time cannot be null");
            this.leaseTime = leaseTime;
            return this;
        }

        /**
         * The retry strategy to use when updating the lease for a subscriber in MongoDB. Default is exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when read/updating the lease.
         *
         * @param retryStrategy The retry strategy to use.
         * @return The same builder instance.
         */
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * Build the {@code NativeMongoLeaseCompetingConsumerStrategy} with the given settings.
         *
         * @return A new instance of {@code NativeMongoLeaseCompetingConsumerStrategy}.
         */
        public NativeMongoLeaseCompetingConsumerStrategy build() {
            Clock clockToUse = clock == null ? Clock.systemUTC() : clock;
            Duration leaseTimeToUse = leaseTime == null ? DEFAULT_LEASE_TIME : leaseTime;
            RetryStrategy retryStrategyToUse = retryStrategy == null ? RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f) : retryStrategy;
            MongoLeaseCompetingConsumerStrategySupport support = new MongoLeaseCompetingConsumerStrategySupport(leaseTimeToUse, clockToUse, retryStrategyToUse)
                    .scheduleRefresh(c -> () -> c.accept(collection));
            return new NativeMongoLeaseCompetingConsumerStrategy(collection, support);
        }
    }
}