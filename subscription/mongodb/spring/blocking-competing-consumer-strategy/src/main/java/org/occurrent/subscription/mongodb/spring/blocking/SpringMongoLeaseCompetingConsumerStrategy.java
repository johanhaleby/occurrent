package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport;
import org.springframework.data.mongodb.core.MongoOperations;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_LEASE_TIME;

/**
 * A (Spring) MongoDB {@link CompetingConsumerStrategy} that uses a lease stored in MongoDB to make sure that only one subscriber can
 * receive events for a particular subscription. A background thread is created to update the lease periodically. Use the {@link Builder} or the {@link #withDefaults(MongoOperations)} method
 * to get started. Note that this strategy is typically used together with a {@code CompetingConsumerSubscriptionModel}.
 */
public class SpringMongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {

    private final MongoOperations mongoOperations;
    private final MongoLeaseCompetingConsumerStrategySupport support;
    private final String collectionName;

    /**
     * Create a new instance using the default configurations:
     * <table>
     *     <tr><td colspan="2">Lease time</td><td>20 seconds</td></tr>
     *     <tr><td colspan="2">Collection name</td><td>competing-consumer-locks</td></tr>
     *     <tr><td colspan="2">Retry Strategy</td><td>Exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when read/updating the lease</td></tr>
     *     <tr><td colspan="2">Clock</td><td>UTC</td></tr>
     * </table>
     *
     * @param mongoOperations The Spring {@link MongoOperations} instance that should be used to store the lease information for the subscribers.
     * @return A new instance of {@code SpringMongoLeaseCompetingConsumerStrategy}
     * @see Builder to change the default settings
     */
    public static SpringMongoLeaseCompetingConsumerStrategy withDefaults(MongoOperations mongoOperations) {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoOperations).build();
    }

    private SpringMongoLeaseCompetingConsumerStrategy(MongoOperations mongoOperations, String collectionName, MongoLeaseCompetingConsumerStrategySupport support) {
        Objects.requireNonNull(mongoOperations, MongoOperations.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(collectionName, "Collection name cannot be null");
        Objects.requireNonNull(support, MongoLeaseCompetingConsumerStrategySupport.class.getSimpleName() + " cannot be null");
        this.mongoOperations = mongoOperations;
        this.collectionName = collectionName;
        this.support = support;
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
        return withCompetingConsumerLocksCollectionReturn(collection -> support.registerCompetingConsumer(collection, subscriptionId, subscriberId));
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
        withCompetingConsumerLocksCollectionDo(collection -> support.unregisterCompetingConsumer(collection, subscriptionId, subscriberId));
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

    private void withCompetingConsumerLocksCollectionDo(Consumer<MongoCollection<BsonDocument>> fn) {
        withCompetingConsumerLocksCollectionReturn(collection -> {
            fn.accept(collection);
            return null;
        });
    }

    private <T> T withCompetingConsumerLocksCollectionReturn(Function<MongoCollection<BsonDocument>, T> fn) {
        return staticallyWithCompetingConsumerLocksCollectionReturn(mongoOperations, collectionName, fn);
    }

    private static <T> T staticallyWithCompetingConsumerLocksCollectionReturn(MongoOperations mongoOperations, String collectionName, Function<MongoCollection<BsonDocument>, T> fn) {
        return mongoOperations.execute(db -> {
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            return fn.apply(collection);
        });
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
     * A builder for creating a {@code SpringMongoLeaseCompetingConsumerStrategy} with custom settings.
     */
    public static final class Builder {
        private final MongoOperations mongoOperations;
        private Clock clock;
        private Duration leaseTime;
        private String collectionName;
        private RetryStrategy retryStrategy;

        /**
         * Create a new builder with the given {@link MongoOperations} instance.
         *
         * @param mongoOperations The Spring {@link MongoOperations} instance that should be used to store the lease information for the subscribers.
         */
        public Builder(MongoOperations mongoOperations) {
            Objects.requireNonNull(mongoOperations, MongoOperations.class.getSimpleName() + " cannot be null");
            this.mongoOperations = mongoOperations;
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
         * @param collectionName The mongodb collection to use.
         */
        public Builder collectionName(String collectionName) {
            Objects.requireNonNull(collectionName, "Collection name cannot be null");
            String collectionNameToUse = collectionName.trim();
            if (collectionNameToUse.equals("")) {
                throw new IllegalArgumentException("Collection name cannot be empty");
            }
            this.collectionName = collectionNameToUse;

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
         * Build the {@code SpringMongoLeaseCompetingConsumerStrategy} with the given settings.
         *
         * @return A new instance of {@code SpringMongoLeaseCompetingConsumerStrategy}.
         */
        public SpringMongoLeaseCompetingConsumerStrategy build() {
            Clock clockToUse = clock == null ? Clock.systemUTC() : clock;
            Duration leaseTimeToUse = leaseTime == null ? DEFAULT_LEASE_TIME : leaseTime;
            String collectionNameToUse = collectionName == null ? DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION : collectionName;
            RetryStrategy retryStrategyToUse = retryStrategy == null ? RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f) : retryStrategy;
            MongoLeaseCompetingConsumerStrategySupport support = new MongoLeaseCompetingConsumerStrategySupport(leaseTimeToUse, clockToUse, retryStrategyToUse)
                    .scheduleRefresh(consumer ->
                            () -> staticallyWithCompetingConsumerLocksCollectionReturn(mongoOperations, collectionNameToUse, collection -> {
                                consumer.accept(collection);
                                return null;
                            }));
            return new SpringMongoLeaseCompetingConsumerStrategy(mongoOperations, collectionNameToUse, support);
        }
    }
}