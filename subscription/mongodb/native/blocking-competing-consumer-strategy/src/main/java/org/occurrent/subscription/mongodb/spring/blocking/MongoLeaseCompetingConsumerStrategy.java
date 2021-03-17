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

public class MongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {

    private final MongoCollection<BsonDocument> collection;
    private final MongoLeaseCompetingConsumerStrategySupport support;

    public static MongoLeaseCompetingConsumerStrategy withDefaults(MongoDatabase db) {
        return new MongoLeaseCompetingConsumerStrategy.Builder(db).build();
    }

    private MongoLeaseCompetingConsumerStrategy(MongoCollection<BsonDocument> collection, MongoLeaseCompetingConsumerStrategySupport support) {
        this.support = support;
        Objects.requireNonNull(collection, MongoCollection.class.getSimpleName() + " cannot be null");
        this.collection = collection;
    }

    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return support.registerCompetingConsumer(collection, subscriptionId, subscriberId);
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        support.unregisterCompetingConsumer(collection, subscriptionId, subscriberId);
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        return support.hasLock(subscriptionId, subscriberId);
    }

    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
        support.addListener(listenerConsumer);
    }

    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
        support.removeListener(listenerConsumer);
    }

    @PreDestroy
    @Override
    public void shutdown() {
        support.shutdown();
    }

    public static final class Builder {
        private final MongoCollection<BsonDocument> collection;
        private Clock clock;
        private Duration leaseTime;
        private RetryStrategy retryStrategy;


        public Builder(MongoDatabase db) {
            this(Objects.requireNonNull(db, MongoDatabase.class.getSimpleName() + " cannot be null").getCollection(DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION, BsonDocument.class));
        }

        public Builder(MongoCollection<BsonDocument> collection) {
            Objects.requireNonNull(collection, MongoCollection.class.getSimpleName() + " cannot be null");
            this.collection = collection;
        }

        public Builder clock(Clock clock) {
            Objects.requireNonNull(clock, Clock.class.getSimpleName() + " cannot be null");
            this.clock = clock;
            return this;
        }

        public Builder leaseTime(Duration leaseTime) {
            Objects.requireNonNull(leaseTime, "Lease time cannot be null");
            this.leaseTime = leaseTime;
            return this;
        }

        public Builder retryStrategy(RetryStrategy retryStrategy) {
            Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            this.retryStrategy = retryStrategy;
            return this;
        }

        public MongoLeaseCompetingConsumerStrategy build() {
            Clock clockToUse = clock == null ? Clock.systemUTC() : clock;
            Duration leaseTimeToUse = leaseTime == null ? DEFAULT_LEASE_TIME : leaseTime;
            RetryStrategy retryStrategyToUse = retryStrategy == null ? RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f) : retryStrategy;
            MongoLeaseCompetingConsumerStrategySupport support = new MongoLeaseCompetingConsumerStrategySupport(leaseTimeToUse, clockToUse, retryStrategyToUse)
                    .scheduleRefresh(c -> () -> c.accept(collection));
            return new MongoLeaseCompetingConsumerStrategy(collection, support);
        }
    }
}