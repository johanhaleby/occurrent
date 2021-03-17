package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport;
import org.springframework.data.mongodb.core.MongoOperations;

import javax.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION;
import static org.occurrent.subscription.mongodb.spring.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.DEFAULT_LEASE_TIME;

public class SpringMongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {

    private final MongoOperations mongoOperations;
    private final MongoLeaseCompetingConsumerStrategySupport support;
    private final String collectionName;

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

    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return withCompetingConsumerLocksCollectionReturn(collection -> support.registerCompetingConsumer(collection, subscriptionId, subscriberId));
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        withCompetingConsumerLocksCollectionDo(collection -> support.unregisterCompetingConsumer(collection, subscriptionId, subscriberId));
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

    @PreDestroy
    @Override
    public void shutdown() {
        support.shutdown();
    }

    public static final class Builder {
        private final MongoOperations mongoOperations;
        private Clock clock;
        private Duration leaseTime;
        private String collectionName;
        private RetryStrategy retryStrategy;

        public Builder(MongoOperations mongoOperations) {
            Objects.requireNonNull(mongoOperations, MongoOperations.class.getSimpleName() + " cannot be null");
            this.mongoOperations = mongoOperations;
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

        public Builder collectionName(String collectionName) {
            Objects.requireNonNull(collectionName, "Collection name cannot be null");
            String collectionNameToUse = collectionName.trim();
            if (collectionNameToUse.equals("")) {
                throw new IllegalArgumentException("Collection name cannot be empty");
            }
            this.collectionName = collectionNameToUse;

            return this;
        }

        public Builder retryStrategy(RetryStrategy retryStrategy) {
            Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            this.retryStrategy = retryStrategy;
            return this;
        }

        public SpringMongoLeaseCompetingConsumerStrategy build() {
            Clock clockToUse = clock == null ? Clock.systemUTC() : clock;
            Duration leaseTimeToUse = leaseTime == null ? DEFAULT_LEASE_TIME : leaseTime;
            String collectionNameToUse = collectionName == null ? DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION : collectionName;
            RetryStrategy retryStrategyToUse = retryStrategy == null ? RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f) : retryStrategy;
            MongoLeaseCompetingConsumerStrategySupport support = new MongoLeaseCompetingConsumerStrategySupport(leaseTimeToUse, clockToUse, retryStrategyToUse)
                    .scheduleRefresh(consumer -> () -> staticallyWithCompetingConsumerLocksCollectionReturn(mongoOperations, collectionNameToUse, collection -> {
                        consumer.accept(collection);
                        return null;
                    }));
            return new SpringMongoLeaseCompetingConsumerStrategy(mongoOperations, collectionNameToUse, support);
        }
    }
}