package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;

import javax.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {
    private static final Logger log = LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategy.class);

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    private final MongoOperations mongoOperations;
    private final Clock clock;
    private final Duration leaseTime;
    private final String collectionName;
    private final ScheduledRefresh scheduledRefresh;
    private final Map<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;

    private volatile boolean running;

    public static MongoLeaseCompetingConsumerStrategy withDefaults(MongoOperations mongoOperations) {
        return new MongoLeaseCompetingConsumerStrategy.Builder(mongoOperations).build();
    }

    private MongoLeaseCompetingConsumerStrategy(MongoOperations mongoOperations, Duration leaseTime, String collectionName, Clock clock, ScheduledRefresh scheduledRefresh, RetryStrategy retryStrategy) {
        Objects.requireNonNull(mongoOperations, MongoOperations.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(clock, Clock.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(leaseTime, "Lease time cannot be null");
        Objects.requireNonNull(collectionName, "Collection name cannot be null");
        Objects.requireNonNull(scheduledRefresh, ScheduledRefresh.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.mongoOperations = mongoOperations;
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.collectionName = collectionName;
        this.scheduledRefresh = scheduledRefresh;
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();

        if (retryStrategy instanceof Retry) {
            Retry retry = ((Retry) retryStrategy);
            this.retryStrategy = retry.retryIf(retry.retryPredicate.and(__ -> running));
        } else {
            this.retryStrategy = retryStrategy;
        }

        scheduledRefresh.scheduleInBackground(this::refreshOrAcquireLease, leaseTime);
    }

    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");

        log.info("### [registerCompetingConsumer] {} {}", subscriptionId, subscriberId);
        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Status oldStatus = competingConsumers.get(competingConsumer);
        boolean acquired = withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.acquireOrRefreshFor(collection, clock, retryStrategy, leaseTime, subscriptionId, subscriberId)).isPresent();
        log.info("### [registerCompetingConsumer] acquired {} {} {}", acquired, subscriptionId, subscriberId);
        competingConsumers.put(competingConsumer, acquired ? Status.LOCK_ACQUIRED : Status.LOCK_NOT_ACQUIRED);
        if (oldStatus != Status.LOCK_ACQUIRED && acquired) {
            log.info("### [registerCompetingConsumer] oldLock false, new true {} {}", subscriptionId, subscriberId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        } else if (oldStatus == Status.LOCK_ACQUIRED && !acquired) {
            log.info("### [registerCompetingConsumer] oldLock true, new false {} {}", subscriptionId, subscriberId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
        return acquired;
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        log.info("### [unregisterCompetingConsumer] {} {}", subscriptionId, subscriberId);
        Status status = competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
        withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.remove(collection, retryStrategy, subscriptionId));
        if (status == Status.LOCK_ACQUIRED) {
            log.info("### [unregisterCompetingConsumer] LOCK_ACQUIRED {} {}", subscriptionId, subscriberId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
    }

    @Override
    public boolean isRegisteredCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        return competingConsumers.containsKey(new CompetingConsumer(subscriptionId, subscriberId));
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        return status == Status.LOCK_ACQUIRED;
    }

    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.add(listenerConsumer);
    }

    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.remove(listenerConsumer);
    }

    private <T> T withCompetingConsumerLocksCollectionDo(Function<MongoCollection<BsonDocument>, T> fn) {
        return mongoOperations.execute(db -> {
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            return fn.apply(collection);
        });
    }

    @PreDestroy
    @Override
    public void shutdown() {
        running = false;
        scheduledRefresh.close();
    }

    private void refreshOrAcquireLease() {
        competingConsumers.forEach((cc, status) -> {
            if (status == Status.LOCK_ACQUIRED) {
                log.info("### [refreshOrAcquireLease] Lock is acquired by {} {}", cc.subscriptionId, cc.subscriberId);
                boolean stillHasLock = withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.commit(collection, clock, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId));
                if (!stillHasLock) {
                    log.info("### [refreshOrAcquireLease] Lock was lost {} {}", cc.subscriptionId, cc.subscriberId);
                    // Lock was lost!
                    competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                    competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId));
                } else {
                    log.info("### [refreshOrAcquireLease] Still has lock {} {}", cc.subscriptionId, cc.subscriberId);
                }
            } else {
                log.info("### [refreshOrAcquireLease] Will try to acquire lock {} {}", cc.subscriptionId, cc.subscriberId);
                registerCompetingConsumer(cc.subscriptionId, cc.subscriberId);
            }
        });
    }

    private static class CompetingConsumer {
        private final String subscriptionId;
        private final String subscriberId;

        private CompetingConsumer(String subscriptionId, String subscriberId) {
            this.subscriptionId = subscriptionId;
            this.subscriberId = subscriberId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompetingConsumer)) return false;
            CompetingConsumer that = (CompetingConsumer) o;
            return Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(subscriberId, that.subscriberId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subscriptionId, subscriberId);
        }
    }

    private enum Status {
        LOCK_ACQUIRED, LOCK_NOT_ACQUIRED
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

        public MongoLeaseCompetingConsumerStrategy build() {
            Clock clockToUse = clock == null ? Clock.systemUTC() : clock;
            Duration leaseTimeToUse = leaseTime == null ? DEFAULT_LEASE_TIME : leaseTime;
            String collectionNameToUse = collectionName == null ? DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION : collectionName;
            RetryStrategy retryStrategyToUse = retryStrategy == null ? RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f) : retryStrategy;
            return new MongoLeaseCompetingConsumerStrategy(mongoOperations, leaseTimeToUse, collectionNameToUse, clockToUse, ScheduledRefresh.auto(), retryStrategyToUse);
        }
    }
}