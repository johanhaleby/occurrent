package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import javax.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MongoLeaseCompetingConsumerStrategy implements CompetingConsumerStrategy {

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    private final MongoCollection<BsonDocument> collection;
    private final Clock clock;
    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final Map<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;

    private volatile boolean running;

    public static MongoLeaseCompetingConsumerStrategy withDefaults(MongoDatabase db) {
        return new MongoLeaseCompetingConsumerStrategy.Builder(db).build();
    }

    private MongoLeaseCompetingConsumerStrategy(MongoCollection<BsonDocument> collection, Duration leaseTime, Clock clock, ScheduledRefresh scheduledRefresh, RetryStrategy retryStrategy) {
        Objects.requireNonNull(collection, MongoCollection.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(clock, Clock.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(leaseTime, "Lease time cannot be null");
        Objects.requireNonNull(scheduledRefresh, ScheduledRefresh.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.collection = collection;
        this.clock = clock;
        this.leaseTime = leaseTime;
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

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Status oldStatus = competingConsumers.get(competingConsumer);
        boolean acquired = MongoListenerLockService.acquireOrRefreshFor(collection, clock, retryStrategy, leaseTime, subscriptionId, subscriberId).isPresent();
        competingConsumers.put(competingConsumer, acquired ? Status.LOCK_ACQUIRED : Status.LOCK_NOT_ACQUIRED);
        if (oldStatus != Status.LOCK_ACQUIRED && acquired) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        } else if (oldStatus == Status.LOCK_ACQUIRED && !acquired) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
        return acquired;
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
        MongoListenerLockService.remove(collection, retryStrategy, subscriptionId);
        if (status == Status.LOCK_ACQUIRED) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
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

    @PreDestroy
    @Override
    public void shutdown() {
        running = false;
        scheduledRefresh.close();
    }

    private void refreshOrAcquireLease() {
        competingConsumers.forEach((cc, status) -> {
            if (status == Status.LOCK_ACQUIRED) {
                boolean stillHasLock = MongoListenerLockService.commit(collection, clock, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId);
                if (!stillHasLock) {
                    // Lock was lost!
                    competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                    competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId));
                }
            } else {
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
            return new MongoLeaseCompetingConsumerStrategy(collection, leaseTimeToUse, clockToUse, ScheduledRefresh.auto(), retryStrategyToUse);
        }
    }
}