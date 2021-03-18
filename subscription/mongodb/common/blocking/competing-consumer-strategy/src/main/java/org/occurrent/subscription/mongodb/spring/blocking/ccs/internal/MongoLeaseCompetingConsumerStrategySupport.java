package org.occurrent.subscription.mongodb.spring.blocking.ccs.internal;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Common operations for MongoDB lease-based competing consumer strategies
 */
public class MongoLeaseCompetingConsumerStrategySupport {

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    private final Clock clock;
    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final Map<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;

    private volatile boolean running;


    public MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, Clock clock, RetryStrategy retryStrategy) {
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.scheduledRefresh = ScheduledRefresh.auto();
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();

        if (retryStrategy instanceof Retry) {
            Retry retry = ((Retry) retryStrategy);
            this.retryStrategy = retry.retryIf(retry.retryPredicate.and(__ -> running));
        } else {
            this.retryStrategy = retryStrategy;
        }
    }

    public MongoLeaseCompetingConsumerStrategySupport scheduleRefresh(Function<Consumer<MongoCollection<BsonDocument>>, Runnable> fn) {
        scheduledRefresh.scheduleInBackground(() -> fn.apply(this::refreshOrAcquireLease).run(), leaseTime);
        return this;
    }

    public boolean registerCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
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

    public void unregisterCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
        MongoListenerLockService.remove(collection, retryStrategy, subscriptionId);
        if (status == Status.LOCK_ACQUIRED) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
    }

    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        return status == Status.LOCK_ACQUIRED;
    }

    public void addListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.add(listenerConsumer);
    }

    public void removeListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.remove(listenerConsumer);
    }

    public void shutdown() {
        running = false;
        scheduledRefresh.close();
    }

    private void refreshOrAcquireLease(MongoCollection<BsonDocument> collection) {
        competingConsumers.forEach((cc, status) -> {
            if (status == Status.LOCK_ACQUIRED) {
                boolean stillHasLock = MongoListenerLockService.commit(collection, clock, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId);
                if (!stillHasLock) {
                    // Lock was lost!
                    competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                    competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId));
                }
            } else {
                registerCompetingConsumer(collection, cc.subscriptionId, cc.subscriberId);
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
}