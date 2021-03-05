package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.subscription.api.blocking.CompetingConsumersStrategy;
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

public class MongoLeaseCompetingConsumersStrategy implements CompetingConsumersStrategy {

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";

    private final MongoOperations mongoOperations;
    private final Clock clock;
    private final Duration leaseTime;
    private final String collectionName;
    private final ScheduledRefresh scheduledRefresh;
    private final Map<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;

    public MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations) {
        this(mongoOperations, Duration.ofSeconds(20), DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION);
    }

    public MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations, Duration leaseTime, String collectionName) {
        this(mongoOperations, leaseTime, collectionName, Clock.systemUTC());
    }

    public MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations, Duration leaseTime, String collectionName, Clock clock) {
        this(mongoOperations, leaseTime, collectionName, clock, ScheduledRefresh.auto());
    }

    private MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations, Duration leaseTime, String collectionName, Clock clock, ScheduledRefresh scheduledRefresh) {
        Objects.requireNonNull(mongoOperations, MongoOperations.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(clock, Clock.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(leaseTime, "Lease time cannot be null");
        Objects.requireNonNull(collectionName, "Collection name cannot be null");
        Objects.requireNonNull(scheduledRefresh, ScheduledRefresh.class.getSimpleName() + " name cannot be null");
        this.mongoOperations = mongoOperations;
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.collectionName = collectionName;
        this.scheduledRefresh = scheduledRefresh;
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();

        scheduledRefresh.scheduleInBackground(this::refreshOrAcquireLease, leaseTime);
    }

    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        boolean acquired = withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.acquireOrRefreshFor(collection, clock, leaseTime, subscriptionId, subscriberId)).isPresent();
        competingConsumers.put(new CompetingConsumer(subscriptionId, subscriberId), acquired ? Status.ACQUIRED_LOCK : Status.NOT_ACQUIRED_LOCK);
        if (acquired) {
            competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        }
        return acquired;
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.remove(collection, subscriptionId));
        competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
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
        return status == Status.ACQUIRED_LOCK;
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
        scheduledRefresh.close();
    }

    private void refreshOrAcquireLease() {
        competingConsumers.forEach((cc, status) -> {
            if (status == Status.ACQUIRED_LOCK) {
                boolean stillHasLock = withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.commit(collection, clock, leaseTime, cc.subscriptionId, cc.subscriberId));
                if (!stillHasLock) {
                    // Lock was lost!
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
        ACQUIRED_LOCK, NOT_ACQUIRED_LOCK
    }
}