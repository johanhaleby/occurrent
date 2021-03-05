package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.subscription.api.blocking.CompetingConsumersStrategy;
import org.springframework.data.mongodb.core.MongoOperations;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MongoLeaseCompetingConsumersStrategy implements CompetingConsumersStrategy {

    public static final String COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";

    private final MongoOperations mongoOperations;
    private final Clock clock;
    private final Duration leaseTime;
    private final String collectionName;
    private final Set<String> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;

    public MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations, Clock clock, Duration leaseTime, String collectionName) {
        this.mongoOperations = mongoOperations;
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.collectionName = collectionName;
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public static CompetingConsumersStrategy withDefaults(MongoOperations mongoOperations) {
        return new MongoLeaseCompetingConsumersStrategy(mongoOperations, Clock.systemUTC(), Duration.ofSeconds(30), COMPETING_CONSUMER_LOCKS_COLLECTION);
    }

    @Override
    public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        boolean acquired = withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.acquireOrRefreshFor(collection, clock, leaseTime, subscriptionId, subscriberId)).isPresent();
        if (acquired) {
            competingConsumers.add(key(subscriptionId, subscriberId));
        }
        return acquired;
    }

    @Override
    public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.remove(collection, subscriptionId));
        competingConsumers.remove(key(subscriptionId, subscriberId));
    }

    @Override
    public boolean isRegisteredCompetingConsumer(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        return competingConsumers.contains(key(subscriptionId, subscriberId));
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

    private static String key(String subscriptionId, String subscriberId) {
        return subscriptionId + ":" + subscriberId;
    }
}